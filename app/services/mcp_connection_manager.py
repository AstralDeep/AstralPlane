# app/services/mcp_connection_manager.py
import asyncio
import copy
import json
import logging
import time
from contextlib import AsyncExitStack
from datetime import datetime
from functools import partial
from http.client import HTTPException
from typing import Dict, Any, Optional, List, Tuple, Union, Set

import anyio
import mcp.types as mcp_types
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.shared.context import RequestContext
from mcp.shared.exceptions import McpError
from mcp.types import (
	INTERNAL_ERROR, METHOD_NOT_FOUND, CallToolResult,
	LoggingMessageNotificationParams
)
from pydantic import ValidationError

from app.config import Settings  # Keep for other settings if any
# Import your new SQLAlchemy model for type hinting
from app.models.mcp_server_config_model import MCPServerConfig
from app.models.schemas import MCPLogEntry
from app.models.schemas import (
	PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
	# MCPLogEntry, # This was imported in your main.py, ensure it's here if used directly
	ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage, MCPNotificationPayload, MCPNotificationMessage,
	MCPProgressPayload, MCPProgressMessage
)
from app.services.connection_manager import ConnectionManager  # For UI client connections

logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('mcp.client.sse').setLevel(logging.INFO)  # Or your preferred level


def _create_error_content(message: str, code: int = INTERNAL_ERROR, code_str: Optional[str] = None) -> Union[
	Dict[str, Any], Any]:
	if mcp_types and hasattr(mcp_types, 'ErrorData'):
		return mcp_types.ErrorData(code=code, message=message)
	else:
		error_key = code_str or "ERROR"
		return {error_key: code, "message": message}


class MCPConnectionManager:
	def __init__(self, settings: Settings, connection_manager: ConnectionManager):
		logger.info("Initializing MCPConnectionManager (DB-Driven Mode)...")
		self.settings = settings  # May still be used for timeouts, etc.
		self.ui_connection_manager = connection_manager  # For sending updates to UI clients

		# This will store the state for each server, keyed by str(MCPServerConfig.id)
		self.sse_connections: Dict[str, Dict[str, Any]] = {}
		self._connection_lock = asyncio.Lock()
		# self.server_configs is removed; configs come from DB via initialize_servers_from_db

		logger.info("MCPConnectionManager initialized (empty, awaiting DB configurations).")

	# --- NEW: Initialization from Database ---
	async def initialize_servers_from_db(self, db_configs: List[MCPServerConfig]):
		logger.info(f"Loading and preparing {len(db_configs)} MCP server configurations from database.")
		async with self._connection_lock:
			# Clear existing connections if any (e.g., during a re-initialization scenario)
			for server_id_to_clear in list(self.sse_connections.keys()):
				# Perform a more graceful cleanup if sessions are active
				details = self.sse_connections.get(server_id_to_clear)
				if details and (details.get("exit_stack") or details.get("session")):
					await self._do_cleanup_for_server(server_id_to_clear, details, "re-initialization")
				del self.sse_connections[server_id_to_clear]

			for db_config in db_configs:
				server_id_str = str(db_config.id)
				# Prepare the config dict that connect_and_prepare_server expects
				# It should match the structure previously derived from settings.MCP_SERVERS
				# Ensure 'id', 'url', 'name' are present.
				config_dict_for_server = {
					"id": server_id_str,  # Essential: Used as the key and internally
					"url": db_config.url,
					"name": db_config.name,
					"description": db_config.description,
					# Add any other fields from MCPServerConfig that connect_and_prepare_server might need
					# or that were previously in settings.MCP_SERVERS structure.
					# For example, if you had specific 'auth_token_env_var' or 'client_name_prefix'
					# these would now need to be part of your MCPServerConfig model or handled differently.
					# For now, assuming 'id', 'url', 'name' are primary.
					"transport": "sse"  # Assuming all DB configured servers are SSE for this manager
				}

				self.sse_connections[server_id_str] = {
					"server_id": server_id_str,  # Redundant but clear
					"status": "pending_initialization",  # New status
					"ref_count": 0,
					"config_from_db": db_config,  # Store the SQLAlchemy model instance
					"config_for_connection": config_dict_for_server,  # Dict used by connect_and_prepare
					"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
					"required_primitives": set(), "error_message": None,
					"last_connect_attempt": None, "last_successful_connect": None,
				}
				logger.debug(f"[{server_id_str}] Configured server '{db_config.name}' from DB.")

		# Asynchronously attempt to connect to active servers
		connect_tasks = []
		for server_id_str, details in self.sse_connections.items():
			db_config_obj: MCPServerConfig = details["config_from_db"]
			if db_config_obj.is_active:
				logger.info(
					f"[{server_id_str}] Scheduling proactive connection for active server '{db_config_obj.name}'.")
				# Create task so initialization isn't blocked
				connect_tasks.append(
					asyncio.create_task(
						self.connect_and_prepare_server(server_id_str, details["config_for_connection"])
					)
				)
			else:
				await self._update_connection_state(server_id_str, {"status": "inactive_configured"})
				logger.info(
					f"[{server_id_str}] Server '{db_config_obj.name}' is configured but inactive. Skipping proactive connect.")

		if connect_tasks:
			await asyncio.gather(*connect_tasks, return_exceptions=True)  # Wait for initial attempts
			logger.info(f"Initial proactive connection attempts for {len(connect_tasks)} active servers complete.")
		else:
			logger.info("No active servers found in DB to connect to proactively.")

	# --- Core Connection and Preparation Logic (largely unchanged, uses config_for_connection) ---
	# In app/services/mcp_connection_manager.py -> class MCPConnectionManager

	async def connect_and_prepare_server(self, server_id: str, server_config: Dict[str, Any]) -> bool:
		# server_config is expected to be a dictionary like:
		# {"id": str(db_config.id), "url": db_config.url, "name": db_config.name, ...}
		server_name_for_logs = server_config.get('name', server_id)  # Use name for clearer logs
		logger.info(f"[{server_id}] Proactive connection attempt starting for server '{server_name_for_logs}'...")

		server_url = server_config.get("url")
		if not server_url:
			logger.error(
				f"[{server_id}] Proactive connect failed for '{server_name_for_logs}': Missing 'url' in config.")
			# Assuming _update_connection_state is an async method that handles its own locking
			await self._update_connection_state(server_id, {
				"status": "error",
				"error_message": "Missing URL in configuration"
			})
			return False

		exit_stack = AsyncExitStack()
		session: Optional[ClientSession] = None  # Annotation for the 'session' variable
		ui_layout: Optional[dict] = None
		processed_tools: Optional[dict] = None
		required_primitives: Set[str] = set()
		connect_time = time.monotonic()

		# Update status to "connecting"
		# This call assumes _update_connection_state acquires its own lock if needed.
		await self._update_connection_state(server_id, {
			"status": "connecting",
			"last_connect_attempt": connect_time,
			"error_message": None,
			"session": None,  # Clear previous session/stack details if retrying
			"exit_stack": None,
			"tools": None,
			"ui_layout": None,
			"required_primitives": set()
		})

		try:
			await exit_stack.__aenter__()  # Prepare the exit stack

			read_stream, write_stream = await exit_stack.enter_async_context(sse_client(server_url))
			message_handler_with_id = partial(self._handle_incoming_message, server_id=server_id)

			# --- Addressing the Type Error ---
			# Step 1: Explicitly create and type the ClientSession instance
			# The constructor of ClientSession should return an instance of ClientSession.
			mcp_client_instance: ClientSession = ClientSession(
				read_stream, write_stream,
				sampling_callback=self._default_sampling_callback,
				message_handler=message_handler_with_id
			)

			# Step 2: Enter this explicitly typed instance into the context stack.
			# enter_async_context should return the result of mcp_client_instance.__aenter__(),
			# which is typically the instance itself (ClientSession).
			session = await exit_stack.enter_async_context(mcp_client_instance)

			# Defensive check, though __aenter__ typically returns self or raises an error
			if session is None:
				logger.critical(
					f"[{server_id}] Critical error: ClientSession context entry returned None for '{server_name_for_logs}'. This is unexpected.")
				# Update state to reflect this critical failure
				await self._safe_aclose(exit_stack, server_id, "critical session None cleanup")
				await self._update_connection_state(server_id, {
					"status": "error",
					"error_message": "Failed to enter ClientSession context (returned None)",
					"session": None, "exit_stack": None
				})
				return False
			# --- End of Type Error Address ---

			# Use timeouts from self.settings if available, otherwise fallback to original values
			init_timeout = getattr(self.settings, 'MCP_INIT_TIMEOUT', 15.0)
			init_result = await asyncio.wait_for(session.initialize(), timeout=init_timeout)
			self._check_mcp_result_for_error(init_result, f"Initialize for '{server_name_for_logs}' ({server_id})")
			logger.info(f"[{server_id}] MCP Session Initialized for '{server_name_for_logs}'.")

			tools_timeout = getattr(self.settings, 'MCP_LIST_TOOLS_TIMEOUT', 15.0)
			tools_result = await asyncio.wait_for(session.list_tools(), timeout=tools_timeout)
			self._check_mcp_result_for_error(tools_result, f"ListTools for '{server_name_for_logs}' ({server_id})")
			processed_tools = self._process_discovered_tools(
				tools_result.tools if tools_result and hasattr(tools_result, 'tools') else []
			)
			logger.info(
				f"[{server_id}] Discovered tools for '{server_name_for_logs}': {list(processed_tools.keys()) if processed_tools else 'None'}")

			ui_layout_timeout = getattr(self.settings, 'MCP_GET_UI_LAYOUT_TIMEOUT', 20.0)
			ui_layout = await asyncio.wait_for(self._get_server_ui_layout(session, server_id),
											   timeout=ui_layout_timeout)

			if ui_layout:
				required_primitives = self._extract_required_primitives(ui_layout)
				logger.info(
					f"[{server_id}] Retrieved UI layout for '{server_name_for_logs}'. Required primitives: {required_primitives}")
			else:
				logger.warning(f"[{server_id}] UI layout not retrieved or is invalid for '{server_name_for_logs}'.")

			# Successfully connected and prepared
			await self._update_connection_state(server_id, {
				"status": "connected",
				"session": session,  # Store the actual ClientSession instance
				"exit_stack": exit_stack,  # Store the exit_stack to be closed later
				"tools": processed_tools,
				"ui_layout": ui_layout,
				"required_primitives": required_primitives,
				"error_message": None,  # Clear any previous error
				"last_successful_connect": time.monotonic(),
			})
			logger.info(
				f"[{server_id}] Proactive connection successful for '{server_name_for_logs}'. UI Layout Retrieved: {ui_layout is not None}")
			return True

		except (asyncio.TimeoutError, McpError, anyio.EndOfStream, anyio.ClosedResourceError, ConnectionRefusedError,
				Exception) as e:
			# Consolidated error handling
			error_msg_detail = ""
			if isinstance(e, asyncio.TimeoutError):
				error_msg_detail = f"Operation timed out: {e}"
			elif isinstance(e, McpError):
				error_msg_detail = f"MCP Protocol Error: {getattr(e, 'error', e)}"  # Access error attribute safely
			elif isinstance(e, (anyio.EndOfStream, anyio.ClosedResourceError)):
				error_msg_detail = f"Connection closed unexpectedly: {e}"
			elif isinstance(e, ConnectionRefusedError):
				error_msg_detail = f"Connection refused by server: {e}"
			else:
				error_msg_detail = f"Unexpected error during connection: {e}"  # General exceptions

			full_error_message = f"Proactive connection failed for '{server_name_for_logs}' ({server_id}). {error_msg_detail}"
			# Log more selectively for common connection issues vs unexpected code errors
			log_exc_info = not isinstance(e, (
				McpError, asyncio.TimeoutError, ConnectionRefusedError, anyio.EndOfStream, anyio.ClosedResourceError))
			logger.error(full_error_message, exc_info=log_exc_info)

			await self._safe_aclose(exit_stack, server_id, "proactive connection failure cleanup")
			await self._update_connection_state(server_id, {
				"status": "error",
				"error_message": str(e),  # Store the string representation of the error
				"session": None,
				"exit_stack": None,
				"tools": None,  # Clear stale data on failure
				"ui_layout": None,
				"required_primitives": set(),
			})
			return False

	# --- NEW: Dynamic Management Methods ---
	async def add_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Adding new server '{db_config.name}' from live configuration update.")
		async with self._connection_lock:
			if server_id_str in self.sse_connections:
				logger.warning(f"[{server_id_str}] Attempted to add server that already exists. Consider using update.")
				# Optionally, treat as an update or return early
				# For now, let's proceed to update its state as if it's new or being re-enabled
				pass  # Fall through to update/connection logic

			config_dict_for_server = {
				"id": server_id_str, "url": db_config.url, "name": db_config.name,
				"description": db_config.description, "transport": "sse"
			}
			self.sse_connections[server_id_str] = {
				"server_id": server_id_str, "status": "pending_add", "ref_count": 0,
				"config_from_db": db_config, "config_for_connection": config_dict_for_server,
				"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
				"required_primitives": set(), "error_message": None,
				"last_connect_attempt": None, "last_successful_connect": None,
			}
			logger.debug(f"[{server_id_str}] Added server '{db_config.name}' to internal state.")

		if db_config.is_active:
			logger.info(f"[{server_id_str}] New server '{db_config.name}' is active. Attempting proactive connection.")
			await self.connect_and_prepare_server(server_id_str, config_dict_for_server)
		else:
			await self._update_connection_state(server_id_str, {"status": "inactive_configured"})
			logger.info(f"[{server_id_str}] New server '{db_config.name}' added as inactive.")

	async def update_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Updating server '{db_config.name}' from live configuration change.")

		async with self._connection_lock:
			current_details = self.sse_connections.get(server_id_str)
			if not current_details:
				logger.warning(f"[{server_id_str}] Config update for non-existent server. Treating as add.")
				# Unlock and call add, then lock again if further ops, or just call add and return
				# For simplicity here, we'll call add_server_from_config.
				# This assumes add_server_from_config is safe to call if already locked.
				# A better approach might be to release lock and call, or ensure add is lock-agnostic internally.
				# Given current structure, let's call it directly and it will re-acquire lock.
				# await self.add_server_from_config(db_config) <-- This would deadlock if not careful
				# Simpler: update state then decide on connection.
				# Let's assume the entry must exist for an update. If not, it's an error or should be an "add".
				# For now, if it doesn't exist, we can effectively "add" it by setting up its state.
				# However, the CRUD API should ensure "update" is for existing items.
				# So, if current_details is None, it's likely an issue with frontend/API logic
				# or this manager wasn't properly initialized with it if it's an existing DB entry.
				logger.error(
					f"[{server_id_str}] Update called for a server not in memory. This might indicate a state mismatch.")
				# Fallback: treat as an "add" by directly setting up the config then proceeding.
				# This will ensure it exists for the logic below.
				config_dict_for_server_temp = {
					"id": server_id_str, "url": db_config.url, "name": db_config.name,
					"description": db_config.description, "transport": "sse"
				}
				self.sse_connections[server_id_str] = {
					"server_id": server_id_str, "status": "pending_update", "ref_count": 0,
					"config_from_db": db_config, "config_for_connection": config_dict_for_server_temp,
					"session": None, "exit_stack": None,  # etc.
				}
				current_details = self.sse_connections[server_id_str]

			old_config_from_db: Optional[MCPServerConfig] = current_details.get("config_from_db")
			needs_reconnect = False

			if old_config_from_db:
				if old_config_from_db.url != db_config.url:
					needs_reconnect = True
				# Add other critical parameter checks that necessitate a reconnect
				if old_config_from_db.is_active != db_config.is_active:
					needs_reconnect = True  # Status change always triggers action
			else:  # If no old config, it's like a new addition, so connect if active
				needs_reconnect = True

			# Update stored configurations
			config_dict_for_server = {
				"id": server_id_str, "url": db_config.url, "name": db_config.name,
				"description": db_config.description, "transport": "sse"
			}
			current_details["config_from_db"] = db_config
			current_details["config_for_connection"] = config_dict_for_server
			# Preserve ref_count, tools, ui_layout unless reconnecting
			# Preserve error messages unless a successful connect happens

			if needs_reconnect:
				logger.info(
					f"[{server_id_str}] Configuration change requires connection update for '{db_config.name}'.")
				# Disconnect if currently connected or in an error state from a previous connection attempt
				if current_details.get("session") or current_details.get("exit_stack") or current_details.get(
						"status") == "error":
					logger.info(f"[{server_id_str}] Cleaning up old connection before attempting update...")
					await self._do_cleanup_for_server(server_id_str, current_details, "config update")
			# _do_cleanup_for_server will reset session, exit_stack, and potentially status

			# Update status based on activity (even if not reconnecting, status reflects desired state)
			if not db_config.is_active:
				current_details["status"] = "inactive_configured"  # Update status directly
				# Ensure tools/ui_layout are cleared if it becomes inactive
				current_details["tools"] = None
				current_details["ui_layout"] = None
				current_details["required_primitives"] = set()
				current_details["session"] = None  # Ensure session is None
				current_details["exit_stack"] = None  # Ensure exit_stack is None
				logger.info(f"[{server_id_str}] Server '{db_config.name}' updated to inactive.")
			elif needs_reconnect:  # And is_active is True
				# Status will be set by connect_and_prepare_server
				pass
			else:  # No reconnect needed, and is_active (or was already active)
				# Status should remain 'connected' if it was, or be updated if it was inactive.
				if current_details.get("status") == "inactive_configured":
					# If it was inactive and now active, but no other changes trigger reconnect,
					# it still needs a connection attempt if not already connected.
					# This case might be complex if partial updates are allowed without full reconnect logic.
					# For now, assume if is_active changes to True, a reconnect/connect attempt is desired.
					# The needs_reconnect flag driven by is_active changing should handle this.
					pass

		# Perform connection actions outside the lock to avoid holding it during network I/O
		if db_config.is_active and needs_reconnect:
			logger.info(f"[{server_id_str}] Server '{db_config.name}' is active. Attempting connection/reconnection.")
			await self.connect_and_prepare_server(server_id_str, config_dict_for_server)
		elif not db_config.is_active and needs_reconnect:  # Was active, now inactive
			logger.info(f"[{server_id_str}] Server '{db_config.name}' was made inactive. Ensured cleanup.")

	# Cleanup happened inside lock if it was connected. Status already set.

	async def remove_server_by_id(self, server_db_id_str: str):
		logger.info(f"[{server_db_id_str}] Removing server from live configuration.")
		async with self._connection_lock:
			connection_details = self.sse_connections.pop(server_db_id_str, None)  # Remove from dict

		if connection_details:
			logger.info(
				f"[{server_db_id_str}] Cleaning up connection for removed server '{connection_details.get('config_for_connection', {}).get('name', server_db_id_str)}'.")
			await self._do_cleanup_for_server(server_db_id_str, connection_details, "server removal")
			logger.info(f"[{server_db_id_str}] Server removed and cleaned up.")
		else:
			logger.warning(f"[{server_db_id_str}] Attempted to remove a server that was not found in manager state.")

	# --- Internal Helper for Cleanup ---
	async def _do_cleanup_for_server(self, server_id: str, details: Dict[str, Any], context_msg: str):
		"""Helper to perform cleanup for a single server's connection resources."""
		logger.debug(
			f"[{server_id}] Performing resource cleanup ({context_msg}). Current status: {details.get('status')}")
		exit_stack: Optional[AsyncExitStack] = details.get("exit_stack")
		await self._safe_aclose(exit_stack, server_id, f"cleanup context: {context_msg}")

		# Update state after cleanup, ensuring these are reset
		# This needs to be done under lock if called from a context that doesn't already hold it
		# Or, the caller (_cleanup_sse_connection, update_server_from_config) manages the lock.
		# For _do_cleanup_for_server, let's assume caller manages the lock for sse_connections update.
		details["session"] = None
		details["exit_stack"] = None
		details["tools"] = None
		details["ui_layout"] = None
		details["required_primitives"] = set()
		# Don't necessarily change status to 'disconnected' if it was 'error', keep error info
		if details.get("status") not in ["error",
										 "inactive_configured"]:  # Avoid overwriting persistent error or inactive state
			details["status"] = "disconnected"  # Or some other appropriate post-cleanup status
		details["ref_count"] = 0  # Reset ref count
		logger.debug(f"[{server_id}] Resources cleaned up ({context_msg}). New status: {details.get('status')}")

	# --- Existing Methods (Sampling Callback, Notification Handlers, Tool Execution, Session Management) ---
	# These methods largely use server_id to fetch details from self.sse_connections.
	# They should continue to work, provided server_id is str(MCPServerConfig.id).

	# _default_sampling_callback: (Keep as is)
	async def _default_sampling_callback(self, context: Union[RequestContext["ClientSession", None], Any],
										 params: Union[mcp_types.CreateMessageRequestParams, Any]) -> Union[
		mcp_types.CreateMessageResult, mcp_types.ErrorData, Dict[str, Any]]:
		server_id = "unknown"
		if context and hasattr(context, 'session'):
			details = await self._find_details_by_session(context.session)
			if details: server_id = details.get("server_id", "unknown")
		logger.info(f"[{server_id}] Received sampling_callback request (createMessage) from server.")
		logger.debug(f"[{server_id}] Sampling Params: {params}")
		server_message_text = "No message found in server request"
		if mcp_types and params and hasattr(params, 'messages') and params.messages:
			last_message = params.messages[-1]
			if hasattr(last_message, 'content') and isinstance(last_message.content,
															   mcp_types.TextContent): server_message_text = last_message.content.text
		mock_response_text = f"Backend received: '{server_message_text}'. This is a mocked callback response."
		logger.info(f"[{server_id}] Sending mocked sampling response: '{mock_response_text}'")
		if mcp_types and hasattr(mcp_types, 'CreateMessageResult') and hasattr(mcp_types, 'TextContent'):
			return mcp_types.CreateMessageResult(role="assistant",
												 content=mcp_types.TextContent(type="text", text=mock_response_text),
												 model="mock-backend-callback-model", stopReason="endTurn")
		else:
			return {"role": "assistant", "content": {"type": "text", "text": mock_response_text},
					"model": "mock-backend-callback-model-dict", "stopReason": "endTurn"}

	# _handle_incoming_message: (Keep as is)
	async def _handle_incoming_message(self, message: Any, server_id: str):
		ServerNotification = getattr(mcp_types, 'ServerNotification', None)
		ProgressNotificationParams = getattr(mcp_types, 'ProgressNotificationParams', None)
		LoggingMessageNotificationParams = getattr(mcp_types, 'LoggingMessageNotificationParams', None)
		UpdateBindingNotificationParams = getattr(mcp_types, 'UpdateBindingNotificationParams', None)
		ResourceUpdatedNotificationParams = getattr(mcp_types, 'ResourceUpdatedNotificationParams', None)
		CancelledNotificationParams = getattr(mcp_types, 'CancelledNotificationParams', None)
		UpdateBindingNotificationParams = getattr(mcp_types, 'UpdateBindingNotificationParams', None)

		method_name = None
		params = None
		is_valid_structure = False

		if ServerNotification and isinstance(message, ServerNotification):
			notification_root = message.root
			method_name = getattr(notification_root, 'method', None)
			params = getattr(notification_root, 'params', None)
			is_valid_structure = True
		elif isinstance(message, dict):
			method_name = message.get('method')
			params = message.get('params')
			is_valid_structure = True
		elif hasattr(message, 'method') and hasattr(message, 'params'):
			method_name = getattr(message, 'method', None)
			params = getattr(message, 'params', None)
			is_valid_structure = True

		if not is_valid_structure: logger.warning(
			f"[{server_id}] Received unknown type via message_handler: {type(message)} - {message!r}"); return
		if not method_name: logger.warning(
			f"[{server_id}] Received message without a 'method' field: {message!r}"); return

		logger.info(f"[{server_id}] Routing incoming notification. Method: '{method_name}'")
		try:
			if method_name == "app/streaming_log_update":
				await self._handle_streaming_update(server_id, params or {})
			elif method_name == "notifications/progress":
				if (ProgressNotificationParams and isinstance(params, ProgressNotificationParams)) or isinstance(params,
																												 dict):
					await self._handle_progress(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for progress: {type(params)}")
			elif method_name == "notifications/message":
				if (LoggingMessageNotificationParams and isinstance(params,
																	LoggingMessageNotificationParams)) or isinstance(
					params, dict):
					await self._handle_mcp_log_message(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for message: {type(params)}")
			elif method_name == "notifications/update_binding":  # Ensure UpdateBindingNotificationParams is imported/defined
				if (UpdateBindingNotificationParams and isinstance(params,
																   UpdateBindingNotificationParams)) or isinstance(
					params, dict):
					await self._handle_update_binding(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for update_binding: {type(params)}")
			elif method_name == "notifications/tools/list_changed":
				await self._handle_tool_list_changed(server_id, params)
			elif method_name == "notifications/resources/updated":
				if (ResourceUpdatedNotificationParams and isinstance(params,
																	 ResourceUpdatedNotificationParams)) or isinstance(
					params, dict):
					await self._handle_resource_updated(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for resources/updated: {type(params)}")
			elif method_name == "notifications/resources/list_changed":
				await self._handle_resource_list_changed(server_id, params)
			elif method_name == "notifications/prompts/list_changed":
				await self._handle_prompt_list_changed(server_id, params)
			elif method_name == "notifications/cancelled":
				if (CancelledNotificationParams and isinstance(params, CancelledNotificationParams)) or isinstance(
						params, dict):
					await self._handle_cancelled_by_server(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for cancelled: {type(params)}")
			elif method_name == "notifications/update_binding":
				logger.debug(f"[{server_id}] Routing to standard handler _handle_update_binding.")
				# This condition correctly handles UpdateBindingNotificationParams being None
				if (UpdateBindingNotificationParams and isinstance(params,
																   UpdateBindingNotificationParams)) or isinstance(
					params, dict):
					await self._handle_update_binding(server_id, params or {})
				else:
					logger.warning(f"[{server_id}] Invalid params type for update_binding: {type(params)}")
			else:
				logger.warning(f"[{server_id}] Received unhandled notification method: {method_name}")
		except Exception as handler_exc:
			logger.error(f"[{server_id}] Error executing handler for method '{method_name}': {handler_exc}",
						 exc_info=True)

	async def _handle_progress(self, server_id: str, params: Union[mcp_types.ProgressNotificationParams, Dict]):
		logger.debug(f"[{server_id}] Handling Progress Notification: {params}")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping progress send.")
			return
		try:
			# Safely access attributes, defaulting to None if not present or if params is a dict
			token = params.get('progressToken') if isinstance(params, dict) else getattr(params, 'progressToken', None)
			percentage = params.get('percentage') if isinstance(params, dict) else getattr(params, 'percentage', None)
			message_text = params.get('message') if isinstance(params, dict) else getattr(params, 'message', None)
			title = params.get('title') if isinstance(params, dict) else getattr(params, 'title', None)

			payload = MCPProgressPayload(
				server_id=server_id,
				token=token,
				percentage=percentage,
				message=message_text,
				title=title
			)
			message_to_send = MCPProgressMessage(payload=payload)
			await self.ui_connection_manager.send_text(message_to_send.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating MCPProgressMessage: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending progress notification: {e}", exc_info=True)

	async def _handle_mcp_log_message(self, server_id: str, params: Union[LoggingMessageNotificationParams, Dict, Any]):
		logger.debug(
			f"[{server_id}] Handling MCP Log Notification ('notifications/message'). Params Type: {type(params)}, Value: {params!r}")
		stream_id = f"mcp:{server_id}"
		default_log_binding = f"mcp_stream:{server_id}:log_messages"

		TARGETED_BINDING_PREFIX = f"mcp_stream:{server_id}:"
		EXPECTED_RAW_STREAM_LOGGER_NAME = f"{TARGETED_BINDING_PREFIX}raw_llm_stream"

		target_binding = default_log_binding
		update_type: str = "append"
		content_to_send: Any = None

		try:
			log_level_raw = getattr(params, 'level', 'log')
			log_data: Any = getattr(params, 'data', None)
			logger_name: Optional[str] = getattr(params, 'logger', None)

			if log_data is None:
				logger.warning(
					f"[{server_id}] Log message has None or missing 'data'. Ignoring. Logger='{logger_name}'")
				return

			final_content = log_data  # Default to original log_data

			if isinstance(logger_name, str) and logger_name.startswith(TARGETED_BINDING_PREFIX):
				if logger_name == EXPECTED_RAW_STREAM_LOGGER_NAME:
					target_binding = logger_name
					final_content = str(log_data)  # Ensure string for raw stream
					update_type = "append"
					content_to_send = final_content
					logger.debug(f"[{server_id}] Detected raw stream chunk for '{target_binding}'.")
				else:
					target_binding = logger_name
					# final_content is already log_data
					update_type = "replace"
					content_to_send = final_content
					logger.debug(
						f"[{server_id}] Detected targeted UI update for '{target_binding}'. Type: {type(content_to_send).__name__}")
			else:  # Standard log message
				log_level = str(log_level_raw).lower()
				valid_levels = {"error", "warning", "info", "debug", "log"}
				if log_level not in valid_levels: log_level = "log"
				log_data_str = str(final_content)  # Convert to string for MCPLogEntry
				try:
					log_entry = MCPLogEntry(level=log_level, message=log_data_str, timestamp=datetime.now())
					content_to_send = log_entry.model_dump(exclude_none=True)
					logger.debug(f"[{server_id}] Sending structured log entry to '{target_binding}'.")
				except ImportError:
					content_to_send = f"[{log_level.upper()}] {log_data_str}"
					logger.warning(
						f"[{server_id}] MCPLogEntry schema not found, sending plain text log to '{target_binding}'.")
				except Exception as log_entry_err:
					content_to_send = f"[LOG_ERROR:{log_level.upper()}] {log_data_str}"
					logger.error(f"[{server_id}] Error creating MCPLogEntry: {log_entry_err}", exc_info=True)

			if content_to_send is None:
				logger.error(
					f"[{server_id}] Failed to determine content_to_send for notification. Params: {params!r}, Target: {target_binding}")
				return

			update_payload_obj = PrimitiveContentUpdatePayload(
				targetBinding=target_binding,
				content=content_to_send,
				updateType=update_type
			)
			final_update_message_to_send = PrimitiveContentUpdateMessage(payload=update_payload_obj)

			if self.ui_connection_manager.get_connection_count(stream_id) > 0:
				json_message = final_update_message_to_send.model_dump_json(exclude_none=True)
				await self.ui_connection_manager.send_text(json_message, stream_id)
				logger.debug(f"[{server_id}] Sent primitive_content_update to binding '{target_binding}'.")
			else:
				logger.debug(
					f"[{server_id}] No clients for stream {stream_id}, skipping send for binding '{target_binding}'.")

		except AttributeError as ae:  # Catch issues with getattr on params if it's not structured as expected
			logger.error(
				f"[{server_id}] AttributeError processing 'notifications/message': {ae}. Params Type: {type(params)}, Params: {params!r}",
				exc_info=True)
		except ValidationError as ve:
			logger.error(
				f"[{server_id}] ValidationError creating UI update message: {ve}. Content: {content_to_send!r}",
				exc_info=True)
		except Exception as e:
			logger.error(
				f"[{server_id}] Unexpected error processing 'notifications/message': {e}. Params Type: {type(params)}, Params: {params!r}",
				exc_info=True)

	async def _handle_streaming_update(self, server_id: str, params: Union[Dict, Any]):
		if not hasattr(self, 'ui_connection_manager') or self.ui_connection_manager is None:
			logger.error(
				f"[{server_id}] UI ConnectionManager not available in _handle_streaming_update. Cannot proceed.")
			return

		logger.debug(
			f"[{server_id}] Handling 'app/streaming_log_update'. Params type: {type(params)}, Value: {params!r}")
		stream_id = f"mcp:{server_id}"

		try:
			target_binding: Optional[str] = None
			chunk_text: Any = None

			if hasattr(params, 'targetBinding') and hasattr(params, 'chunk'):
				target_binding = getattr(params, 'targetBinding', None)
				chunk_text = getattr(params, 'chunk', None)
			elif isinstance(params, dict):
				target_binding = params.get('targetBinding')
				chunk_text = params.get('chunk')
			else:
				logger.warning(
					f"[{server_id}] Params unexpected type {type(params)} for 'app/streaming_log_update'. Ignoring.")
				return

			if not target_binding or not isinstance(target_binding, str):
				logger.warning(
					f"[{server_id}] 'app/streaming_log_update' invalid/missing 'targetBinding'. Params: {params!r}. Ignoring.")
				return

			chunk_text_str = "" if chunk_text is None else str(chunk_text)
			if chunk_text is None:
				logger.warning(
					f"[{server_id}] 'app/streaming_log_update' missing 'chunk' for '{target_binding}'. Sending empty string.")

			logger.info(f"[{server_id}] Sending raw chunk to binding '{target_binding}': '{chunk_text_str[:100]}...'")

			raw_payload = PrimitiveContentUpdatePayload(
				targetBinding=target_binding,
				content=chunk_text_str,
				updateType="append"
			)
			raw_message_obj = PrimitiveContentUpdateMessage(payload=raw_payload)

			if self.ui_connection_manager.get_connection_count(stream_id) > 0:
				json_message = raw_message_obj.model_dump_json(exclude_none=True)
				await self.ui_connection_manager.send_text(json_message, stream_id)
				logger.debug(
					f"[{server_id}] Relayed raw chunk via PrimitiveContentUpdate to binding '{target_binding}'.")
			else:
				logger.debug(
					f"[{server_id}] No clients for stream {stream_id}, skipping raw chunk relay to '{target_binding}'.")
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating message for 'app/streaming_log_update': {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing 'app/streaming_log_update': {e}", exc_info=True)

	async def _handle_update_binding(self, server_id: str, params: Union[
		Dict, Any]):  # Or just `params: Dict` if you are confident it will always be a dict
		"""Handles the 'notifications/update_binding' message from the MCP server."""
		logger.debug(
			f"[{server_id}] Handling Update Binding Notification. Params type: {type(params)}, Value: {params!r}")
		stream_id = f"mcp:{server_id}"

		# Ensure you use the correct connection manager attribute (e.g., self.ui_connection_manager or self.connection_manager)
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected, skipping update_binding send.")
			return
		try:
			# The existing logic using .get() for dicts is appropriate here
			target_binding = params.get('binding') if isinstance(params, dict) else getattr(params, 'binding', None)
			content_payload = params.get('payload') if isinstance(params, dict) else getattr(params, 'payload', None)

			if not target_binding:
				logger.error(f"[{server_id}] Received update_binding with missing 'binding'. Params: {params!r}")
				return

			logger.info(f"[{server_id}] Relaying update for binding '{target_binding}' to frontend.")

			serializable_content = content_payload
			if hasattr(content_payload, 'model_dump'):  # For Pydantic models
				serializable_content = content_payload.model_dump(exclude_none=True)
			elif not isinstance(content_payload, (str, int, float, bool, list, dict, type(None))):
				logger.warning(
					f"[{server_id}] update_binding payload type {type(content_payload)} might not be directly serializable. Converting to str().")
				serializable_content = str(content_payload)

			update_payload_obj = PrimitiveContentUpdatePayload(
				targetBinding=target_binding,
				content=serializable_content,
				updateType="replace"
			)
			update_msg = PrimitiveContentUpdateMessage(payload=update_payload_obj)

			await self.ui_connection_manager.send_text(update_msg.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating UI update message for update_binding: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending update_binding notification: {e}", exc_info=True)

	async def _refresh_tools_and_notify_frontend(self, server_id: str):
		logger.info(f"[{server_id}] Background task started: Refreshing tool list.")
		session_wrapper, error = await self.get_or_create_session(server_id)
		if error or not session_wrapper:
			logger.error(f"[{server_id}] Background task: Cannot refresh tools, session not available ({error})")
			return

		actual_mcp_session: ClientSession = session_wrapper
		processed_tools: Optional[Dict[str, Any]] = None
		try:
			tools_timeout = self.settings.MCP_LIST_TOOLS_TIMEOUT  # Ensure this setting exists
			tools_result = await asyncio.wait_for(actual_mcp_session.list_tools(), timeout=tools_timeout)
			self._check_mcp_result_for_error(tools_result, f"ListTools (BG Update for {server_id})")

			tools_list = getattr(tools_result, 'tools', []) if tools_result else []
			processed_tools = self._process_discovered_tools(tools_list)

			await self._update_connection_state(server_id, {"tools": processed_tools})
			logger.info(
				f"[{server_id}] Background task: Updated tools: {list(processed_tools.keys() if processed_tools else [])}")

			stream_id = f"mcp:{server_id}"
			if self.ui_connection_manager.get_connection_count(stream_id) > 0 and processed_tools:
				tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
				for tool_name, tool_data in processed_tools.items():
					if not isinstance(tool_data, dict): continue
					try:
						tool_info = ToolSchemaInfo(
							name=tool_data.get('name', tool_name),
							description=tool_data.get('description'),
							input_schema=tool_data.get('inputSchema', tool_data.get('input_schema')),
							output_schema=tool_data.get('outputSchema', tool_data.get('output_schema'))
						)
						tool_schemas_for_payload[tool_name] = tool_info
					except Exception as schema_err:
						logger.error(
							f"[{server_id}] BG task: Error creating ToolSchemaInfo for '{tool_name}': {schema_err}")

				if tool_schemas_for_payload:
					schemas_payload = ToolSchemasPayload(server_id=server_id, tools=tool_schemas_for_payload)
					schemas_message = ToolSchemasMessage(payload=schemas_payload)
					try:
						schemas_json = schemas_message.model_dump_json(exclude_none=True, by_alias=True)
						await self.ui_connection_manager.send_text(schemas_json, stream_id)
						logger.info(f"[{server_id}] BG task: Sent updated tool schemas to frontend.")
					except Exception as send_err:
						logger.error(f"[{server_id}] BG task: Failed to send updated tool schemas: {send_err}",
									 exc_info=True)
				else:
					logger.warning(f"[{server_id}] BG task: No valid tool schemas processed to send.")
			elif self.ui_connection_manager.get_connection_count(stream_id) > 0:  # No tools, but clients connected
				logger.info(
					f"[{server_id}] BG task: Processed tools list is empty after refresh. Notifying frontend if necessary or sending empty list.")
				# Optionally send an empty tool list
				schemas_payload = ToolSchemasPayload(server_id=server_id, tools={})
				schemas_message = ToolSchemasMessage(payload=schemas_payload)
				await self.ui_connection_manager.send_text(
					schemas_message.model_dump_json(exclude_none=True, by_alias=True), stream_id)


		except asyncio.TimeoutError:
			logger.error(f"[{server_id}] Background task: Timeout error while re-fetching tools.")
		except McpError as e:
			logger.error(f"[{server_id}] Background task: MCP Error re-fetching tools: {getattr(e, 'error', e)!r}")
		except Exception as e:
			logger.error(f"[{server_id}] Background task: Unexpected error re-fetching tools: {e}", exc_info=True)
		finally:
			await self.release_session(server_id)
			logger.info(f"[{server_id}] Background task finished: Refreshing tool list.")

	async def _handle_tool_list_changed(self, server_id: str, params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] Handling ToolListChanged Notification. Scheduling background refresh...")
		asyncio.create_task(self._refresh_tools_and_notify_frontend(server_id))
		logger.debug(f"[{server_id}] Tool list refresh task created and handler finished.")

	async def _handle_resource_updated(self, server_id: str, params: Union[
		mcp_types.ResourceUpdatedNotificationParams, Dict]):
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected, skipping resource updated send.")
			return
		try:
			resource_uri = "<unknown_uri>"
			if hasattr(params, 'uri'):
				resource_uri = str(getattr(params, 'uri'))
			elif isinstance(params, dict) and 'uri' in params:
				resource_uri = str(params.get('uri'))
			else:
				logger.warning(f"[{server_id}] Could not extract 'uri' from ResourceUpdated params: {params!r}")

			logger.info(f"[{server_id}] Handling ResourceUpdated Notification for URI: {resource_uri}")
			payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceUpdated",
											 details={"uri": resource_uri})
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating MCPNotificationMessage for ResourceUpdated: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending ResourceUpdated notification: {e}", exc_info=True)

	async def _handle_resource_list_changed(self, server_id: str, params: Union[
		mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] Handling ResourceListChanged Notification.")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected, skipping resource list changed send.")
			return
		try:
			payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceListChanged")
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating MCPNotificationMessage for ResourceListChanged: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending ResourceListChanged notification: {e}", exc_info=True)

	async def _handle_prompt_list_changed(self, server_id: str,
										  params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] Handling PromptListChanged Notification.")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected, skipping prompt list changed send.")
			return
		try:
			payload = MCPNotificationPayload(server_id=server_id, notification_type="PromptListChanged")
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating MCPNotificationMessage for PromptListChanged: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending PromptListChanged notification: {e}", exc_info=True)

	async def _handle_cancelled_by_server(self, server_id: str, params: Union[
		mcp_types.CancelledNotificationParams, Dict]):
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients connected, skipping server cancellation send.")
			return
		try:
			request_id = getattr(params, 'requestId', params.get('requestId', '<unknown_request>'))
			logger.info(f"[{server_id}] Handling Cancelled Notification FROM SERVER for request ID: {request_id}")
			payload = MCPNotificationPayload(server_id=server_id, notification_type="CancelledByServer",
											 details={"requestId": request_id})
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating MCPNotificationMessage for CancelledByServer: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error processing/sending server cancellation notification: {e}", exc_info=True)

	# execute_tool: (Keep as is, but ensure get_or_create_session, release_session adapt)
	async def execute_tool(self, server_id: str, tool_name: str, params: Dict[str, Any], ws_stream_id: str) -> Tuple[
		Any, Optional[str]]:
		tool_call_timeout = self.settings.MCP_CALL_TOOL_TIMEOUT
		tool_exec_start_time = time.monotonic()
		logger.info(f"[{server_id}] Attempting tool '{tool_name}' for WS '{ws_stream_id}'. Params: {params}")

		# get_or_create_session now returns the actual ClientSession object directly if successful
		session_object, error_msg = await self.get_or_create_session(server_id)

		if error_msg or not session_object:
			# If _create_error_content returns ErrorData, it's fine. If dict, also fine for main.py
			return _create_error_content(error_msg or "Session not available",
										 INTERNAL_ERROR), error_msg or "Session not available"

		actual_mcp_session: ClientSession = session_object  # Type hint for clarity

		try:
			# tools are now fetched from the state updated by connect_and_prepare_server
			# This avoids repeated list_tools calls if tools are stable.
			# get_discovered_tools_internal already provides the cached tools.
			tools = await self.get_discovered_tools_internal(server_id)  # This is already how you did it

			if tools is None:
				err_msg = f"Tool discovery data missing or server '{server_id}' not ready."
				logger.error(f"[{server_id}] {err_msg}")
				return _create_error_content(err_msg, INTERNAL_ERROR), err_msg
			if tool_name not in tools:
				err_msg = f"Tool '{tool_name}' not available on server '{server_id}'. Available: {list(tools.keys())}"
				logger.error(f"[{server_id}] {err_msg}")
				return _create_error_content(err_msg, METHOD_NOT_FOUND, "METHOD_NOT_FOUND"), err_msg

			logger.debug(f"[{server_id}] Calling actual_mcp_session.call_tool('{tool_name}')...")
			tool_call_timeout = self.settings.MCP_CALL_TOOL_TIMEOUT
			tool_result = await asyncio.wait_for(
				actual_mcp_session.call_tool(name=tool_name, arguments=params),
				timeout=tool_call_timeout
			)
			tool_exec_duration = (time.monotonic() - tool_exec_start_time) * 1000
			logger.info(f"[{server_id}] Tool '{tool_name}' completed. (Took {tool_exec_duration:.2f} ms).")

			self._check_mcp_result_for_error(tool_result, f"ExecuteTool({tool_name}) for {server_id}")

			result_content = getattr(tool_result, 'content', None) if tool_result else None
			logger.debug(f"[{server_id}] Tool '{tool_name}' successful. Content type: {type(result_content)}")
			return result_content, None

		except asyncio.TimeoutError:
			error_message = f"Tool call '{tool_name}' timed out after {tool_call_timeout}s."
			logger.error(f"[{server_id}] {error_message}")
			return _create_error_content(error_message, INTERNAL_ERROR, "TIMEOUT"), error_message
		except anyio.ClosedResourceError as closed_err:  # Handle connection drops
			error_message = f"MCP connection closed during tool execution: {closed_err}"
			logger.error(f"[{server_id}] {error_message}", exc_info=False)
			await self._update_connection_state(server_id,
												{"status": "error", "error_message": error_message, "session": None,
												 "exit_stack": None})
			return _create_error_content(error_message, INTERNAL_ERROR), error_message
		except McpError as mcp_err:
			error_message = f"MCP protocol error during tool execution: {mcp_err.error}"
			logger.error(f"[{server_id}] {error_message}", exc_info=False)
			# Pass the mcp_err.error object itself, as it might be an ErrorData instance
			return getattr(mcp_err, 'error', error_message), error_message  # Return error object and string message
		except Exception as e:
			error_message = f"Unexpected error during tool execution '{tool_name}': {e}"
			logger.error(f"[{server_id}] {error_message}", exc_info=True)
			return _create_error_content(error_message, INTERNAL_ERROR), error_message
		finally:
			await self.release_session(server_id)  # Release ref count

	# --- Session Management (Refactored for DB-driven state) ---
	async def get_or_create_session(self, server_id: str) -> Tuple[Optional[ClientSession], Optional[str]]:
		logger.debug(f"[{server_id}] Request received for session.")
		async with self._connection_lock:
			connection_details = self.sse_connections.get(server_id)

			if not connection_details:
				error_msg = f"Server '{server_id}' not configured in manager."
				logger.error(error_msg)
				return None, error_msg

			db_config: Optional[MCPServerConfig] = connection_details.get("config_from_db")
			if not db_config:  # Should not happen if initialized correctly
				error_msg = f"Internal error: DB config missing for server '{server_id}'."
				logger.critical(error_msg)
				return None, error_msg

			if not db_config.is_active:
				error_msg = f"Server '{db_config.name}' (ID: {server_id}) is configured but inactive."
				logger.warning(error_msg)
				return None, error_msg

			current_status = connection_details.get("status")
			session = connection_details.get("session")

			if current_status == "connected" and session:
				connection_details["ref_count"] += 1
				logger.info(
					f"[{server_id}] Providing existing session for '{db_config.name}'. Ref count: {connection_details['ref_count']}")
				return session, None  # Return the actual ClientSession object
			elif current_status in ["pending_initialization", "pending_add", "pending_update", "disconnected", "error"]:
				# If not connected but should be active, attempt connection now (make it less proactive reliant)
				logger.warning(
					f"[{server_id}] Session for '{db_config.name}' not ready (Status: {current_status}). Attempting to establish/re-establish connection.")
				# Unlock before calling connect_and_prepare_server to avoid deadlock, as it also locks
				# However, connect_and_prepare_server will update state, so we need its result.
				# This makes on-demand connection tricky if connect_and_prepare_server is purely fire-and-forget.
				# For robust on-demand, connect_and_prepare_server should ideally return the session or indicate success/failure directly.
				# Given its current structure (returns bool, updates state internally):
				# We need to release the lock, call it, then re-acquire to check state. This is complex.

				# Simpler for now: rely on initial proactive connections or reconnections after updates.
				# If a session is requested and it's not 'connected', return error.
				# Users of get_or_create_session must handle this.
				error_msg = f"Server '{db_config.name}' (ID: {server_id}) connection not ready (Status: {current_status}). Last error: {connection_details.get('error_message', 'N/A')}."
				logger.warning(f"[{server_id}] {error_msg}")
				return None, error_msg
			elif current_status == "connecting":
				error_msg = f"Server '{db_config.name}' (ID: {server_id}) is currently connecting. Try again shortly."
				logger.info(f"[{server_id}] {error_msg}")  # Info, as this is transient
				return None, error_msg
			else:  # Should not be reached if statuses are comprehensive
				error_msg = f"Server '{db_config.name}' (ID: {server_id}) in unknown state: {current_status}."
				logger.error(f"[{server_id}] {error_msg}")
				return None, error_msg

	async def release_session(self, server_id: str):
		# (Keep as is - manages ref_count)
		logger.debug(f"[{server_id}] Received request to release session reference.")
		async with self._connection_lock:
			connection_details = self.sse_connections.get(server_id)
			if connection_details:  # Check if server still exists
				current_status = connection_details.get("status")
				# Only decrement ref_count if a session actually exists or could exist
				if current_status == "connected" or connection_details.get("session") is not None:
					ref_count = connection_details.get("ref_count", 0)
					if ref_count > 0:
						ref_count -= 1
						connection_details["ref_count"] = ref_count
						logger.info(
							f"[{server_id}] Decremented session reference for '{connection_details.get('config_from_db').name if connection_details.get('config_from_db') else server_id}'. New ref count: {ref_count}")
					else:
						logger.warning(
							f"[{server_id}] Attempted to release session with ref_count already at 0 for '{connection_details.get('config_from_db').name if connection_details.get('config_from_db') else server_id}'.")
				else:
					logger.debug(
						f"[{server_id}] No active session to release ref_count for (Status: {current_status}).")

	# --- State Management and Accessors ---
	async def _update_connection_state(self, server_id: str, updates: Dict[str, Any]):
		async with self._connection_lock:
			await self._update_connection_state_nolock(server_id, updates)

	async def _update_connection_state_nolock(self, server_id: str, updates: Dict[str, Any]):
		# Assumes lock is already held by caller
		if server_id in self.sse_connections:
			self.sse_connections[server_id].update(updates)
			# Avoid logging sensitive parts of 'updates' like full session objects
			loggable_updates = {k: v for k, v in updates.items() if
								k not in ["session", "exit_stack", "config_from_db", "config_for_connection"]}
			if "status" in updates: loggable_updates["status"] = updates["status"]  # ensure status is logged
			logger.debug(f"[{server_id}] Updated connection state (nolock): {loggable_updates}")
		else:
			logger.error(f"Attempted to update state (nolock) for unknown server_id: {server_id}")

	async def get_connection_details(self, server_id: str) -> Optional[Dict[str, Any]]:
		# (Adjust to read from config_from_db for name, etc.)
		logger.debug(f"[{server_id}] Getting connection details.")
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)
			if not details: return None

			db_conf: Optional[MCPServerConfig] = details.get("config_from_db")
			config_name = db_conf.name if db_conf else server_id
			config_url = db_conf.url if db_conf else "N/A"
			config_is_active = db_conf.is_active if db_conf else False

			# Create a copy for safety, exclude sensitive/large objects for general details view
			details_copy = {
				"id": server_id,  # This is MCPServerConfig.id as string
				"name": config_name,
				"url": config_url,
				"configured_active": config_is_active,
				"status": details.get("status"),
				"error_message": details.get("error_message"),
				"ref_count": details.get("ref_count"),
				# "config": details.get("config_for_connection"), # Maybe too verbose for health check
				"tools_available": list(details.get("tools", {}).keys()) if details.get("tools") is not None else [],
				# Return empty list
				"ui_layout_retrieved": details.get("ui_layout") is not None,
				"required_primitives": list(details.get("required_primitives", set())),
				"last_connect_attempt": details.get("last_connect_attempt"),
				"last_successful_connect": details.get("last_successful_connect"),
			}
			return details_copy

	# get_discovered_tools: (Keep as is - uses self.sse_connections)
	def get_discovered_tools(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting discovered tools (public).")
		# No lock needed for read if deepcopy is made from a locked read,
		# but direct access to mutable dicts within sse_connections should be careful.
		# For simplicity and safety with current structure:
		# Using a direct read here relies on the "tools" dict itself being replaced, not mutated in place by other threads.
		# If "tools" could be mutated, a lock and deepcopy would be safer here.
		# Given _update_connection_state replaces the "tools" dict, this direct read is likely okay.
		details = self.sse_connections.get(server_id)  # Read without lock for this public getter
		if details and details.get("status") == "connected":
			tools = details.get("tools")
			return copy.deepcopy(tools) if tools is not None else {}  # Return empty dict if None
		return None  # If not connected or no tools

	# get_discovered_tools_internal: (Keep as is - uses self.sse_connections with lock)
	async def get_discovered_tools_internal(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting discovered tools state (internal).")
		async with self._connection_lock:  # Lock for internal state access
			details = self.sse_connections.get(server_id)
			if details and details.get("status") == "connected" and details.get("tools") is not None:
				return details.get("tools")  # Return direct reference, assumes caller handles carefully
		return None

	# get_retrieved_ui_layout, get_required_primitives, is_server_ui_ready (Keep as is)
	async def get_retrieved_ui_layout(self, server_id: str) -> Optional[dict]:
		logger.debug(f"[{server_id}] Accessing retrieved UI layout.")
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected":
			layout = details.get("ui_layout")
			return copy.deepcopy(layout) if layout else None
		return None

	async def get_required_primitives(self, server_id: str) -> Optional[Set[str]]:
		logger.debug(f"[{server_id}] Accessing required primitives.")
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected" and details.get("ui_layout"):
			return details.get("required_primitives", set()).copy()
		return None  # Return None if not ready, or empty set? Consistent return type is good.

	async def is_server_ui_ready(self, server_id: str) -> bool:
		logger.debug(f"[{server_id}] Checking UI readiness.")
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)
		return bool(details and details.get("status") == "connected" and details.get("ui_layout") is not None)

	# --- Cleanup Logic (Modified to use _do_cleanup_for_server) ---
	async def cleanup_all_connections(self):
		shutdown_start_time = time.monotonic()
		logger.info("Initiating shutdown cleanup for all MCP connections...")
		server_ids_to_clean = []
		details_map_for_cleaning = {}

		async with self._connection_lock:
			server_ids_to_clean = list(self.sse_connections.keys())
			for server_id in server_ids_to_clean:
				details = self.sse_connections.get(server_id)
				if details and (details.get("exit_stack") or details.get("session")):
					details_map_for_cleaning[server_id] = details  # Store details for cleanup
					# Mark as disconnecting under lock
					await self._update_connection_state_nolock(server_id, {"status": "disconnecting", "ref_count": 0})

		tasks = []
		for server_id, details_to_clean in details_map_for_cleaning.items():
			logger.info(f"Scheduling shutdown cleanup task for server: {server_id}")
			tasks.append(self._do_cleanup_for_server(server_id, details_to_clean, "application shutdown"))
		# _do_cleanup_for_server will update the final status in sse_connections

		if tasks:
			results = await asyncio.gather(*tasks, return_exceptions=True)
			for i, result in enumerate(results):  # result might be None if _do_cleanup returns nothing
				server_id_cleaned = list(details_map_for_cleaning.keys())[i]
				if isinstance(result, Exception):
					logger.error(f"[{server_id_cleaned}] Error during bulk shutdown cleanup task: {result}")
				else:
					logger.info(f"[{server_id_cleaned}] Shutdown cleanup task completed.")
		shutdown_duration = (time.monotonic() - shutdown_start_time) * 1000
		logger.info(
			f"MCPConnectionManager shutdown cleanup initiated for {len(tasks)} connections (Total time: {shutdown_duration:.2f} ms).")

	# _cleanup_sse_connection (This was your old method, replaced by _do_cleanup_for_server logic)
	# The new _do_cleanup_for_server is called by remove_server_by_id and update_server_from_config.
	# _cleanup_all_connections now also uses _do_cleanup_for_server.

	# --- Internal Helper Methods (_safe_aclose, _check_mcp_result_for_error, _process_discovered_tools, _get_server_ui_layout, _extract_required_primitives, _find_details_by_session) ---
	# These should be largely fine. _find_details_by_session might need to compare against the actual session object if it's not just a dict.
	# Your _find_details_by_session looks okay as it compares `details.get("session") is session_instance`.

	async def _safe_aclose(self, resource: Optional[AsyncExitStack], server_id: str, context: str):
		if resource and hasattr(resource, 'aclose'):
			try:
				await resource.aclose()
				logger.debug(f"[{server_id}] Successfully closed resource during {context}.")
			except Exception as e:
				logger.error(f"[{server_id}] Error closing resource during {context}: {e}",
							 exc_info=True)  # Log full trace for close errors

	# _check_mcp_result_for_error (Keep as is)
	def _check_mcp_result_for_error(self, result: Any, operation_name: str):
		if not result:
			return
		error_content = None
		is_error = False
		ErrorData = getattr(mcp_types, 'ErrorData', None)
		TextContent = getattr(mcp_types, 'TextContent', None)
		if hasattr(result, 'isError') and result.isError:
			is_error = True
			error_content = getattr(result, 'content', 'Unknown Error Content')
		elif ErrorData and isinstance(result, ErrorData):
			is_error = True
			error_content = result
		elif isinstance(result, dict) and result.get("error"):
			is_error = True
			error_content = result.get("error")
		if is_error:
			logger.error(f"MCP Error during '{operation_name}': {error_content!r}")
			if ErrorData and isinstance(error_content, ErrorData):
				raise McpError(error=error_content)
			elif isinstance(error_content, dict) and 'message' in error_content and 'code' in error_content:
				try:
					mcp_error_data = ErrorData(**error_content) if ErrorData else error_content
					raise McpError(
						error=mcp_error_data)
				except Exception as construct_err:
					logger.error(f"Failed to construct McpError from dict: {construct_err}")
					raise Exception(
						f"MCP Operation '{operation_name}' failed: {error_content}")
			elif isinstance(error_content, list) and error_content and TextContent:
				first_item = error_content[0]
				if hasattr(first_item, 'text') and isinstance(first_item, TextContent):
					raise Exception(f"MCP Operation '{operation_name}' failed: {first_item.text}")
				else:
					raise Exception(f"MCP Operation '{operation_name}' failed with list content: {error_content!r}")
			else:
				raise Exception(f"MCP Operation '{operation_name}' failed: {error_content!r}")

	# _process_discovered_tools (Keep as is)
	def _process_discovered_tools(self, tools_list: Optional[List[Any]]) -> Dict[str, Any]:
		processed_tools: Dict[str, Any] = {}
		if not mcp_types:
			logger.warning("Cannot process tools: mcp_types missing.")
			return processed_tools
		if not tools_list:
			logger.debug("Tool list is empty or None.")
			return processed_tools
		logger.debug(f"Processing {len(tools_list)} discovered tools...")
		tool_class_to_check = None
		first_item = tools_list[0]
		if hasattr(mcp_types, 'Tool') and isinstance(first_item, mcp_types.Tool):
			tool_class_to_check = mcp_types.Tool
		elif hasattr(mcp_types, 'ToolInfo') and isinstance(first_item, mcp_types.ToolInfo):
			tool_class_to_check = mcp_types.ToolInfo
		else:
			logger.warning(f"Unexpected tool type in list: {type(first_item)}. Cannot process.")
			return processed_tools
		for tool_info in tools_list:
			if not isinstance(tool_info, tool_class_to_check):
				logger.warning(f"Skipping unexpected type: {type(tool_info)}")
				continue
			tool_name = getattr(tool_info, 'name', None)
			if not tool_name:
				logger.warning(f"Skipping tool missing 'name': {tool_info!r}")
				continue
			input_schema = getattr(tool_info, 'inputSchema', getattr(tool_info, 'input_schema', None))
			output_schema = getattr(tool_info, 'outputSchema', getattr(tool_info, 'output_schema', None))
			tool_data = {"name": tool_name, "description": getattr(tool_info, 'description', ''),
						 "input_schema": input_schema, "output_schema": output_schema}
			processed_tools[tool_name] = tool_data
		logger.debug(f"Finished processing tools: Found {len(processed_tools)} valid tools.")
		return processed_tools

	# _get_server_ui_layout (Keep as is)
	async def _get_server_ui_layout(self, session: ClientSession, server_id_for_log: str) -> Optional[dict]:
		tool_name = "get_ui_layout"
		logger.info(f"[{server_id_for_log}] Attempting to retrieve UI layout using tool: '{tool_name}'")
		try:
			tool_result: CallToolResult = await session.call_tool(name=tool_name, arguments=None)
			self._check_mcp_result_for_error(tool_result, tool_name)
			if tool_result and hasattr(tool_result, 'content') and isinstance(tool_result.content, list) and len(
					tool_result.content) == 1:
				content_item = tool_result.content[0]
				ui_layout = None
				if isinstance(content_item, dict):
					ui_layout = content_item
					logger.info(f"[{server_id_for_log}] Retrieved UI layout as dictionary.")
				elif mcp_types and isinstance(content_item, mcp_types.TextContent):
					try:
						ui_layout = json.loads(content_item.text)
						logger.info(f"[{server_id_for_log}] Parsed UI layout from TextContent.")
					except json.JSONDecodeError as json_err:
						logger.error(
							f"[{server_id_for_log}] Failed to parse JSON from '{tool_name}' TextContent: {json_err}.")
						return None
				else:
					logger.error(
						f"[{server_id_for_log}] Unexpected content item type ({type(content_item)}) from '{tool_name}'.")
					return None
				if not isinstance(ui_layout, dict) or 'id' not in ui_layout:
					logger.error(f"[{server_id_for_log}] Retrieved or parsed UI layout is invalid.")
					return None
				return ui_layout
			else:
				logger.error(
					f"[{server_id_for_log}] Unexpected content format from '{tool_name}': {getattr(tool_result, 'content', 'N/A')!r}")
				return None
		except McpError as e:
			err_code = getattr(getattr(e, 'error', None), 'code', None)
			if err_code == METHOD_NOT_FOUND:
				logger.warning(f"[{server_id_for_log}] UI layout tool '{tool_name}' not found on server.")
			else:
				logger.error(f"[{server_id_for_log}] MCP protocol error calling '{tool_name}': {e.error!r}",
							 exc_info=False)
			return None
		except Exception as e:
			logger.error(f"[{server_id_for_log}] Unexpected exception calling '{tool_name}': {e}", exc_info=True)
			return None

	# _extract_required_primitives (Keep as is)
	def _extract_required_primitives(self, layout: Optional[Dict[str, Any]]) -> Set[str]:
		primitives = set()
		if not layout or not isinstance(layout, dict):
			return primitives
		primitive_type = layout.get('type')
		if primitive_type and isinstance(primitive_type, str):
			primitives.add(primitive_type)
		children = layout.get('children')
		if children and isinstance(children, list):
			for child in children:
				if isinstance(child, dict):
					primitives.update(self._extract_required_primitives(child))
				else:
					logger.warning(
						f"Invalid child type ({type(child)}) in UI layout children: ID '{layout.get('id', 'unknown')}'.")
		return primitives

	# _find_details_by_session (Keep as is)
	async def _find_details_by_session(self, session_instance: ClientSession) -> Optional[Dict[str, Any]]:
		async with self._connection_lock:
			for details in self.sse_connections.values():
				if details.get("session") is session_instance:
					return details
		return None


async def get_mcp_connection_manager(request: mcp_types.Request) -> MCPConnectionManager:
	"""
	FastAPI dependency to get the MCPConnectionManager instance from app.state.
	The instance is expected to be initialized during the application lifespan.
	"""
	if not hasattr(request.app.state, 'mcp_connection_manager') or request.app.state.mcp_connection_manager is None:
		# This logger would need to be defined at the module level or imported
		logging.critical(
			"CRITICAL: MCPConnectionManager not initialized in app.state before access by dependency!")  # Use logging module directly or class's logger
		raise HTTPException()
	return request.app.state.mcp_connection_manager
