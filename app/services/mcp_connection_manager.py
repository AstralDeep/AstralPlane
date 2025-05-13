# app/services/mcp_connection_manager.py
import asyncio
import copy
import json
import logging
import time
from contextlib import AsyncExitStack
from datetime import datetime
from functools import partial
# Note: http.client.HTTPException was present in your original file,
# but for FastAPI dependencies, fastapi.HTTPException is typically used.
# The get_mcp_connection_manager function at the end uses fastapi.HTTPException.
# from http.client import HTTPException
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

from fastapi import Request as FastAPIRequest, WebSocket
from fastapi import HTTPException  # Use FastAPI's HTTPException

from app.config import Settings
from app.models.mcp_server_config_model import MCPServerConfig
from app.models.schemas import MCPLogEntry
from app.models.schemas import (
	PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
	ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage, MCPNotificationPayload, MCPNotificationMessage,
	MCPProgressPayload, MCPProgressMessage
)
from app.services.connection_manager import ConnectionManager

logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('mcp.client.sse').setLevel(logging.INFO)


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
		self.settings = settings
		self.ui_connection_manager = connection_manager

		self.sse_connections: Dict[str, Dict[str, Any]] = {}
		# Initialize the server_configs attribute as an empty dictionary
		self.server_configs: Dict[str, Dict[str, Any]] = {}  # <--- MODIFIED
		self._connection_lock = asyncio.Lock()

		logger.info("MCPConnectionManager initialized (empty, awaiting DB configurations).")

	async def initialize_servers_from_db(self, db_configs: List[MCPServerConfig]):
		logger.info(f"Loading and preparing {len(db_configs)} MCP server configurations from database.")
		new_server_configs_map: Dict[str, Dict[str, Any]] = {}

		async with self._connection_lock:
			existing_server_ids_to_clear = list(self.sse_connections.keys())
			self.server_configs.clear()  # <--- MODIFIED: Clear old server_configs

			for server_id_to_clear in existing_server_ids_to_clear:
				details = self.sse_connections.get(server_id_to_clear)
				if details and (details.get("exit_stack") or details.get("session")):
					await self._do_cleanup_for_server(server_id_to_clear, details, "re-initialization")
				if server_id_to_clear in self.sse_connections:  # Ensure key exists before deleting
					del self.sse_connections[server_id_to_clear]

			for db_config in db_configs:
				server_id_str = str(db_config.id)
				config_dict_for_server = {
					"id": server_id_str,
					"url": db_config.url,
					"name": db_config.name,
					"description": db_config.description,
					"is_active": db_config.is_active,  # <--- MODIFIED: Include is_active
					"transport": "sse"
				}

				new_server_configs_map[server_id_str] = config_dict_for_server  # <--- MODIFIED

				self.sse_connections[server_id_str] = {
					"server_id": server_id_str,
					"status": "pending_initialization",
					"ref_count": 0,
					"config_from_db": db_config,
					"config_for_connection": config_dict_for_server,
					"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
					"required_primitives": set(), "error_message": None,
					"last_connect_attempt": None, "last_successful_connect": None,
				}
				logger.debug(f"[{server_id_str}] Configured server '{db_config.name}' from DB.")

			self.server_configs = new_server_configs_map  # <--- MODIFIED
			logger.info(f"self.server_configs populated with {len(self.server_configs)} entries.")

		connect_tasks = []
		# Iterate over the newly populated sse_connections to decide on connections
		# Note: self.sse_connections is already updated under lock above.
		for server_id_str, details in self.sse_connections.items():
			config_for_connection = details["config_for_connection"]
			# Use is_active from the derived config_for_connection dictionary
			if config_for_connection.get("is_active", False):  # <--- MODIFIED logic
				logger.info(
					f"[{server_id_str}] Scheduling proactive connection for active server '{config_for_connection.get('name')}'.")
				connect_tasks.append(
					asyncio.create_task(
						self.connect_and_prepare_server(server_id_str, config_for_connection)
					)
				)
			else:
				await self._update_connection_state(server_id_str, {"status": "inactive_configured"})
				logger.info(
					f"[{server_id_str}] Server '{config_for_connection.get('name')}' is configured but inactive. Skipping proactive connect.")

		if connect_tasks:
			await asyncio.gather(*connect_tasks, return_exceptions=True)
			logger.info(f"Initial proactive connection attempts for {len(connect_tasks)} active servers complete.")
		else:
			logger.info("No active servers found in DB to connect to proactively.")

	async def connect_and_prepare_server(self, server_id: str, server_config: Dict[str, Any]) -> bool:
		server_name_for_logs = server_config.get('name', server_id)
		logger.info(f"[{server_id}] Proactive connection attempt starting for server '{server_name_for_logs}'...")

		server_url = server_config.get("url")
		if not server_url:
			logger.error(
				f"[{server_id}] Proactive connect failed for '{server_name_for_logs}': Missing 'url' in config.")
			await self._update_connection_state(server_id, {
				"status": "error",
				"error_message": "Missing URL in configuration"
			})
			return False

		exit_stack = AsyncExitStack()
		session: Optional[ClientSession] = None
		ui_layout: Optional[dict] = None
		processed_tools: Optional[dict] = None
		required_primitives: Set[str] = set()
		connect_time = time.monotonic()

		await self._update_connection_state(server_id, {
			"status": "connecting",
			"last_connect_attempt": connect_time,
			"error_message": None,
			"session": None, "exit_stack": None,
			"tools": None, "ui_layout": None, "required_primitives": set()
		})

		try:
			await exit_stack.__aenter__()
			read_stream, write_stream = await exit_stack.enter_async_context(sse_client(server_url))
			message_handler_with_id = partial(self._handle_incoming_message, server_id=server_id)
			mcp_client_instance: ClientSession = ClientSession(
				read_stream, write_stream,
				sampling_callback=self._default_sampling_callback,
				message_handler=message_handler_with_id
			)
			session = await exit_stack.enter_async_context(mcp_client_instance)

			if session is None:
				logger.critical(
					f"[{server_id}] Critical error: ClientSession context entry returned None for '{server_name_for_logs}'.")
				await self._safe_aclose(exit_stack, server_id, "critical session None cleanup")
				await self._update_connection_state(server_id, {
					"status": "error",
					"error_message": "Failed to enter ClientSession context (returned None)",
					"session": None, "exit_stack": None
				})
				return False

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

			await self._update_connection_state(server_id, {
				"status": "connected",
				"session": session, "exit_stack": exit_stack,
				"tools": processed_tools, "ui_layout": ui_layout,
				"required_primitives": required_primitives, "error_message": None,
				"last_successful_connect": time.monotonic(),
			})
			logger.info(
				f"[{server_id}] Proactive connection successful for '{server_name_for_logs}'. UI Layout Retrieved: {ui_layout is not None}")
			return True

		except (asyncio.TimeoutError, McpError, anyio.EndOfStream, anyio.ClosedResourceError, ConnectionRefusedError,
				Exception) as e:
			error_msg_detail = ""
			if isinstance(e, asyncio.TimeoutError):
				error_msg_detail = f"Operation timed out: {e}"
			elif isinstance(e, McpError):
				error_msg_detail = f"MCP Protocol Error: {getattr(e, 'error', e)}"
			elif isinstance(e, (anyio.EndOfStream, anyio.ClosedResourceError)):
				error_msg_detail = f"Connection closed unexpectedly: {e}"
			elif isinstance(e, ConnectionRefusedError):
				error_msg_detail = f"Connection refused by server: {e}"
			else:
				error_msg_detail = f"Unexpected error during connection: {e}"

			full_error_message = f"Proactive connection failed for '{server_name_for_logs}' ({server_id}). {error_msg_detail}"
			log_exc_info = not isinstance(e, (
				McpError, asyncio.TimeoutError, ConnectionRefusedError, anyio.EndOfStream, anyio.ClosedResourceError))
			logger.error(full_error_message, exc_info=log_exc_info)

			await self._safe_aclose(exit_stack, server_id, "proactive connection failure cleanup")
			await self._update_connection_state(server_id, {
				"status": "error", "error_message": str(e),
				"session": None, "exit_stack": None,
				"tools": None, "ui_layout": None, "required_primitives": set(),
			})
			return False

	async def add_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Adding new server '{db_config.name}' from live configuration update.")

		config_dict_for_server = {  # <--- MODIFIED: Consistent structure
			"id": server_id_str, "url": db_config.url, "name": db_config.name,
			"description": db_config.description,
			"is_active": db_config.is_active,
			"transport": "sse"
		}

		async with self._connection_lock:
			if server_id_str in self.sse_connections:
				logger.warning(f"[{server_id_str}] Attempted to add server that already exists. Consider using update.")
			# Not re-assigning here, assuming update logic would be called or this is an initial setup
			# where this state implies it's already being processed.

			self.server_configs[server_id_str] = config_dict_for_server  # <--- MODIFIED

			self.sse_connections[server_id_str] = {
				"server_id": server_id_str, "status": "pending_add", "ref_count": 0,
				"config_from_db": db_config, "config_for_connection": config_dict_for_server,
				"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
				"required_primitives": set(), "error_message": None,
				"last_connect_attempt": None, "last_successful_connect": None,
			}
			logger.debug(
				f"[{server_id_str}] Added server '{db_config.name}' to internal state (sse_connections and server_configs).")

		if config_dict_for_server["is_active"]:  # <--- MODIFIED: Use derived dict
			logger.info(f"[{server_id_str}] New server '{db_config.name}' is active. Attempting proactive connection.")
			await self.connect_and_prepare_server(server_id_str, config_dict_for_server)
		else:
			await self._update_connection_state(server_id_str, {"status": "inactive_configured"})
			logger.info(f"[{server_id_str}] New server '{db_config.name}' added as inactive.")

	async def update_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Updating server '{db_config.name}' from live configuration change.")

		config_dict_for_server = {  # <--- MODIFIED: Consistent structure
			"id": server_id_str, "url": db_config.url, "name": db_config.name,
			"description": db_config.description,
			"is_active": db_config.is_active,
			"transport": "sse"
		}
		needs_reconnect = False

		async with self._connection_lock:
			current_details = self.sse_connections.get(server_id_str)
			if not current_details:
				logger.error(f"[{server_id_str}] Update called for a server not in memory. Fallback: Adding server.")
				# Fallback: Treat as an add if it doesn't exist in sse_connections
				self.sse_connections[server_id_str] = {
					"server_id": server_id_str, "status": "pending_update_as_add", "ref_count": 0,
					"config_from_db": db_config, "config_for_connection": config_dict_for_server,
					"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
					"required_primitives": set(), "error_message": None,
					"last_connect_attempt": None, "last_successful_connect": None,
				}
				self.server_configs[server_id_str] = config_dict_for_server  # <--- MODIFIED
				current_details = self.sse_connections[server_id_str]  # Get newly created details
				needs_reconnect = True  # It's effectively a new server
			else:
				old_config_from_db: Optional[MCPServerConfig] = current_details.get("config_from_db")
				if old_config_from_db:
					if old_config_from_db.url != db_config.url or \
							old_config_from_db.is_active != db_config.is_active:  # Check critical fields
						needs_reconnect = True
				else:  # No old DB config means it might be a new add or inconsistent state
					needs_reconnect = True

				current_details["config_from_db"] = db_config
				current_details["config_for_connection"] = config_dict_for_server
				self.server_configs[server_id_str] = config_dict_for_server  # <--- MODIFIED

			if needs_reconnect:
				logger.info(
					f"[{server_id_str}] Configuration change for '{db_config.name}' requires connection update.")
				if current_details.get("session") or current_details.get("exit_stack") or current_details.get(
						"status") == "error":
					logger.info(f"[{server_id_str}] Cleaning up old connection for '{db_config.name}' before update...")
					await self._do_cleanup_for_server(server_id_str, current_details, "config update")

			if not config_dict_for_server["is_active"]:  # <--- MODIFIED: Use derived dict
				current_details["status"] = "inactive_configured"
				current_details["tools"] = None;
				current_details["ui_layout"] = None
				current_details["required_primitives"] = set();
				current_details["session"] = None
				current_details["exit_stack"] = None
				logger.info(f"[{server_id_str}] Server '{db_config.name}' updated to inactive.")

		if config_dict_for_server["is_active"] and needs_reconnect:  # <--- MODIFIED: Use derived dict
			logger.info(f"[{server_id_str}] Server '{db_config.name}' is active. Attempting connection/reconnection.")
			await self.connect_and_prepare_server(server_id_str, config_dict_for_server)
		elif not config_dict_for_server["is_active"] and needs_reconnect:
			logger.info(
				f"[{server_id_str}] Server '{db_config.name}' was made inactive. Ensured cleanup if previously connected.")

	async def remove_server_by_id(self, server_db_id_str: str):
		logger.info(f"[{server_db_id_str}] Removing server from live configuration.")
		connection_details = None
		removed_server_name = server_db_id_str

		async with self._connection_lock:
			removed_config_dict = self.server_configs.pop(server_db_id_str, None)  # <--- MODIFIED
			if removed_config_dict:
				removed_server_name = removed_config_dict.get('name', server_db_id_str)

			connection_details = self.sse_connections.pop(server_db_id_str, None)

		if connection_details:
			logger.info(
				f"[{server_db_id_str}] Cleaning up connection for removed server '{removed_server_name}'.")
			await self._do_cleanup_for_server(server_db_id_str, connection_details, "server removal")
			logger.info(
				f"[{server_db_id_str}] Server '{removed_server_name}' removed and cleaned up from sse_connections.")
		elif removed_config_dict:
			logger.info(
				f"[{server_db_id_str}] Server '{removed_server_name}' removed from server_configs (was not in sse_connections).")
		else:
			logger.warning(f"[{server_db_id_str}] Attempted to remove a server that was not found in manager state.")

	async def _do_cleanup_for_server(self, server_id: str, details: Dict[str, Any], context_msg: str):
		logger.debug(
			f"[{server_id}] Performing resource cleanup ({context_msg}). Current status: {details.get('status')}")
		exit_stack: Optional[AsyncExitStack] = details.get("exit_stack")
		await self._safe_aclose(exit_stack, server_id, f"cleanup context: {context_msg}")

		details["session"] = None;
		details["exit_stack"] = None
		details["tools"] = None;
		details["ui_layout"] = None
		details["required_primitives"] = set()
		if details.get("status") not in ["error", "inactive_configured"]:
			details["status"] = "disconnected"
		details["ref_count"] = 0
		logger.debug(f"[{server_id}] Resources cleaned up ({context_msg}). New status: {details.get('status')}")

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
			if hasattr(last_message, 'content') and isinstance(last_message.content, mcp_types.TextContent):
				server_message_text = last_message.content.text
		mock_response_text = f"Backend received: '{server_message_text}'. This is a mocked callback response."
		logger.info(f"[{server_id}] Sending mocked sampling response: '{mock_response_text}'")
		if mcp_types and hasattr(mcp_types, 'CreateMessageResult') and hasattr(mcp_types, 'TextContent'):
			return mcp_types.CreateMessageResult(role="assistant",
												 content=mcp_types.TextContent(type="text", text=mock_response_text),
												 model="mock-backend-callback-model", stopReason="endTurn")
		else:
			return {"role": "assistant", "content": {"type": "text", "text": mock_response_text},
					"model": "mock-backend-callback-model-dict", "stopReason": "endTurn"}

	async def _handle_incoming_message(self, message: Any, server_id: str):
		ServerNotification = getattr(mcp_types, 'ServerNotification', None)
		ProgressNotificationParams = getattr(mcp_types, 'ProgressNotificationParams', None)
		LoggingMessageNotificationParams = getattr(mcp_types, 'LoggingMessageNotificationParams',
												   None)  # Already defined in file
		UpdateBindingNotificationParams = getattr(mcp_types, 'UpdateBindingNotificationParams', None)
		ResourceUpdatedNotificationParams = getattr(mcp_types, 'ResourceUpdatedNotificationParams', None)
		CancelledNotificationParams = getattr(mcp_types, 'CancelledNotificationParams', None)
		# UpdateBindingNotificationParams is listed twice in original, one is enough.

		method_name = None;
		params = None;
		is_valid_structure = False

		if ServerNotification and isinstance(message, ServerNotification):
			notification_root = message.root
			method_name = getattr(notification_root, 'method', None)
			params = getattr(notification_root, 'params', None)
			is_valid_structure = True
		elif isinstance(message, dict):
			method_name = message.get('method');
			params = message.get('params');
			is_valid_structure = True
		elif hasattr(message, 'method') and hasattr(message, 'params'):
			method_name = getattr(message, 'method', None);
			params = getattr(message, 'params', None);
			is_valid_structure = True

		if not is_valid_structure: logger.warning(
			f"[{server_id}] Received unknown type via message_handler: {type(message)} - {message!r}"); return
		if not method_name: logger.warning(f"[{server_id}] Received message without 'method': {message!r}"); return

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
			elif method_name == "notifications/update_binding":
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
			# The second "notifications/update_binding" elif was redundant and removed.
			else:
				logger.warning(f"[{server_id}] Received unhandled notification method: {method_name}")
		except Exception as handler_exc:
			logger.error(f"[{server_id}] Error in handler for '{method_name}': {handler_exc}", exc_info=True)

	async def _handle_progress(self, server_id: str, params: Union[mcp_types.ProgressNotificationParams, Dict]):
		logger.debug(f"[{server_id}] Handling Progress Notification: {params}")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients for {stream_id}, skipping progress.")
			return
		try:
			token = params.get('progressToken') if isinstance(params, dict) else getattr(params, 'progressToken', None)
			percentage = params.get('percentage') if isinstance(params, dict) else getattr(params, 'percentage', None)
			message_text = params.get('message') if isinstance(params, dict) else getattr(params, 'message', None)
			title = params.get('title') if isinstance(params, dict) else getattr(params, 'title', None)

			payload = MCPProgressPayload(server_id=server_id, token=token, percentage=percentage, message=message_text,
										 title=title)
			message_to_send = MCPProgressMessage(payload=payload)
			await self.ui_connection_manager.send_text(message_to_send.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError MCPProgressMessage: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error sending progress: {e}", exc_info=True)

	async def _handle_mcp_log_message(self, server_id: str, params: Union[LoggingMessageNotificationParams, Dict, Any]):
		logger.debug(f"[{server_id}] Handling MCP Log ('notifications/message'). Params: {type(params)}, {params!r}")
		stream_id = f"mcp:{server_id}"
		default_log_binding = f"mcp_stream:{server_id}:log_messages"
		TARGETED_BINDING_PREFIX = f"mcp_stream:{server_id}:"
		EXPECTED_RAW_STREAM_LOGGER_NAME = f"{TARGETED_BINDING_PREFIX}raw_llm_stream"
		target_binding = default_log_binding;
		update_type: str = "append";
		content_to_send: Any = None

		try:
			log_level_raw = getattr(params, 'level', 'log');
			log_data: Any = getattr(params, 'data', None)
			logger_name: Optional[str] = getattr(params, 'logger', None)
			if log_data is None: logger.warning(
				f"[{server_id}] Log 'data' is None. Logger='{logger_name}'. Ignoring."); return
			final_content = log_data

			if isinstance(logger_name, str) and logger_name.startswith(TARGETED_BINDING_PREFIX):
				if logger_name == EXPECTED_RAW_STREAM_LOGGER_NAME:
					target_binding = logger_name;
					final_content = str(log_data);
					update_type = "append"
					content_to_send = final_content;
					logger.debug(f"[{server_id}] Raw stream chunk for '{target_binding}'.")
				else:
					target_binding = logger_name;
					update_type = "replace";
					content_to_send = final_content
					logger.debug(
						f"[{server_id}] Targeted UI update for '{target_binding}'. Type: {type(content_to_send).__name__}")
			else:
				log_level = str(log_level_raw).lower();
				valid_levels = {"error", "warning", "info", "debug", "log"}
				if log_level not in valid_levels: log_level = "log"
				log_data_str = str(final_content)
				try:
					log_entry = MCPLogEntry(level=log_level, message=log_data_str, timestamp=datetime.now())
					content_to_send = log_entry.model_dump(exclude_none=True)
					logger.debug(f"[{server_id}] Sending structured log to '{target_binding}'.")
				except ImportError:
					content_to_send = f"[{log_level.upper()}] {log_data_str}";
					logger.warning(
						f"[{server_id}] MCPLogEntry schema missing, sending plain text log to '{target_binding}'.")
				except Exception as log_entry_err:
					content_to_send = f"[LOG_ERROR:{log_level.upper()}] {log_data_str}";
					logger.error(
						f"[{server_id}] Error creating MCPLogEntry: {log_entry_err}", exc_info=True)

			if content_to_send is None: logger.error(
				f"[{server_id}] No content for notification. Params: {params!r}, Target: {target_binding}"); return
			update_payload_obj = PrimitiveContentUpdatePayload(targetBinding=target_binding, content=content_to_send,
															   updateType=update_type)
			final_update_message_to_send = PrimitiveContentUpdateMessage(payload=update_payload_obj)

			if self.ui_connection_manager.get_connection_count(stream_id) > 0:
				await self.ui_connection_manager.send_text(
					final_update_message_to_send.model_dump_json(exclude_none=True), stream_id)
				logger.debug(f"[{server_id}] Sent primitive_content_update to '{target_binding}'.")
			else:
				logger.debug(f"[{server_id}] No clients for {stream_id}, skipping send for '{target_binding}'.")
		except AttributeError as ae:
			logger.error(
				f"[{server_id}] AttributeError processing 'notifications/message': {ae}. Params: {type(params)}, {params!r}",
				exc_info=True)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError creating UI update: {ve}. Content: {content_to_send!r}",
						 exc_info=True)
		except Exception as e:
			logger.error(
				f"[{server_id}] Unexpected error in 'notifications/message': {e}. Params: {type(params)}, {params!r}",
				exc_info=True)

	async def _handle_streaming_update(self, server_id: str, params: Union[Dict, Any]):
		if not hasattr(self, 'ui_connection_manager') or self.ui_connection_manager is None: logger.error(
			f"[{server_id}] UI ConnectionManager missing. Cannot proceed."); return
		logger.debug(f"[{server_id}] Handling 'app/streaming_log_update'. Params: {type(params)}, {params!r}");
		stream_id = f"mcp:{server_id}"
		try:
			target_binding: Optional[str] = None;
			chunk_text: Any = None
			if hasattr(params, 'targetBinding') and hasattr(params, 'chunk'):
				target_binding = getattr(params, 'targetBinding', None);
				chunk_text = getattr(params, 'chunk', None)
			elif isinstance(params, dict):
				target_binding = params.get('targetBinding');
				chunk_text = params.get('chunk')
			else:
				logger.warning(
					f"[{server_id}] Params unexpected type {type(params)} for 'app/streaming_log_update'. Ignoring.");
				return
			if not target_binding or not isinstance(target_binding, str): logger.warning(
				f"[{server_id}] 'app/streaming_log_update' invalid/missing 'targetBinding'. Params: {params!r}. Ignoring."); return
			chunk_text_str = "" if chunk_text is None else str(chunk_text)
			if chunk_text is None: logger.warning(
				f"[{server_id}] 'app/streaming_log_update' missing 'chunk' for '{target_binding}'. Sending empty.")
			logger.info(f"[{server_id}] Sending raw chunk to '{target_binding}': '{chunk_text_str[:100]}...'")
			raw_payload = PrimitiveContentUpdatePayload(targetBinding=target_binding, content=chunk_text_str,
														updateType="append")
			raw_message_obj = PrimitiveContentUpdateMessage(payload=raw_payload)
			if self.ui_connection_manager.get_connection_count(stream_id) > 0:
				await self.ui_connection_manager.send_text(raw_message_obj.model_dump_json(exclude_none=True),
														   stream_id)
				logger.debug(f"[{server_id}] Relayed raw chunk to '{target_binding}'.")
			else:
				logger.debug(
					f"[{server_id}] No clients for {stream_id}, skipping raw chunk relay to '{target_binding}'.")
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError for 'app/streaming_log_update': {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error in 'app/streaming_log_update': {e}", exc_info=True)

	async def _handle_update_binding(self, server_id: str, params: Union[Dict, Any]):
		logger.debug(f"[{server_id}] Handling Update Binding Notification. Params: {type(params)}, {params!r}");
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0: logger.debug(
			f"[{server_id}] No clients, skipping update_binding."); return
		try:
			target_binding = params.get('binding') if isinstance(params, dict) else getattr(params, 'binding', None)
			content_payload = params.get('payload') if isinstance(params, dict) else getattr(params, 'payload', None)
			if not target_binding: logger.error(
				f"[{server_id}] update_binding missing 'binding'. Params: {params!r}"); return
			logger.info(f"[{server_id}] Relaying update for binding '{target_binding}'.")
			serializable_content = content_payload
			if hasattr(content_payload, 'model_dump'):
				serializable_content = content_payload.model_dump(exclude_none=True)
			elif not isinstance(content_payload, (str, int, float, bool, list, dict, type(None))):
				logger.warning(
					f"[{server_id}] update_binding payload type {type(content_payload)} not serializable. Converting to str().")
				serializable_content = str(content_payload)
			update_payload_obj = PrimitiveContentUpdatePayload(targetBinding=target_binding,
															   content=serializable_content, updateType="replace")
			update_msg = PrimitiveContentUpdateMessage(payload=update_payload_obj)
			await self.ui_connection_manager.send_text(update_msg.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError for update_binding: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error in update_binding: {e}", exc_info=True)

	async def _refresh_tools_and_notify_frontend(self, server_id: str):
		logger.info(f"[{server_id}] BG task: Refreshing tool list.");
		session_wrapper, error = await self.get_or_create_session(server_id)
		if error or not session_wrapper: logger.error(
			f"[{server_id}] BG task: Cannot refresh tools, session unavailable ({error})"); return
		actual_mcp_session: ClientSession = session_wrapper;
		processed_tools: Optional[Dict[str, Any]] = None
		try:
			tools_timeout = self.settings.MCP_LIST_TOOLS_TIMEOUT
			tools_result = await asyncio.wait_for(actual_mcp_session.list_tools(), timeout=tools_timeout)
			self._check_mcp_result_for_error(tools_result, f"ListTools (BG Update for {server_id})")
			tools_list = getattr(tools_result, 'tools', []) if tools_result else []
			processed_tools = self._process_discovered_tools(tools_list)
			await self._update_connection_state(server_id, {"tools": processed_tools})
			logger.info(
				f"[{server_id}] BG task: Updated tools: {list(processed_tools.keys() if processed_tools else [])}")
			stream_id = f"mcp:{server_id}"
			if self.ui_connection_manager.get_connection_count(stream_id) > 0 and processed_tools:
				tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
				for tool_name, tool_data in processed_tools.items():
					if not isinstance(tool_data, dict): continue
					try:
						tool_info = ToolSchemaInfo(name=tool_data.get('name', tool_name),
												   description=tool_data.get('description'),
												   input_schema=tool_data.get('inputSchema',
																			  tool_data.get('input_schema')),
												   output_schema=tool_data.get('outputSchema',
																			   tool_data.get('output_schema')))
						tool_schemas_for_payload[tool_name] = tool_info
					except Exception as schema_err:
						logger.error(
							f"[{server_id}] BG task: Error creating ToolSchemaInfo for '{tool_name}': {schema_err}")
				if tool_schemas_for_payload:
					schemas_payload = ToolSchemasPayload(server_id=server_id, tools=tool_schemas_for_payload)
					schemas_message = ToolSchemasMessage(payload=schemas_payload)
					try:
						await self.ui_connection_manager.send_text(
							schemas_message.model_dump_json(exclude_none=True, by_alias=True), stream_id)
						logger.info(f"[{server_id}] BG task: Sent updated tool schemas to frontend.")
					except Exception as send_err:
						logger.error(f"[{server_id}] BG task: Failed to send tool schemas: {send_err}", exc_info=True)
				else:
					logger.warning(f"[{server_id}] BG task: No valid tool schemas to send.")
			elif self.ui_connection_manager.get_connection_count(stream_id) > 0:
				logger.info(f"[{server_id}] BG task: Tools list empty after refresh. Sending empty list.")
				schemas_payload = ToolSchemasPayload(server_id=server_id, tools={})
				schemas_message = ToolSchemasMessage(payload=schemas_payload)
				await self.ui_connection_manager.send_text(
					schemas_message.model_dump_json(exclude_none=True, by_alias=True), stream_id)
		except asyncio.TimeoutError:
			logger.error(f"[{server_id}] BG task: Timeout re-fetching tools.")
		except McpError as e:
			logger.error(f"[{server_id}] BG task: MCP Error re-fetching tools: {getattr(e, 'error', e)!r}")
		except Exception as e:
			logger.error(f"[{server_id}] BG task: Unexpected error re-fetching tools: {e}", exc_info=True)
		finally:
			await self.release_session(server_id);
			logger.info(f"[{server_id}] BG task finished: Refresh tool list.")

	async def _handle_tool_list_changed(self, server_id: str, params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] ToolListChanged Notification. Scheduling refresh...");
		asyncio.create_task(self._refresh_tools_and_notify_frontend(server_id))

	async def _handle_resource_updated(self, server_id: str,
									   params: Union[mcp_types.ResourceUpdatedNotificationParams, Dict]):
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0: logger.debug(
			f"[{server_id}] No clients, skipping resource updated."); return
		try:
			resource_uri = str(getattr(params, 'uri')) if hasattr(params, 'uri') else (
				str(params.get('uri')) if isinstance(params, dict) else "<unknown_uri>")
			if resource_uri == "<unknown_uri>": logger.warning(
				f"[{server_id}] Could not extract 'uri' from ResourceUpdated: {params!r}")
			logger.info(f"[{server_id}] ResourceUpdated for URI: {resource_uri}")
			payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceUpdated",
											 details={"uri": resource_uri})
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError MCPNotificationMessage/ResourceUpdated: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error sending ResourceUpdated: {e}", exc_info=True)

	async def _handle_resource_list_changed(self, server_id: str,
											params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] ResourceListChanged Notification.");
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0: logger.debug(
			f"[{server_id}] No clients, skipping resource list changed."); return
		try:
			payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceListChanged")
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError MCPNotificationMessage/ResourceListChanged: {ve}",
						 exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error sending ResourceListChanged: {e}", exc_info=True)

	async def _handle_prompt_list_changed(self, server_id: str,
										  params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] PromptListChanged Notification.");
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0: logger.debug(
			f"[{server_id}] No clients, skipping prompt list changed."); return
		try:
			payload = MCPNotificationPayload(server_id=server_id, notification_type="PromptListChanged")
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError MCPNotificationMessage/PromptListChanged: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error sending PromptListChanged: {e}", exc_info=True)

	async def _handle_cancelled_by_server(self, server_id: str,
										  params: Union[mcp_types.CancelledNotificationParams, Dict]):
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0: logger.debug(
			f"[{server_id}] No clients, skipping server cancellation."); return
		try:
			request_id = getattr(params, 'requestId', params.get('requestId', '<unknown_request>'))
			logger.info(f"[{server_id}] Cancelled Notification FROM SERVER for request ID: {request_id}")
			payload = MCPNotificationPayload(server_id=server_id, notification_type="CancelledByServer",
											 details={"requestId": request_id})
			message = MCPNotificationMessage(payload=payload)
			await self.ui_connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError MCPNotificationMessage/CancelledByServer: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error sending server cancellation: {e}", exc_info=True)

	async def execute_tool(self, server_id: str, tool_name: str, params: Dict[str, Any], ws_stream_id: str) -> Tuple[
		Any, Optional[str]]:
		tool_call_timeout = self.settings.MCP_CALL_TOOL_TIMEOUT
		tool_exec_start_time = time.monotonic()
		logger.info(f"[{server_id}] Attempting tool '{tool_name}' for WS '{ws_stream_id}'. Params: {params}")
		session_object, error_msg = await self.get_or_create_session(server_id)
		if error_msg or not session_object: return _create_error_content(error_msg or "Session not available",
																		 INTERNAL_ERROR), error_msg or "Session not available"
		actual_mcp_session: ClientSession = session_object
		try:
			tools = await self.get_discovered_tools_internal(server_id)
			if tools is None: err_msg = f"Tool data missing or server '{server_id}' not ready."; logger.error(
				f"[{server_id}] {err_msg}"); return _create_error_content(err_msg, INTERNAL_ERROR), err_msg
			if tool_name not in tools: err_msg = f"Tool '{tool_name}' not on server '{server_id}'. Available: {list(tools.keys())}"; logger.error(
				f"[{server_id}] {err_msg}"); return _create_error_content(err_msg, METHOD_NOT_FOUND,
																		  "METHOD_NOT_FOUND"), err_msg
			logger.debug(f"[{server_id}] Calling actual_mcp_session.call_tool('{tool_name}')...");
			tool_call_timeout = self.settings.MCP_CALL_TOOL_TIMEOUT
			tool_result = await asyncio.wait_for(actual_mcp_session.call_tool(name=tool_name, arguments=params),
												 timeout=tool_call_timeout)
			logger.info(
				f"[{server_id}] Tool '{tool_name}' completed. (Took {(time.monotonic() - tool_exec_start_time) * 1000:.2f} ms).")
			self._check_mcp_result_for_error(tool_result, f"ExecuteTool({tool_name}) for {server_id}")
			result_content = getattr(tool_result, 'content', None) if tool_result else None
			logger.debug(f"[{server_id}] Tool '{tool_name}' successful. Content type: {type(result_content)}")
			return result_content, None
		except asyncio.TimeoutError:
			error_message = f"Tool '{tool_name}' timed out after {tool_call_timeout}s.";
			logger.error(
				f"[{server_id}] {error_message}");
			return _create_error_content(error_message, INTERNAL_ERROR,
										 "TIMEOUT"), error_message
		except anyio.ClosedResourceError as closed_err:
			error_message = f"MCP connection closed: {closed_err}";
			logger.error(f"[{server_id}] {error_message}",
						 exc_info=False);
			await self._update_connection_state(
				server_id, {"status": "error", "error_message": error_message, "session": None,
							"exit_stack": None});
			return _create_error_content(error_message,
										 INTERNAL_ERROR), error_message
		except McpError as mcp_err:
			error_message = f"MCP protocol error: {mcp_err.error}";
			logger.error(f"[{server_id}] {error_message}",
						 exc_info=False);
			return getattr(
				mcp_err, 'error', error_message), error_message
		except Exception as e:
			error_message = f"Unexpected error for tool '{tool_name}': {e}";
			logger.error(
				f"[{server_id}] {error_message}", exc_info=True);
			return _create_error_content(error_message,
										 INTERNAL_ERROR), error_message
		finally:
			await self.release_session(server_id)

	async def get_or_create_session(self, server_id: str) -> Tuple[Optional[ClientSession], Optional[str]]:
		logger.debug(f"[{server_id}] Request for session.");
		async with self._connection_lock:
			connection_details = self.sse_connections.get(server_id)
		if not connection_details: error_msg = f"Server '{server_id}' not configured."; logger.error(
			error_msg); return None, error_msg
		db_config: Optional[MCPServerConfig] = connection_details.get("config_from_db")
		if not db_config: error_msg = f"Internal error: DB config missing for '{server_id}'."; logger.critical(
			error_msg); return None, error_msg
		if not db_config.is_active: error_msg = f"Server '{db_config.name}' (ID: {server_id}) inactive."; logger.warning(
			error_msg); return None, error_msg
		current_status = connection_details.get("status");
		session = connection_details.get("session")
		if current_status == "connected" and session:
			connection_details["ref_count"] += 1;
			logger.info(
				f"[{server_id}] Existing session for '{db_config.name}'. Ref: {connection_details['ref_count']}");
			return session, None
		elif current_status in ["pending_initialization", "pending_add", "pending_update", "disconnected", "error"]:
			error_msg = f"Server '{db_config.name}' (ID: {server_id}) not ready (Status: {current_status}). Last error: {connection_details.get('error_message', 'N/A')}.";
			logger.warning(f"[{server_id}] {error_msg}");
			return None, error_msg
		elif current_status == "connecting":
			error_msg = f"Server '{db_config.name}' (ID: {server_id}) connecting. Try again.";
			logger.info(
				f"[{server_id}] {error_msg}");
			return None, error_msg
		else:
			error_msg = f"Server '{db_config.name}' (ID: {server_id}) in unknown state: {current_status}.";
			logger.error(
				f"[{server_id}] {error_msg}");
			return None, error_msg

	async def release_session(self, server_id: str):
		logger.debug(f"[{server_id}] Request to release session.");
		async with self._connection_lock:
			connection_details = self.sse_connections.get(server_id)
		if connection_details:
			current_status = connection_details.get("status")
			if current_status == "connected" or connection_details.get("session") is not None:
				ref_count = connection_details.get("ref_count", 0)
				if ref_count > 0:
					connection_details["ref_count"] = ref_count - 1;
					logger.info(
						f"[{server_id}] Decremented session ref for '{connection_details.get('config_from_db').name if connection_details.get('config_from_db') else server_id}'. New ref: {connection_details['ref_count']}")
				else:
					logger.warning(
						f"[{server_id}] Attempt to release session with ref_count 0 for '{connection_details.get('config_from_db').name if connection_details.get('config_from_db') else server_id}'.")
			else:
				logger.debug(f"[{server_id}] No active session to release (Status: {current_status}).")

	async def _update_connection_state(self, server_id: str, updates: Dict[str, Any]):
		async with self._connection_lock: await self._update_connection_state_nolock(server_id, updates)

	async def _update_connection_state_nolock(self, server_id: str, updates: Dict[str, Any]):
		if server_id in self.sse_connections:
			self.sse_connections[server_id].update(updates)
			loggable_updates = {k: v for k, v in updates.items() if
								k not in ["session", "exit_stack", "config_from_db", "config_for_connection"]}
			if "status" in updates: loggable_updates["status"] = updates["status"]
			logger.debug(f"[{server_id}] Updated connection state (nolock): {loggable_updates}")
		else:
			logger.error(f"Attempt to update state (nolock) for unknown server_id: {server_id}")

	async def get_connection_details(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting connection details.");
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if not details: return None
		db_conf: Optional[MCPServerConfig] = details.get("config_from_db");
		config_name = db_conf.name if db_conf else server_id
		config_url = db_conf.url if db_conf else "N/A";
		config_is_active = db_conf.is_active if db_conf else False
		details_copy = {"id": server_id, "name": config_name, "url": config_url, "configured_active": config_is_active,
						"status": details.get("status"), "error_message": details.get("error_message"),
						"ref_count": details.get("ref_count"),
						"tools_available": list(details.get("tools", {}).keys()) if details.get(
							"tools") is not None else [],
						"ui_layout_retrieved": details.get("ui_layout") is not None,
						"required_primitives": list(details.get("required_primitives", set())),
						"last_connect_attempt": details.get("last_connect_attempt"),
						"last_successful_connect": details.get("last_successful_connect")}
		return details_copy

	def get_discovered_tools(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting discovered tools (public).");
		details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected": tools = details.get("tools"); return copy.deepcopy(
			tools) if tools is not None else {}
		return None

	async def get_discovered_tools_internal(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting discovered tools (internal).");
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected" and details.get("tools") is not None: return details.get(
			"tools")
		return None

	async def get_retrieved_ui_layout(self, server_id: str) -> Optional[dict]:
		logger.debug(f"[{server_id}] Accessing UI layout.");
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected": layout = details.get("ui_layout"); return copy.deepcopy(
			layout) if layout else None
		return None

	async def get_required_primitives(self, server_id: str) -> Optional[Set[str]]:
		logger.debug(f"[{server_id}] Accessing required primitives.");
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected" and details.get("ui_layout"): return details.get(
			"required_primitives", set()).copy()
		return None

	async def is_server_ui_ready(self, server_id: str) -> bool:
		logger.debug(f"[{server_id}] Checking UI readiness.");
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		return bool(details and details.get("status") == "connected" and details.get("ui_layout") is not None)

	async def cleanup_all_connections(self):
		shutdown_start_time = time.monotonic();
		logger.info("Initiating shutdown for all MCP connections...")
		server_ids_to_clean = [];
		details_map_for_cleaning = {}
		async with self._connection_lock:
			server_ids_to_clean = list(self.sse_connections.keys())
			for server_id in server_ids_to_clean:
				details = self.sse_connections.get(server_id)
				if details and (details.get("exit_stack") or details.get("session")):
					details_map_for_cleaning[server_id] = details
					await self._update_connection_state_nolock(server_id, {"status": "disconnecting", "ref_count": 0})
		tasks = []
		for server_id, details_to_clean in details_map_for_cleaning.items():
			logger.info(f"Scheduling shutdown cleanup for server: {server_id}")
			tasks.append(self._do_cleanup_for_server(server_id, details_to_clean, "application shutdown"))
		if tasks:
			results = await asyncio.gather(*tasks, return_exceptions=True)
			for i, result in enumerate(results):
				server_id_cleaned = list(details_map_for_cleaning.keys())[i]
				if isinstance(result, Exception):
					logger.error(f"[{server_id_cleaned}] Error during shutdown cleanup: {result}")
				else:
					logger.info(f"[{server_id_cleaned}] Shutdown cleanup completed.")
		logger.info(
			f"MCPConnectionManager shutdown initiated for {len(tasks)} connections (Total: {(time.monotonic() - shutdown_start_time) * 1000:.2f} ms).")

	async def _safe_aclose(self, resource: Optional[AsyncExitStack], server_id: str, context: str):
		if resource and hasattr(resource, 'aclose'):
			try:
				await resource.aclose();
				logger.debug(f"[{server_id}] Closed resource during {context}.")
			except Exception as e:
				logger.error(f"[{server_id}] Error closing resource during {context}: {e}", exc_info=True)

	def _check_mcp_result_for_error(self, result: Any, operation_name: str):
		if not result: return
		error_content = None;
		is_error = False
		ErrorData = getattr(mcp_types, 'ErrorData', None);
		TextContent = getattr(mcp_types, 'TextContent', None)
		if hasattr(result, 'isError') and result.isError:
			is_error = True;
			error_content = getattr(result, 'content', 'Unknown Error')
		elif ErrorData and isinstance(result, ErrorData):
			is_error = True;
			error_content = result
		elif isinstance(result, dict) and result.get("error"):
			is_error = True;
			error_content = result.get("error")
		if is_error:
			logger.error(f"MCP Error during '{operation_name}': {error_content!r}")
			if ErrorData and isinstance(error_content, ErrorData):
				raise McpError(error=error_content)
			elif isinstance(error_content, dict) and 'message' in error_content and 'code' in error_content:
				try:
					mcp_error_data = ErrorData(**error_content) if ErrorData else error_content;
					raise McpError(
						error=mcp_error_data)
				except Exception as construct_err:
					logger.error(f"Failed to construct McpError: {construct_err}");
					raise Exception(
						f"MCP Op '{operation_name}' failed: {error_content}")
			elif isinstance(error_content, list) and error_content and TextContent:
				first_item = error_content[0]
				if hasattr(first_item, 'text') and isinstance(first_item, TextContent):
					raise Exception(f"MCP Op '{operation_name}' failed: {first_item.text}")
				else:
					raise Exception(f"MCP Op '{operation_name}' failed with list: {error_content!r}")
			else:
				raise Exception(f"MCP Op '{operation_name}' failed: {error_content!r}")

	def _process_discovered_tools(self, tools_list: Optional[List[Any]]) -> Dict[str, Any]:
		processed_tools: Dict[str, Any] = {};
		if not mcp_types: logger.warning("mcp_types missing."); return processed_tools
		if not tools_list: logger.debug("Tool list empty."); return processed_tools
		logger.debug(f"Processing {len(tools_list)} tools...");
		tool_class_to_check = None;
		first_item = tools_list[0]
		if hasattr(mcp_types, 'Tool') and isinstance(first_item, mcp_types.Tool):
			tool_class_to_check = mcp_types.Tool
		elif hasattr(mcp_types, 'ToolInfo') and isinstance(first_item, mcp_types.ToolInfo):
			tool_class_to_check = mcp_types.ToolInfo
		else:
			logger.warning(f"Unexpected tool type: {type(first_item)}.");
			return processed_tools
		for tool_info in tools_list:
			if not isinstance(tool_info, tool_class_to_check): logger.warning(
				f"Skipping type: {type(tool_info)}"); continue
			tool_name = getattr(tool_info, 'name', None);
			if not tool_name: logger.warning(f"Skipping tool missing 'name': {tool_info!r}"); continue
			input_schema = getattr(tool_info, 'inputSchema', getattr(tool_info, 'input_schema', None))
			output_schema = getattr(tool_info, 'outputSchema', getattr(tool_info, 'output_schema', None))
			processed_tools[tool_name] = {"name": tool_name, "description": getattr(tool_info, 'description', ''),
										  "input_schema": input_schema, "output_schema": output_schema}
		logger.debug(f"Processed tools: Found {len(processed_tools)}.");
		return processed_tools

	async def _get_server_ui_layout(self, session: ClientSession, server_id_for_log: str) -> Optional[dict]:
		tool_name = "get_ui_layout";
		logger.info(f"[{server_id_for_log}] Retrieving UI layout via tool: '{tool_name}'")
		try:
			tool_result: CallToolResult = await session.call_tool(name=tool_name, arguments=None)
			self._check_mcp_result_for_error(tool_result, tool_name)
			if tool_result and hasattr(tool_result, 'content') and isinstance(tool_result.content, list) and len(
					tool_result.content) == 1:
				content_item = tool_result.content[0];
				ui_layout = None
				if isinstance(content_item, dict):
					ui_layout = content_item;
					logger.info(f"[{server_id_for_log}] Retrieved UI layout (dict).")
				elif mcp_types and isinstance(content_item, mcp_types.TextContent):
					try:
						ui_layout = json.loads(content_item.text);
						logger.info(
							f"[{server_id_for_log}] Parsed UI layout (TextContent).")
					except json.JSONDecodeError as json_err:
						logger.error(
							f"[{server_id_for_log}] Failed to parse JSON from '{tool_name}' TextContent: {json_err}.");
						return None
				else:
					logger.error(
						f"[{server_id_for_log}] Unexpected content type ({type(content_item)}) from '{tool_name}'.");
					return None
				if not isinstance(ui_layout, dict) or 'id' not in ui_layout: logger.error(
					f"[{server_id_for_log}] UI layout invalid."); return None
				return ui_layout
			else:
				logger.error(
					f"[{server_id_for_log}] Unexpected content from '{tool_name}': {getattr(tool_result, 'content', 'N/A')!r}");
				return None
		except McpError as e:
			err_code = getattr(getattr(e, 'error', None), 'code', None)
			if err_code == METHOD_NOT_FOUND:
				logger.warning(f"[{server_id_for_log}] UI layout tool '{tool_name}' not found.")
			else:
				logger.error(f"[{server_id_for_log}] MCP error calling '{tool_name}': {e.error!r}", exc_info=False)
			return None
		except Exception as e:
			logger.error(f"[{server_id_for_log}] Unexpected error calling '{tool_name}': {e}",
						 exc_info=True);
			return None

	def _extract_required_primitives(self, layout: Optional[Dict[str, Any]]) -> Set[str]:
		primitives = set();
		if not layout or not isinstance(layout, dict): return primitives
		primitive_type = layout.get('type');
		if primitive_type and isinstance(primitive_type, str): primitives.add(primitive_type)
		children = layout.get('children')
		if children and isinstance(children, list):
			for child in children:
				if isinstance(child, dict):
					primitives.update(self._extract_required_primitives(child))
				else:
					logger.warning(
						f"Invalid child type ({type(child)}) in UI layout: ID '{layout.get('id', 'unknown')}'.")
		return primitives

	async def _find_details_by_session(self, session_instance: ClientSession) -> Optional[Dict[str, Any]]:
		async with self._connection_lock:
			for details in self.sse_connections.values():
				if details.get("session") is session_instance: return details
		return None


async def get_mcp_connection_manager(request: FastAPIRequest) -> MCPConnectionManager:
	if not hasattr(request.app.state, 'mcp_connection_manager') or request.app.state.mcp_connection_manager is None:
		logger.critical("CRITICAL: MCPConnectionManager not initialized in app.state!")
		raise HTTPException(status_code=500, detail="MCPConnectionManager not initialized in app.state")
	return request.app.state.mcp_connection_manager


# NEW dependency specifically for WebSocket routes
async def get_mcp_connection_manager_ws(websocket: WebSocket) -> MCPConnectionManager:
	"""
   FastAPI dependency to get the MCPConnectionManager instance from app.state
   for WebSocket contexts.
   """
	if not hasattr(websocket.app.state, 'mcp_connection_manager') or websocket.app.state.mcp_connection_manager is None:
		logger.critical("CRITICAL: MCPConnectionManager not initialized in app.state (WS context)!")
		# For WebSockets, you might want to handle this by closing the connection
		# rather than raising HTTPException, but for a critical component like this,
		# an error that stops the connection is appropriate if the manager is essential.
		# This will likely result in the WebSocket connection being terminated.
		raise RuntimeError("MCPConnectionManager not initialized in app.state for WebSocket.")
	return websocket.app.state.mcp_connection_manager
