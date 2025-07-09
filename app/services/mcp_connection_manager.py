# app/services/mcp_connection_manager.py
import asyncio
import copy
import json
import logging
import time
from contextlib import AsyncExitStack
from datetime import datetime
from functools import partial
from typing import Dict, Any, Optional, List, Tuple, Union, Set
import httpx

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

INITIAL_RETRY_DELAY_SECONDS = 5
MAX_RETRY_DELAY_SECONDS = 300  # 5 minutes
RETRY_BACKOFF_FACTOR = 2

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
		self.server_configs: Dict[str, Dict[str, Any]] = {}
		self._connection_lock = asyncio.Lock()

		self._reconnect_loop_task: Optional[asyncio.Task] = None
		self._health_check_loop_task: Optional[asyncio.Task] = None
		self._stop_reconnect_event = asyncio.Event()

		logger.info("MCPConnectionManager initialized (empty, awaiting DB configurations).")

	def _get_initial_server_state_dict(self, server_id_str: str, db_config: MCPServerConfig,
									   config_for_connection: Dict[str, Any]) -> Dict[str, Any]:
		return {
			"server_id": server_id_str,
			"status": "pending_initialization",
			# More statuses: CONNECTING, CONNECTED, DISCONNECTED, ERROR_RETRYING, ERROR_PERMANENT, INACTIVE
			"ref_count": 0,
			"config_from_db": db_config,
			"config_for_connection": config_for_connection,
			"session": None, "exit_stack": None, "tools": None, "ui_layout": None,
			"required_primitives": set(), "error_message": None,
			"last_connect_attempt_time": None,  # Renamed for clarity (timestamp)
			"last_successful_connect_time": None,  # Renamed for clarity (timestamp)
			"retry_count": 0,
			"current_retry_delay_seconds": INITIAL_RETRY_DELAY_SECONDS,
			"last_known_startup_id": None,
			"next_retry_time": None,  # Timestamp for next attempt
		}

	async def initialize_servers_from_db(self, db_configs: List[MCPServerConfig]):
		logger.info(f"Initializing MCPConnectionManager with {len(db_configs)} server configurations from database.")
		new_server_configs_map: Dict[str, Dict[str, Any]] = {}

		async with self._connection_lock:
			# --- Step 1: Identify changes from current state to new DB state ---
			current_managed_server_ids = set(self.sse_connections.keys())
			new_db_server_ids = {str(c.id) for c in db_configs}

			ids_to_remove_from_manager = current_managed_server_ids - new_db_server_ids
			for server_id_to_remove in ids_to_remove_from_manager:
				details = self.sse_connections.pop(server_id_to_remove, None)
				self.server_configs.pop(server_id_to_remove, None)  # Also remove from server_configs map
				if details:
					# Perform cleanup. Ensure status is set so retry loop ignores.
					details["status"] = "REMOVED"
					if "config_for_connection" in details:  # Should exist
						details["config_for_connection"]["is_active"] = False
					await self._do_cleanup_for_server(server_id_to_remove, details,
													  "removed during DB re-initialization")
				logger.info(f"[{server_id_to_remove}] Removed from manager as it's no longer in DB config.")

			# Clear server_configs to be repopulated from fresh db_configs
			self.server_configs.clear()

			# --- Step 2: Process each configuration from the database ---
			for db_config in db_configs:
				server_id_str = str(db_config.id)
				# Prepare the configuration dictionary used for connection parameters and state
				config_dict_for_server = {
					"id": server_id_str,
					"url": db_config.url,
					"name": db_config.name,
					"description": db_config.description,
					"is_active": db_config.is_active,  # Crucial for deciding to connect
					"transport": "sse"  # Assuming SSE, adjust if configurable
				}
				# Update the canonical server_configs map
				new_server_configs_map[server_id_str] = config_dict_for_server

				if server_id_str in self.sse_connections:
					# Server already known (was not removed above), update its configuration
					existing_details = self.sse_connections[server_id_str]
					old_conn_config = existing_details.get("config_for_connection", {})

					# Determine if a full reconnect/reset is needed
					config_changed_critically = (
							old_conn_config.get("url") != config_dict_for_server["url"] or
							old_conn_config.get("is_active") != config_dict_for_server["is_active"]
					)

					if config_changed_critically and (
							existing_details.get("session") or existing_details.get("exit_stack")):
						logger.info(
							f"[{server_id_str}] Critical config change for '{db_config.name}'. Cleaning up old session.")
						await self._do_cleanup_for_server(server_id_str, existing_details, "DB config critical change")

					# Update the stored configurations
					existing_details["config_from_db"] = db_config
					existing_details["config_for_connection"] = config_dict_for_server

					if config_changed_critically:
						# Reset status to trigger a new connection attempt by the logic below or by the retry loop
						existing_details["status"] = "pending_initialization"
						existing_details["error_message"] = None  # Clear previous errors
						existing_details["retry_count"] = 0
						existing_details["current_retry_delay_seconds"] = INITIAL_RETRY_DELAY_SECONDS
						existing_details["next_retry_time"] = None
					logger.debug(f"[{server_id_str}] Updated existing server '{db_config.name}' based on DB config.")
				else:
					# This is a new server not previously in sse_connections
					self.sse_connections[server_id_str] = self._get_initial_server_state_dict(
						server_id_str, db_config, config_dict_for_server
					)
					logger.debug(
						f"[{server_id_str}] Added new server '{db_config.name}' to manager based on DB config.")

			self.server_configs = new_server_configs_map  # Assign the fully populated map
			logger.info(f"Canonical server_configs map populated with {len(self.server_configs)} entries.")

		# --- Step 3: Prepare and execute initial connection tasks concurrently ---
		initial_connect_tasks = []
		server_ids_and_configs_for_initial_connect = []

		# Gather server_ids and their configs needing an initial connection attempt.
		# This read access to sse_connections is brief.
		async with self._connection_lock:
			for server_id_str, details in self.sse_connections.items():
				config_for_connection = details["config_for_connection"]
				# Only attempt connection if it's active and its status indicates it needs initialization.
				if config_for_connection.get("is_active", False) and details["status"] == "pending_initialization":
					server_ids_and_configs_for_initial_connect.append((server_id_str, config_for_connection))
				elif not config_for_connection.get("is_active", False):
					# If a server is marked as inactive, ensure its state reflects this.
					if details["status"] not in ["INACTIVE", "REMOVED"]:  # Avoid unnecessary updates if already correct
						await self._update_connection_state_nolock(server_id_str,
																   {"status": "INACTIVE", "next_retry_time": None})
					logger.info(
						f"[{server_id_str}] Server '{config_for_connection.get('name')}' is inactive. Skipping proactive connect.")

		# Create asyncio.Task for each connection attempt to run them concurrently.
		for server_id_str, config_for_connection in server_ids_and_configs_for_initial_connect:
			logger.info(
				f"[{server_id_str}] Scheduling concurrent proactive connection for active server '{config_for_connection.get('name')}'.")
			task = asyncio.create_task(
				self.connect_and_prepare_server(server_id_str, config_for_connection),
				name=f"initial_connect_{server_id_str}"  # Name is helpful for debugging (Python 3.8+)
			)
			initial_connect_tasks.append(task)

		if initial_connect_tasks:
			logger.info(f"Attempting to connect to {len(initial_connect_tasks)} active servers concurrently...")
			# asyncio.gather runs all tasks concurrently and waits for them to complete.
			results = await asyncio.gather(*initial_connect_tasks, return_exceptions=True)

			for i, result_or_exc in enumerate(results):
				# It's good practice to log the outcome of each initial attempt.
				# The task name helps identify which server the result belongs to.
				task_name = initial_connect_tasks[i].get_name()

				if isinstance(result_or_exc, Exception):
					logger.error(f"Initial connection task '{task_name}' raised an exception: {result_or_exc}")
				elif result_or_exc is False:  # connect_and_prepare_server returns bool
					logger.warning(
						f"Initial connection task '{task_name}' returned False (connection failed). It will be retried by the background loop if applicable.")
				elif result_or_exc is True:
					logger.info(f"Initial connection task '{task_name}' completed successfully.")
			# If connect_and_prepare_server had True/False, success/failure is logged within it too.

			logger.info(
				f"Initial proactive connection attempts for {len(initial_connect_tasks)} active servers have been processed.")
		else:
			logger.info(
				"No active servers required an immediate proactive connection attempt during DB initialization.")

	async def connect_and_prepare_server(self, server_id: str, server_config: Dict[str, Any]) -> bool:
		server_name_for_logs = server_config.get('name', server_id)

		async with self._connection_lock:  # Ensure status isn't changed by another task concurrently
			details = self.sse_connections.get(server_id)
			if not details:
				logger.error(f"[{server_id}] Attempted to connect non-configured server '{server_name_for_logs}'.")
				return False
			if details["status"] == "CONNECTING":  # Already an attempt in progress
				logger.info(f"[{server_id}] Connection attempt already in progress for '{server_name_for_logs}'.")
				return False  # Or True, depending on how you want to signal this

			await self._update_connection_state_nolock(server_id, {
				"status": "CONNECTING",
				"last_connect_attempt_time": time.monotonic(),
				"error_message": None,  # Clear previous error
				# "session": None, "exit_stack": None, # Cleaned by _do_cleanup or if previous attempt failed
				# "tools": None, "ui_layout": None, "required_primitives": set()
			})

		logger.info(f"[{server_id}] Connection attempt starting for server '{server_name_for_logs}'...")
		server_url = server_config.get("url")
		if not server_url:
			logger.error(f"[{server_id}] Connect failed for '{server_name_for_logs}': Missing 'url' in config.")
			await self._update_connection_state(server_id, {
				"status": "ERROR_PERMANENT",  # Configuration error, won't retry
				"error_message": "Missing URL in configuration",
				"next_retry_time": None,  # No retry for permanent error
				"retry_count": 0  # Reset
			})
			return False

		exit_stack = AsyncExitStack()
		required_primitives: Set[str] = set()

		try:
			await exit_stack.__aenter__()  # Important: manage exit_stack lifecycle
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
					"status": "ERROR_RETRYING",  # Could be transient
					"error_message": "Failed to enter ClientSession context (returned None)",
					"session": None, "exit_stack": None,  # Ensure cleaned
					"retry_count": self.sse_connections[server_id]["retry_count"] + 1,  # Access under lock or pass
					"next_retry_time": time.monotonic() + self.sse_connections[server_id]["current_retry_delay_seconds"]
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
				"status": "CONNECTED",
				"session": session, "exit_stack": exit_stack,  # Store the stack
				"tools": processed_tools, "ui_layout": ui_layout,
				"required_primitives": required_primitives, "error_message": None,
				"last_successful_connect_time": time.monotonic(),
				"retry_count": 0,  # Reset on success
				"current_retry_delay_seconds": INITIAL_RETRY_DELAY_SECONDS,  # Reset delay
				"next_retry_time": None
			})
			logger.info(
				f"[{server_id}] Connection successful for '{server_name_for_logs}'. UI Layout Retrieved: {ui_layout is not None}")
			return True

		except (asyncio.TimeoutError, McpError, anyio.EndOfStream, anyio.ClosedResourceError, ConnectionRefusedError,
				Exception) as e:
			# ... (your existing detailed error message construction) ...
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
			full_error_message = f"Connection failed for '{server_name_for_logs}' ({server_id}). {error_msg_detail}"
			log_exc_info = not isinstance(e, (
				McpError, asyncio.TimeoutError, ConnectionRefusedError, anyio.EndOfStream, anyio.ClosedResourceError))
			logger.error(full_error_message, exc_info=log_exc_info)

			await self._safe_aclose(exit_stack, server_id, "connection failure cleanup")  # Close the current stack

			async with self._connection_lock:  # Safely update retry state
				details = self.sse_connections[server_id]
				new_retry_count = details["retry_count"] + 1
				new_delay = min(details["current_retry_delay_seconds"] * RETRY_BACKOFF_FACTOR, MAX_RETRY_DELAY_SECONDS)

				await self._update_connection_state_nolock(server_id, {
					"status": "ERROR_RETRYING",
					"error_message": str(e),
					"session": None, "exit_stack": None,  # Ensure cleaned
					"tools": None, "ui_layout": None, "required_primitives": set(),
					"retry_count": new_retry_count,
					"current_retry_delay_seconds": new_delay,
					"next_retry_time": time.monotonic() + new_delay
				})
			return False

	async def _do_cleanup_for_server(self, server_id: str, details: Dict[str, Any], context_msg: str):
		logger.debug(
			f"[{server_id}] Performing resource cleanup ({context_msg}). Current status: {details.get('status')}")
		exit_stack: Optional[AsyncExitStack] = details.get("exit_stack")
		await self._safe_aclose(exit_stack, server_id, f"cleanup context: {context_msg}")

		# Don't reset retry_count or next_retry_time here, let the reconnect loop manage it
		# if it's a transient disconnect. If it's a permanent removal, these fields become irrelevant.
		new_status = "DISCONNECTED"
		if details.get("status") == "ERROR_PERMANENT":
			new_status = "ERROR_PERMANENT"
		elif details.get("status") == "INACTIVE":
			new_status = "INACTIVE"
		elif details.get("status") in ["error", "ERROR_RETRYING"] and details.get("config_for_connection", {}).get(
				"is_active"):
			new_status = "ERROR_RETRYING"  # Keep it in retry state if it was an error and server is active

		details.update({
			"session": None, "exit_stack": None,
			"tools": None, "ui_layout": None,
			"required_primitives": set(),
			"status": new_status,
			"ref_count": 0
		})
		# If status is now DISCONNECTED (and active), the retry loop will set next_retry_time
		if new_status == "DISCONNECTED" and details.get("config_for_connection", {}).get("is_active"):
			if details.get("next_retry_time") is None:  # Only if not already scheduled for retry
				details["next_retry_time"] = time.monotonic() + details["current_retry_delay_seconds"]

		logger.debug(f"[{server_id}] Resources cleaned up ({context_msg}). New status: {details.get('status')}")

	# --- Add, Update, Remove server methods need to interact with retry logic ---
	async def add_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Adding new server '{db_config.name}' from live configuration update.")
		config_dict_for_server = {
			"id": server_id_str, "url": db_config.url, "name": db_config.name,
			"description": db_config.description, "is_active": db_config.is_active,
			"transport": "sse"
		}

		async with self._connection_lock:
			if server_id_str in self.sse_connections:
				logger.warning(f"[{server_id_str}] Attempted to add server that already exists. Consider using update.")
				# Potentially call update_server_from_config here or return
				return

			self.server_configs[server_id_str] = config_dict_for_server
			self.sse_connections[server_id_str] = self._get_initial_server_state_dict(
				server_id_str, db_config, config_dict_for_server
			)
			# Mark for immediate check by retry loop if active
			if config_dict_for_server["is_active"]:
				self.sse_connections[server_id_str]["status"] = "DISCONNECTED"  # So retry loop picks it up
				self.sse_connections[server_id_str]["next_retry_time"] = time.monotonic()  # Try ASAP
			else:
				self.sse_connections[server_id_str]["status"] = "INACTIVE"

			logger.debug(f"[{server_id_str}] Added server '{db_config.name}' to internal state.")

	async def update_server_from_config(self, db_config: MCPServerConfig):
		server_id_str = str(db_config.id)
		logger.info(f"[{server_id_str}] Updating server '{db_config.name}' from live configuration change.")
		config_dict_for_server = {
			"id": server_id_str, "url": db_config.url, "name": db_config.name,
			"description": db_config.description, "is_active": db_config.is_active,
			"transport": "sse"
		}

		async with self._connection_lock:
			current_details = self.sse_connections.get(server_id_str)
			if not current_details:
				logger.error(f"[{server_id_str}] Update called for a server not in memory. Adding as new.")
				# Delegate to add_server_from_config logic by setting up initial state
				self.server_configs[server_id_str] = config_dict_for_server
				self.sse_connections[server_id_str] = self._get_initial_server_state_dict(
					server_id_str, db_config, config_dict_for_server
				)
				current_details = self.sse_connections[server_id_str]  # Get newly created details
				if config_dict_for_server["is_active"]:
					current_details["status"] = "DISCONNECTED"
					current_details["next_retry_time"] = time.monotonic()
				else:
					current_details["status"] = "INACTIVE"
				return  # Exit, retry loop will handle

			old_config = current_details.get("config_for_connection", {})
			needs_reconnect_check = (
					old_config.get("url") != config_dict_for_server["url"] or
					old_config.get("is_active") != config_dict_for_server["is_active"]
			)

			# Update configurations
			current_details["config_from_db"] = db_config
			current_details["config_for_connection"] = config_dict_for_server
			self.server_configs[server_id_str] = config_dict_for_server

			if needs_reconnect_check:
				logger.info(
					f"[{server_id_str}] Configuration change for '{db_config.name}' requires connection re-evaluation.")
				if current_details.get("session") or current_details.get("exit_stack"):
					await self._do_cleanup_for_server(server_id_str, current_details, "config update")

				# Reset retry state for re-evaluation by the loop
				current_details["retry_count"] = 0
				current_details["current_retry_delay_seconds"] = INITIAL_RETRY_DELAY_SECONDS
				current_details["error_message"] = None  # Clear old error

				if config_dict_for_server["is_active"]:
					current_details["status"] = "DISCONNECTED"  # Mark for retry loop
					current_details["next_retry_time"] = time.monotonic()  # Try ASAP
				else:
					current_details["status"] = "INACTIVE"
					current_details["next_retry_time"] = None  # No retry if inactive
			else:  # No critical change, but update active status if it changed
				if current_details["status"] != "INACTIVE" and not config_dict_for_server["is_active"]:
					if current_details.get("session") or current_details.get("exit_stack"):
						await self._do_cleanup_for_server(server_id_str, current_details, "made inactive")
					current_details["status"] = "INACTIVE"
					current_details["next_retry_time"] = None
				elif current_details["status"] == "INACTIVE" and config_dict_for_server["is_active"]:
					current_details["status"] = "DISCONNECTED"  # Was inactive, now active, needs connection
					current_details["next_retry_time"] = time.monotonic()

	async def remove_server_by_id(self, server_db_id_str: str):
		logger.info(f"[{server_db_id_str}] Removing server from live configuration.")
		connection_details = None
		removed_server_name = server_db_id_str  # Default if not found in server_configs

		async with self._connection_lock:
			# Remove from server_configs first to get its name if available
			removed_config_dict = self.server_configs.pop(server_db_id_str, None)
			if removed_config_dict:
				removed_server_name = removed_config_dict.get('name', server_db_id_str)

			# Then remove from sse_connections, which holds the live state
			connection_details = self.sse_connections.pop(server_db_id_str, None)

		if connection_details:
			logger.info(
				f"[{server_db_id_str}] Cleaning up connection for removed server '{removed_server_name}'."
			)
			# Before cleanup, modify details to ensure the retry loop completely ignores it
			# and cleanup proceeds correctly for a removed server.
			connection_details["status"] = "REMOVED"  # A definitive status indicating it's gone
			if "config_for_connection" in connection_details and isinstance(connection_details["config_for_connection"],
																			dict):
				connection_details["config_for_connection"]["is_active"] = False
			else:  # Ensure the key exists if it didn't, to prevent errors in retry loop logic if accessed before full cleanup
				connection_details["config_for_connection"] = {"is_active": False}

			await self._do_cleanup_for_server(server_db_id_str, connection_details, "server removal")
			logger.info(
				f"[{server_db_id_str}] Server '{removed_server_name}' removed and cleaned up from sse_connections."
			)
		elif removed_config_dict:  # Found in server_configs but not sse_connections
			logger.info(
				f"[{server_db_id_str}] Server '{removed_server_name}' removed from server_configs (was not in sse_connections, so no live connection to clean)."
			)
		else:  # Not found in either
			logger.warning(
				f"[{server_db_id_str}] Attempted to remove a server that was not found in manager state (neither server_configs nor sse_connections)."
			)

	# --- Reconnection Loop ---
	async def _reconnection_loop(self):
		logger.info("MCPConnectionManager: Reconnection loop started.")
		await asyncio.sleep(INITIAL_RETRY_DELAY_SECONDS)  # Initial grace period

		while not self._stop_reconnect_event.is_set():
			current_time = time.monotonic()
			server_ids_to_attempt_connect = []  # Renamed for clarity

			async with self._connection_lock:
				for server_id, details in self.sse_connections.items():
					config = details.get("config_for_connection", {})
					if not config.get("is_active", False):
						if details["status"] != "INACTIVE":
							details["status"] = "INACTIVE"  # Ensure inactive servers are marked
							details["next_retry_time"] = None  # No retry for inactive
						continue

					status = details.get("status")
					next_retry = details.get("next_retry_time")

					# Conditions to attempt connection:
					# 1. DISCONNECTED and it's time to retry (or no retry time set yet for a new DISCONNECTED)
					# 2. ERROR_RETRYING and it's time to retry
					# 3. pending_initialization (should ideally be caught by initial load, but as a fallback)
					if (status in ["DISCONNECTED", "ERROR_RETRYING"] and (
							next_retry is None or current_time >= next_retry)) or status == "pending_initialization":  # Fallback for pending
						server_ids_to_attempt_connect.append(server_id)

			if not server_ids_to_attempt_connect:
				# If no immediate tasks, wait a bit or until the next scheduled retry
				wait_time = 5.0  # Default poll interval
				async with self._connection_lock:
					active_retry_times = [
						d["next_retry_time"] for d in self.sse_connections.values()
						if d["config_for_connection"].get("is_active") and
						   d["status"] == "ERROR_RETRYING" and
						   d["next_retry_time"] is not None
					]
				if active_retry_times:
					earliest_next_retry = min(active_retry_times)
					wait_time = max(0, min(wait_time, earliest_next_retry - current_time))

				try:
					await asyncio.wait_for(self._stop_reconnect_event.wait(), timeout=wait_time)
				except asyncio.TimeoutError:
					pass  # Loop continues
				continue  # Re-evaluate servers

			logger.debug(
				f"Reconnect Loop: Preparing to attempt connections for {len(server_ids_to_attempt_connect)} servers: {server_ids_to_attempt_connect}")

			connect_tasks = []
			for server_id_to_retry in server_ids_to_attempt_connect:
				# Create tasks outside the main lock, but fetch necessary config under lock
				server_config_for_connect = None
				async with self._connection_lock:
					details = self.sse_connections.get(server_id_to_retry)
					if not details or not details["config_for_connection"].get("is_active"):
						continue  # Server removed or made inactive
					if details["status"] in ["CONNECTING", "CONNECTED"]:
						continue  # Already being handled or connected

					server_config_for_connect = details["config_for_connection"]

				if server_config_for_connect:
					task = asyncio.create_task(
						self.connect_and_prepare_server(server_id_to_retry, server_config_for_connect),
						name=f"reconnect_loop_{server_id_to_retry}"
					)
					connect_tasks.append(task)

			if connect_tasks:
				logger.info(f"Reconnect Loop: Concurrently attempting to connect to {len(connect_tasks)} servers.")
				await asyncio.gather(*connect_tasks, return_exceptions=True)
			# Results are logged within connect_and_prepare_server or by the exception in gather
			# After attempts, the loop will naturally re-evaluate statuses and next_retry_times in the next iteration.

			# Short pause to prevent tight spinning if all attempts fail quickly
			# The main wait logic is at the beginning of the loop now.
			await asyncio.sleep(0.1)

		logger.info("MCPConnectionManager: Reconnection loop stopped.")

	async def start_background_tasks(self):
		if self._reconnect_loop_task and not self._reconnect_loop_task.done():
			logger.info("Reconnect loop already running.")
		else:
			self._stop_reconnect_event.clear()
			self._reconnect_loop_task = asyncio.create_task(self._reconnection_loop())
			logger.info("MCPConnectionManager background reconnect task started.")

		# Add this block to start the health check loop
		if self._health_check_loop_task and not self._health_check_loop_task.done():
			logger.info("Health check loop already running.")
		else:
			self._stop_reconnect_event.clear()  # Ensure it's clear
			self._health_check_loop_task = asyncio.create_task(self._health_check_loop())
			logger.info("MCPConnectionManager background health check task started.")

	async def stop_background_tasks(self):
		# This block is completely replaced to handle both tasks correctly
		if self._reconnect_loop_task or self._health_check_loop_task:
			logger.info("Stopping MCPConnectionManager background tasks...")
			self._stop_reconnect_event.set()

			tasks_to_wait_for = []
			if self._reconnect_loop_task:
				tasks_to_wait_for.append(self._reconnect_loop_task)
			if self._health_check_loop_task:
				tasks_to_wait_for.append(self._health_check_loop_task)

			if tasks_to_wait_for:
				_finished, pending = await asyncio.wait(tasks_to_wait_for, timeout=5.0)
				for task in pending:
					logger.warning(f"Task {task.get_name()} did not stop in time, cancelling.")
					task.cancel()

			self._reconnect_loop_task = None
			self._health_check_loop_task = None
			logger.info("MCPConnectionManager background tasks stopped.")

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

		if not is_valid_structure:
			logger.warning(f"[{server_id}] Received unknown type via message_handler: {type(message)} - {message!r}")
			return
		if not method_name:
			logger.warning(f"[{server_id}] Received message without 'method': {message!r}")
			return

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

	async def _handle_mcp_log_message(self, server_id: str,
									  params: Union[mcp_types.LoggingMessageNotificationParams, Dict, Any]):
		# server_id is now the string ID from the database (e.g., "mcp_mock_chatviewbasic")
		logger.debug(f"[{server_id}] Handling MCP Log ('notifications/message'). Params: {type(params)}, {params!r}")

		# stream_id for ConnectionManager (UI WebSocket manager) is based on server_id
		stream_id_for_ui_manager = f"mcp:{server_id}"

		# --- MODIFICATION: default_log_binding uses server_id (the string PK) directly ---
		# This ensures it matches the updateBinding defined in app/mock_chatviewbasic.py using SERVER_ID_FOR_BINDING
		default_log_binding = f"mcp_stream:{server_id}:log_messages"

		# TARGETED_BINDING_PREFIX also uses server_id (the string PK)
		TARGETED_BINDING_PREFIX_BY_ID = f"mcp_stream:{server_id}:"
		EXPECTED_RAW_STREAM_LOGGER_NAME = f"{TARGETED_BINDING_PREFIX_BY_ID}raw_llm_stream"

		target_binding = default_log_binding  # Default for general logs to LogView
		update_type: str = "append"
		content_to_send: Any = None

		try:
			log_level_raw = getattr(params, 'level', 'log')
			log_data: Any = getattr(params, 'data', None)
			logger_name: Optional[str] = getattr(params, 'logger', None)  # Name of the logger on the MCP server side

			if log_data is None:
				logger.debug(f"[{server_id}] Log 'data' is None from MCP logger '{logger_name}'. Ignoring.")
				return

			final_content = log_data

			# Check if the log event from MCP was from a specifically named logger on the MCP server
			# intended for direct UI element updates (other than the main LogView).
			# This assumes logger_name, if it's a binding, would also use the server's string ID.
			if isinstance(logger_name, str) and logger_name.startswith(TARGETED_BINDING_PREFIX_BY_ID):
				target_binding = logger_name  # The logger_name itself is the full binding string
				if logger_name == EXPECTED_RAW_STREAM_LOGGER_NAME:
					final_content = str(log_data)  # Send raw text
					update_type = "append"
					content_to_send = final_content
					logger.debug(f"[{server_id}] Raw stream chunk for specific targetBinding '{target_binding}'.")
				else:
					content_to_send = final_content  # Could be dict, str, etc.
					update_type = "replace"  # Or "append", depending on the UI element's needs
					logger.debug(
						f"[{server_id}] Targeted UI update for specific targetBinding '{target_binding}'. Content type: {type(content_to_send).__name__}")
			else:
				# This is the path for general logs going to the main LogView (target_binding is already default_log_binding)
				log_level = str(log_level_raw).lower()
				valid_levels = {"error", "warning", "info", "debug", "log"}
				if log_level not in valid_levels:
					log_level = "log"

				log_data_str = str(final_content)  # Ensure message is a string

				try:
					# Create the structured log entry for the UI's LogView
					log_entry = MCPLogEntry(level=log_level, message=log_data_str, timestamp=datetime.now())
					content_to_send = log_entry.model_dump(exclude_none=True)
					logger.debug(
						f"[{server_id}] Prepared structured log for default LogView. TargetBinding: '{target_binding}'. Content: {content_to_send}")
				except Exception as log_entry_err:  # Catch broader errors, including potential Pydantic or ImportError
					content_to_send = f"[LOG_ERROR:{log_level.upper()}] {log_data_str}"  # Fallback content
					logger.error(
						f"[{server_id}] Error creating MCPLogEntry for target '{target_binding}': {log_entry_err}",
						exc_info=True)

			if content_to_send is None:
				logger.error(
					f"[{server_id}] No content prepared for notification. Original Params: {params!r}, Determined Target: {target_binding}")
				return

			update_payload_obj = PrimitiveContentUpdatePayload(targetBinding=target_binding, content=content_to_send,
															   updateType=update_type)
			final_update_message_to_send = PrimitiveContentUpdateMessage(payload=update_payload_obj)

			if self.ui_connection_manager.get_connection_count(stream_id_for_ui_manager) > 0:
				await self.ui_connection_manager.send_text(
					final_update_message_to_send.model_dump_json(exclude_none=True, by_alias=True),
					# by_alias for Pydantic field aliases
					stream_id_for_ui_manager
				)
				logger.debug(
					f"[{server_id}] Sent primitive_content_update to targetBinding '{target_binding}' for UI stream '{stream_id_for_ui_manager}'.")
			else:
				logger.debug(
					f"[{server_id}] No UI clients for stream '{stream_id_for_ui_manager}', skipping send for targetBinding '{target_binding}'.")

		except AttributeError as ae:
			logger.error(
				f"[{server_id}] AttributeError processing 'notifications/message': {ae}. Params: {type(params)}, {params!r}",
				exc_info=True)
		except ValidationError as ve:  # Assuming pydantic.ValidationError is imported
			logger.error(
				f"[{server_id}] ValidationError creating UI update for '{target_binding}': {ve}. Content: {content_to_send!r}",
				exc_info=True)
		except Exception as e:
			logger.error(
				f"[{server_id}] Unexpected error in _handle_mcp_log_message for '{target_binding}': {e}. Params: {type(params)}, {params!r}",
				exc_info=True)

	async def _handle_streaming_update(self, server_id: str, params: Union[Dict, Any]):
		if not hasattr(self, 'ui_connection_manager') or self.ui_connection_manager is None:
			logger.error(f"[{server_id}] UI ConnectionManager missing. Cannot proceed.")
			return
		logger.debug(f"[{server_id}] Handling 'app/streaming_log_update'. Params: {type(params)}, {params!r}")
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
		logger.debug(f"[{server_id}] Handling Update Binding Notification. Params: {type(params)}, {params!r}")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients, skipping update_binding.")
			return
		try:
			target_binding = params.get('binding') if isinstance(params, dict) else getattr(params, 'binding', None)
			content_payload = params.get('payload') if isinstance(params, dict) else getattr(params, 'payload', None)
			if not target_binding:
				logger.error(f"[{server_id}] update_binding missing 'binding'. Params: {params!r}")
				return
			logger.info(f"[{server_id}] Relaying update for binding '{target_binding}'.")
			serializable_content = content_payload
			if hasattr(content_payload, 'model_dump'):
				serializable_content = content_payload.model_dump(exclude_none=True)
			elif not isinstance(content_payload, (str, int, float, bool, list, dict, type(None))):
				logger.warning(
					f"[{server_id}] update_binding payload type {type(content_payload)} not serializable. Converting to str().")
				serializable_content = str(content_payload)
			update_payload_obj = PrimitiveContentUpdatePayload(targetBinding=target_binding, content=serializable_content, updateType="replace")
			update_msg = PrimitiveContentUpdateMessage(payload=update_payload_obj)
			await self.ui_connection_manager.send_text(update_msg.model_dump_json(exclude_none=True), stream_id)
		except ValidationError as ve:
			logger.error(f"[{server_id}] ValidationError for update_binding: {ve}", exc_info=True)
		except Exception as e:
			logger.error(f"[{server_id}] Error in update_binding: {e}", exc_info=True)

	async def _refresh_tools_and_notify_frontend(self, server_id: str):
		logger.info(f"[{server_id}] BG task: Refreshing tool list.")
		session_wrapper, error = await self.get_or_create_session(server_id)
		if error or not session_wrapper:
			logger.error(f"[{server_id}] BG task: Cannot refresh tools, session unavailable ({error})")
			return
		actual_mcp_session: ClientSession = session_wrapper
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
					if not isinstance(tool_data, dict):
						continue
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
			await self.release_session(server_id)
			logger.info(f"[{server_id}] BG task finished: Refresh tool list.")

	async def _handle_tool_list_changed(self, server_id: str, params: Union[mcp_types.NotificationParams, Dict, None]):
		logger.info(f"[{server_id}] ToolListChanged Notification. Scheduling refresh...")
		await asyncio.create_task(self._refresh_tools_and_notify_frontend(server_id))

	async def _handle_resource_updated(self, server_id: str,
									   params: Union[mcp_types.ResourceUpdatedNotificationParams, Dict]):
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients, skipping resource updated.")
			return
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
		logger.info(f"[{server_id}] ResourceListChanged Notification.")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients, skipping resource list changed.")
			return
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
		logger.info(f"[{server_id}] PromptListChanged Notification.")
		stream_id = f"mcp:{server_id}"
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients, skipping prompt list changed.")
			return
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
		if self.ui_connection_manager.get_connection_count(stream_id) == 0:
			logger.debug(f"[{server_id}] No clients, skipping server cancellation.")
			return
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
		logger.info(f"[{server_id}] execute_tool: About to call get_discovered_tools_internal.")
		tools = None
		try:
			tools = await self.get_discovered_tools_internal(server_id)
			if tools is None:
				err_msg = f"Tool data missing or server '{server_id}' not ready."
				logger.error(f"[{server_id}] {err_msg}")
				return _create_error_content(err_msg, INTERNAL_ERROR), err_msg
			if tool_name not in tools:
				err_msg = f"Tool '{tool_name}' not on server '{server_id}'. Available: {list(tools.keys())}"
				logger.error(f"[{server_id}] {err_msg}")
				return _create_error_content(err_msg, METHOD_NOT_FOUND, "METHOD_NOT_FOUND"), err_msg
			logger.debug(f"[{server_id}] Calling actual_mcp_session.call_tool('{tool_name}')...")
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
			error_message = f"Tool '{tool_name}' timed out after {tool_call_timeout}s."
			logger.error(f"[{server_id}] {error_message}")
			return _create_error_content(error_message, INTERNAL_ERROR, "TIMEOUT"), error_message
		except (anyio.ClosedResourceError, anyio.EndOfStream,
				ConnectionRefusedError) as conn_err:  # Catch specific connection errors
			error_message = f"MCP connection to '{server_id}' lost/failed during tool '{tool_name}': {conn_err}"
			logger.error(f"[{server_id}] {error_message}", exc_info=False)
			async with self._connection_lock:  # Safely update state
				details = self.sse_connections.get(server_id)
				if details:
					# This server had an active session that just died.
					# Trigger cleanup of the current session and mark for retry.
					# _do_cleanup_for_server will try to preserve ERROR_RETRYING state if appropriate
					await self._do_cleanup_for_server(server_id, details, "tool execution connection error")
					# Ensure it's marked for retry if active
					if details["config_for_connection"].get("is_active"):
						details["status"] = "ERROR_RETRYING"
						details["error_message"] = error_message
						# Reset retry count as this was a failure of an *active* session, not an initial connect failure
						details["retry_count"] = 0
						details["current_retry_delay_seconds"] = INITIAL_RETRY_DELAY_SECONDS
						details["next_retry_time"] = time.monotonic() + INITIAL_RETRY_DELAY_SECONDS
			return _create_error_content(error_message, INTERNAL_ERROR), error_message
		except McpError as mcp_err:
			error_message = f"MCP protocol error: {mcp_err.error}"
			logger.error(f"[{server_id}] {error_message}", exc_info=False)
			return getattr(
				mcp_err, 'error', error_message), error_message
		except asyncio.CancelledError:
			logger.error(f"[{server_id}] execute_tool: AWAIT ON get_discovered_tools_internal was CANCELLED.")
			# It's important to decide what execute_tool returns in this case.
			# Propagating might be best if the entire ui_action task is being cancelled.
			# For now, let it fall through to the 'tools is None' check, which will return an error.
			# OR: raise
			err_msg = f"Operation cancelled while retrieving tool data for server '{server_id}'."
			logger.error(f"[{server_id}] {err_msg}")
			return _create_error_content(err_msg, INTERNAL_ERROR, "CANCELLED"), err_msg  # Specific error
		except Exception as e:
			error_message = f"Unexpected error for tool '{tool_name}': {e}"
			logger.error(f"[{server_id}] {error_message}", exc_info=True)
			return _create_error_content(error_message, INTERNAL_ERROR), error_message
		finally:
			await self.release_session(server_id)
			if tools is None:
				err_msg = f"Tool data missing or server '{server_id}' not ready. (tools object was None after internal call)"
				logger.error(f"[{server_id}] {err_msg}")
				return _create_error_content(err_msg, INTERNAL_ERROR), err_msg

	# Modify get_or_create_session to reflect new states
	async def get_or_create_session(self, server_id: str) -> Tuple[Optional[ClientSession], Optional[str]]:
		logger.debug(f"[{server_id}] Request for session.")
		async with self._connection_lock:  # Lock for reading consistent state
			connection_details = self.sse_connections.get(server_id)

		if not connection_details:
			error_msg = f"Server '{server_id}' not configured."
			logger.error(error_msg)
			return None, error_msg

		config = connection_details.get("config_for_connection", {})
		server_name = config.get("name", server_id)

		if not config.get("is_active", False):
			error_msg = f"Server '{server_name}' (ID: {server_id}) is configured but INACTIVE."
			logger.warning(error_msg)
			return None, error_msg

		current_status = connection_details.get("status")
		session = connection_details.get("session")

		if current_status == "CONNECTED" and session:
			# Need to re-acquire lock for write if modifying ref_count
			async with self._connection_lock:
				# Re-fetch details in case state changed between locks
				connection_details_for_update = self.sse_connections.get(server_id)
				if connection_details_for_update and connection_details_for_update.get("status") == "CONNECTED":
					connection_details_for_update["ref_count"] += 1
					logger.info(
						f"[{server_id}] Existing session for '{server_name}'. Ref: {connection_details_for_update['ref_count']}")
					return connection_details_for_update.get("session"), None
				else:  # Status changed, treat as if not connected
					current_status = connection_details_for_update.get("status",
																	   "UNKNOWN") if connection_details_for_update else "UNKNOWN"
		# Fall through to error handling below

		# Handle non-connected states
		if current_status == "CONNECTING":
			error_msg = f"Server '{server_name}' (ID: {server_id}) is currently CONNECTING. Please try again shortly."
		elif current_status == "ERROR_RETRYING":
			error_msg = f"Server '{server_name}' (ID: {server_id}) is temporarily unavailable (retrying). Last error: {connection_details.get('error_message', 'N/A')}. Next attempt around {datetime.fromtimestamp(connection_details.get('next_retry_time', 0)).isoformat() if connection_details.get('next_retry_time') else 'soon'}."
		elif current_status == "ERROR_PERMANENT":
			error_msg = f"Server '{server_name}' (ID: {server_id}) has a permanent configuration error: {connection_details.get('error_message', 'N/A')}."
		elif current_status == "DISCONNECTED":
			error_msg = f"Server '{server_name}' (ID: {server_id}) is currently DISCONNECTED and awaiting reconnection. Please try again shortly."
		elif current_status == "INACTIVE":  # Should have been caught earlier, but as a safeguard
			error_msg = f"Server '{server_name}' (ID: {server_id}) is INACTIVE."
		else:  # pending_initialization, etc.
			error_msg = f"Server '{server_name}' (ID: {server_id}) is not ready (Status: {current_status}). Last error: {connection_details.get('error_message', 'N/A')}."

		logger.warning(f"[{server_id}] Session not available: {error_msg}")
		return None, error_msg

	async def release_session(self, server_id: str):
		logger.debug(f"[{server_id}] Request to release session.")
		async with self._connection_lock:
			connection_details = self.sse_connections.get(server_id)
		if connection_details:
			current_status = connection_details.get("status")
			if current_status == "connected" or connection_details.get("session") is not None:
				ref_count = connection_details.get("ref_count", 0)
				if ref_count > 0:
					connection_details["ref_count"] = ref_count - 1
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
			target_dict = self.sse_connections[server_id]

			# Log before update
			logger.info(
				f"[{server_id}] _update_connection_state_nolock: Updating. Current status='{target_dict.get('status')}', UI Layout present before update: {target_dict.get('ui_layout') is not None}")
			logger.debug(
				f"[{server_id}] _update_connection_state_nolock: Updates to apply: { {k: (type(v).__name__ if k in ['session', 'exit_stack', 'ui_layout', 'tools'] else v) for k, v in updates.items()} }")

			target_dict.update(updates)  # The actual update

			# Log after update - this will show the new state of critical fields
			log_summary = {
				"status": target_dict.get("status"),
				"error_message": target_dict.get("error_message"),
				"tools_set": target_dict.get("tools") is not None,
				"ui_layout_set": target_dict.get("ui_layout") is not None,  # CRITICAL CHECK
				"session_set": target_dict.get("session") is not None,
				"ref_count": target_dict.get("ref_count")
			}
			logger.info(f"[{server_id}] _update_connection_state_nolock: State AFTER update. Summary: {log_summary}")
			if "ui_layout" in updates:  # If ui_layout was part of the keys in the 'updates' dict
				logger.info(
					f"[{server_id}] _update_connection_state_nolock: 'ui_layout' key was in updates. Object type: {type(updates['ui_layout']).__name__}, Is None: {updates['ui_layout'] is None}")
			elif "ui_layout" in target_dict:  # Check if it's in the target_dict generally
				logger.info(
					f"[{server_id}] _update_connection_state_nolock: 'ui_layout' key exists in target_dict. Object type: {type(target_dict['ui_layout']).__name__}, Is None: {target_dict['ui_layout'] is None}")

		else:
			logger.error(f"Attempt to update state (nolock) for unknown server_id: {server_id}")

	async def get_connection_details(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.debug(f"[{server_id}] Getting connection details.")
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)
		if not details: return None

		config = details.get("config_for_connection", {})
		config_name = config.get("name", server_id)
		config_url = config.get("url", "N/A")
		config_is_active = config.get("is_active", False)

		details_copy = {
			"id": server_id, "name": config_name, "url": config_url,
			"configured_active": config_is_active,
			"status": details.get("status"),
			"error_message": details.get("error_message"),
			"ref_count": details.get("ref_count"),
			"tools_available": list(details.get("tools", {}).keys()) if details.get("tools") is not None else [],
			"ui_layout_retrieved": details.get("ui_layout") is not None,
			"required_primitives": list(details.get("required_primitives", set())),
			"last_connect_attempt_time": datetime.fromtimestamp(
				details["last_connect_attempt_time"]).isoformat() if details.get("last_connect_attempt_time") else None,
			"last_successful_connect_time": datetime.fromtimestamp(
				details["last_successful_connect_time"]).isoformat() if details.get(
				"last_successful_connect_time") else None,
			"retry_count": details.get("retry_count"),
			"current_retry_delay_seconds": details.get("current_retry_delay_seconds"),
			"next_retry_time": datetime.fromtimestamp(details["next_retry_time"]).isoformat() if details.get(
				"next_retry_time") else None,
		}
		return details_copy

	async def get_discovered_tools_internal(self, server_id: str) -> Optional[Dict[str, Any]]:
		logger.critical(f"CRITICAL_TEST: ENTERED get_discovered_tools_internal for server_id: {server_id}")

		try:
			logger.info(f"[{server_id}] get_discovered_tools_internal: Step 0 - Before first await (lock).")
			async with self._connection_lock:
				logger.info(f"[{server_id}] get_discovered_tools_internal: Step 1 - Lock acquired.")
				details = self.sse_connections.get(server_id)
				logger.info(
					f"[{server_id}] get_discovered_tools_internal: Step 2 - 'details' obtained: {details is not None}")

			if not details:
				logger.warning(
					f"[{server_id}] get_discovered_tools_internal: Step 3a - No details found after lock. Returning None.")
				return None
			logger.info(f"[{server_id}] get_discovered_tools_internal: Step 3b - Details ARE present.")

			status_val = details.get("status", "STATUS_KEY_MISSING")
			tools_obj = details.get("tools")
			tools_data_is_none_val = tools_obj is None

			logger.info(
				f"[{server_id}] get_discovered_tools_internal: Step 4 - FINAL CHECK. Status='{status_val}', Tools data is None: {tools_data_is_none_val}")

			if status_val == "CONNECTED" and not tools_data_is_none_val:
				logger.info(
					f"[{server_id}] get_discovered_tools_internal: Conditions MET. Returning tools data (type: {type(tools_obj).__name__}).")
				return tools_obj

			logger.warning(
				f"[{server_id}] get_discovered_tools_internal: Conditions NOT MET. Status: '{status_val}', Tools data is None: {tools_data_is_none_val}. Returning None.")
			return None

		except asyncio.CancelledError:  # Catch CancelledError specifically first
			logger.error(
				f"[{server_id}] CAUGHT CancelledError IN get_discovered_tools_internal. Task was cancelled. Propagating.")
			raise  # Re-raise CancelledError is often the correct thing to do
		except BaseException as e_base:  # Catch BaseException to see if it's something like SystemExit or KeyboardInterrupt
			logger.error(
				f"[{server_id}] CAUGHT BaseException (e.g. SystemExit, KeyboardInterrupt, or other non-Exception) IN get_discovered_tools_internal: {type(e_base).__name__} - {e_base!r}",
				exc_info=True)
			# Depending on the BaseException, you might re-raise or return None
			if isinstance(e_base, (SystemExit, KeyboardInterrupt)):
				raise  # Definitely re-raise these
			return None  # For other BaseExceptions, treat as failure to get tools

	async def get_retrieved_ui_layout(self, server_id: str) -> Optional[dict]:
		# Ensure this method is being called by ProjectViewsService
		logger.info(f"ENTERED get_retrieved_ui_layout for server_id: {server_id}")  # ADD THIS
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)

		if not details:
			logger.warning(f"[{server_id}] get_retrieved_ui_layout: No details found in sse_connections.")
			return None

		current_status = details.get("status")
		ui_layout_obj = details.get("ui_layout")

		logger.info(
			f"[{server_id}] get_retrieved_ui_layout check: Status='{current_status}', UI Layout Object Present: {ui_layout_obj is not None} (Type: {type(ui_layout_obj).__name__ if ui_layout_obj is not None else 'NoneType'})")

		if current_status == "CONNECTED" and ui_layout_obj is not None:
			try:
				copied_layout = copy.deepcopy(ui_layout_obj)  # deepcopy is good for safety
				logger.info(f"[{server_id}] Successfully retrieved and deepcopied UI layout. Returning layout.")
				return copied_layout
			except Exception as e:
				logger.error(f"[{server_id}] Error during deepcopy of UI layout: {e}", exc_info=True)
				return None
		else:
			logger.warning(
				f"[{server_id}] UI Layout not available or status not CONNECTED for get_retrieved_ui_layout. Status: '{current_status}', Layout None: {ui_layout_obj is None}. Returning None.")
			return None

	async def get_discovered_tools(self, server_id: str) -> Optional[Dict[str, Any]]:
		# Ensure this method is being called by the websocket endpoint in main.py
		logger.info(f"ENTERED get_discovered_tools for server_id: {server_id}")  # ADD THIS
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)

		if not details:
			logger.warning(f"[{server_id}] get_discovered_tools: No details found in sse_connections.")
			return None

		current_status = details.get("status")
		tools_obj = details.get("tools")

		logger.info(
			f"[{server_id}] get_discovered_tools check: Status='{current_status}', Tools Object Present: {tools_obj is not None} (Count: {len(tools_obj) if isinstance(tools_obj, dict) else 'N/A'})")

		if current_status == "CONNECTED" and tools_obj is not None:
			try:
				copied_tools = copy.deepcopy(tools_obj)
				logger.info(f"[{server_id}] Successfully retrieved and deepcopied tools. Returning tools.")
				return copied_tools
			except Exception as e:
				logger.error(f"[{server_id}] Error during deepcopy of tools: {e}", exc_info=True)
				return None  # Or {}
		else:
			logger.warning(
				f"[{server_id}] Tools not available or status not CONNECTED for get_discovered_tools. Status: '{current_status}', Tools None: {tools_obj is None}. Returning None.")
			return None

	async def get_required_primitives(self, server_id: str) -> Optional[Set[str]]:
		logger.debug(f"[{server_id}] Accessing required primitives.")
		async with self._connection_lock: details = self.sse_connections.get(server_id)
		if details and details.get("status") == "connected" and details.get("ui_layout"): return details.get(
			"required_primitives", set()).copy()
		return None

	async def is_server_ui_ready(self, server_id: str) -> bool:
		logger.info(f"[{server_id}] Checking UI readiness (is_server_ui_ready)...")  # Entry log
		async with self._connection_lock:
			details = self.sse_connections.get(server_id)

		if not details:
			logger.warning(f"[{server_id}] is_server_ui_ready: No details found for server in sse_connections.")
			return False

		# Explicitly get values for logging
		current_status = details.get("status")
		ui_layout_obj = details.get("ui_layout")  # Get the actual object

		status_ok = current_status == "CONNECTED"
		layout_ok = ui_layout_obj is not None  # Check if the object itself is None

		logger.info(
			f"[{server_id}] is_server_ui_ready check: Status='{current_status}' (Target='CONNECTED', Match: {status_ok}), UI Layout Object Present: {layout_ok} (Type: {type(ui_layout_obj).__name__ if ui_layout_obj is not None else 'NoneType'})")

		return bool(status_ok and layout_ok)

	async def cleanup_all_connections(self):
		shutdown_start_time = time.monotonic()
		logger.info("Initiating shutdown for all MCP connections...")
		server_ids_to_clean = []
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
				await resource.aclose()
				logger.debug(f"[{server_id}] Closed resource during {context}.")
			except Exception as e:
				logger.error(f"[{server_id}] Error closing resource during {context}: {e}", exc_info=True)

	def _check_mcp_result_for_error(self, result: Any, operation_name: str):
		if not result:
			return
		error_content = None
		is_error = False
		ErrorData = getattr(mcp_types, 'ErrorData', None)
		TextContent = getattr(mcp_types, 'TextContent', None)
		if hasattr(result, 'isError') and result.isError:
			is_error = True
			error_content = getattr(result, 'content', 'Unknown Error')
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
					logger.error(f"Failed to construct McpError: {construct_err}")
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
		processed_tools: Dict[str, Any] = {}
		if not mcp_types:
			logger.warning("mcp_types missing.")
			return processed_tools
		if not tools_list:
			logger.debug("Tool list empty.")
			return processed_tools
		logger.debug(f"Processing {len(tools_list)} tools...")
		tool_class_to_check = None
		first_item = tools_list[0]
		if hasattr(mcp_types, 'Tool') and isinstance(first_item, mcp_types.Tool):
			tool_class_to_check = mcp_types.Tool
		elif hasattr(mcp_types, 'ToolInfo') and isinstance(first_item, mcp_types.ToolInfo):
			tool_class_to_check = mcp_types.ToolInfo
		else:
			logger.warning(f"Unexpected tool type: {type(first_item)}.")
			return processed_tools
		for tool_info in tools_list:
			if not isinstance(tool_info, tool_class_to_check):
				logger.warning(f"Skipping type: {type(tool_info)}")
				continue
			tool_name = getattr(tool_info, 'name', None)
			if not tool_name:
				logger.warning(f"Skipping tool missing 'name': {tool_info!r}")
				continue
			input_schema = getattr(tool_info, 'inputSchema', getattr(tool_info, 'input_schema', None))
			output_schema = getattr(tool_info, 'outputSchema', getattr(tool_info, 'output_schema', None))
			processed_tools[tool_name] = {"name": tool_name, "description": getattr(tool_info, 'description', ''),
										  "input_schema": input_schema, "output_schema": output_schema}
		logger.debug(f"Processed tools: Found {len(processed_tools)}.")
		return processed_tools

	async def _get_server_ui_layout(self, session: ClientSession, server_id_for_log: str) -> Optional[dict]:
		tool_name = "get_ui_layout"
		logger.info(f"[{server_id_for_log}] Retrieving UI layout via tool: '{tool_name}'")
		try:
			tool_result: CallToolResult = await session.call_tool(name=tool_name, arguments=None)
			self._check_mcp_result_for_error(tool_result, tool_name)
			if tool_result and hasattr(tool_result, 'content') and isinstance(tool_result.content, list) and len(
					tool_result.content) == 1:
				content_item = tool_result.content[0]
				ui_layout = None
				if isinstance(content_item, dict):
					ui_layout = content_item
					logger.info(f"[{server_id_for_log}] Retrieved UI layout (dict).")
				elif mcp_types and isinstance(content_item, mcp_types.TextContent):
					try:
						ui_layout = json.loads(content_item.text)
						logger.info(
							f"[{server_id_for_log}] Parsed UI layout (TextContent).")
					except json.JSONDecodeError as json_err:
						logger.error(
							f"[{server_id_for_log}] Failed to parse JSON from '{tool_name}' TextContent: {json_err}.")
						return None
				else:
					logger.error(
						f"[{server_id_for_log}] Unexpected content type ({type(content_item)}) from '{tool_name}'.")
					return None
				if not isinstance(ui_layout, dict) or 'id' not in ui_layout:
					logger.error(f"[{server_id_for_log}] UI layout invalid.")
					return None
				return ui_layout
			else:
				logger.error(
					f"[{server_id_for_log}] Unexpected content from '{tool_name}': {getattr(tool_result, 'content', 'N/A')!r}")
				return None
		except McpError as e:
			err_code = getattr(getattr(e, 'error', None), 'code', None)
			if err_code == METHOD_NOT_FOUND:
				logger.warning(f"[{server_id_for_log}] UI layout tool '{tool_name}' not found.")
			else:
				logger.error(f"[{server_id_for_log}] MCP error calling '{tool_name}': {e.error!r}", exc_info=False)
			return None
		except Exception as e:
			logger.error(f"[{server_id_for_log}] Unexpected error calling '{tool_name}': {e}", exc_info=True)
			return None

	def _extract_required_primitives(self, layout: Optional[Dict[str, Any]]) -> Set[str]:
		primitives = set()
		if not layout or not isinstance(layout, dict):
			return primitives
		primitive_type = layout.get('type')
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

	async def _health_check_loop(self):
		"""Periodically polls the /health endpoint of all active MCP servers."""
		logger.info("MCPConnectionManager: Health check loop started.")
		await asyncio.sleep(10)  # Initial delay before the first check

		while not self._stop_reconnect_event.is_set():
			try:
				async with httpx.AsyncClient(timeout=10.0) as client:
					check_tasks = []

					# Lock briefly to get a snapshot of servers to check
					async with self._connection_lock:
						servers_to_check = [
							(server_id, details)
							for server_id, details in self.sse_connections.items()
							if details.get("config_for_connection", {}).get("is_active")
						]

					for server_id, details in servers_to_check:
						task = asyncio.create_task(
							self._check_single_server_health(client, server_id, details)
						)
						check_tasks.append(task)

					if check_tasks:
						await asyncio.gather(*check_tasks)

				# Wait for 15 seconds before the next health check cycle
				await asyncio.wait_for(self._stop_reconnect_event.wait(), timeout=15.0)
			except asyncio.TimeoutError:
				pass  # Expected behavior for the wait timeout
			except Exception as e:
				logger.error(f"Health check loop encountered an unexpected error: {e}", exc_info=True)
				await asyncio.sleep(15)  # Wait a bit before retrying after a major loop error

		logger.info("MCPConnectionManager: Health check loop stopped.")

	async def _check_single_server_health(self, client: httpx.AsyncClient, server_id: str, details: Dict[str, Any]):
		"""Checks a single server's health and triggers a reconnect if a restart is detected."""
		config = details.get("config_for_connection", {})
		base_url = config.get("url", "")
		server_name = config.get("name", server_id)

		if not base_url:
			return  # Cannot check a server without a URL

		# Construct health URL from the base SSE url (e.g., http://host/sse -> http://host/health)
		health_url = base_url.rstrip('/').replace("/sse", "") + "/health"
		last_known_startup_id = details.get("last_known_startup_id")

		try:
			response = await client.get(health_url)

			if response.status_code == 200:
				data = response.json()
				current_startup_id = data.get("startup_id")

				if current_startup_id and current_startup_id != last_known_startup_id:
					logger.warning(
						f"[{server_id}] RESTART DETECTED for '{server_name}'. Health check startup_id changed.")
					logger.info(f"[{server_id}]   Old startup_id: {last_known_startup_id}")
					logger.info(f"[{server_id}]   New startup_id: {current_startup_id}")

					async with self._connection_lock:
						current_details = self.sse_connections.get(server_id)
						if not current_details: return

						current_details["last_known_startup_id"] = current_startup_id

						# Only force a reconnect if it's currently considered connected.
						# This prevents conflicts with the standard reconnection loop.
						if current_details.get("status") == "CONNECTED":
							logger.info(
								f"[{server_id}] Forcing reconnection for '{server_name}' due to detected restart.")
							await self._do_cleanup_for_server(server_id, current_details,
															  "health check restart detected")

							current_details["status"] = "DISCONNECTED"
							current_details["next_retry_time"] = time.monotonic()
							current_details["retry_count"] = 0
							current_details["current_retry_delay_seconds"] = INITIAL_RETRY_DELAY_SECONDS

				elif not last_known_startup_id and current_startup_id:
					# This is the first successful health check. Just store the ID.
					async with self._connection_lock:
						self.sse_connections[server_id]["last_known_startup_id"] = current_startup_id
					logger.info(f"[{server_id}] Storing initial startup_id for '{server_name}': {current_startup_id}")

			else:
				logger.debug(
					f"[{server_id}] Health check failed for '{server_name}' (likely down or endpoint missing). Status: {response.status_code}")

		except httpx.RequestError as e:
			logger.debug(f"[{server_id}] Health check request failed for '{server_name}' (server likely down): {e}")
		except Exception as e:
			logger.error(f"[{server_id}] Unexpected error during health check for '{server_name}': {e}", exc_info=True)


async def get_mcp_connection_manager(request: FastAPIRequest) -> MCPConnectionManager:
	if not hasattr(request.app.state, 'mcp_connection_manager') or request.app.state.mcp_connection_manager is None:
		logger.critical("CRITICAL: MCPConnectionManager not initialized in app.state!")
		raise HTTPException(status_code=500, detail="MCPConnectionManager not initialized in app.state")
	return request.app.state.mcp_connection_manager


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
