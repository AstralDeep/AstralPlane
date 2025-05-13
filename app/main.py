import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict

from fastapi import (
	FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, Path, Request
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.websockets import WebSocketState

from app.api import mcp_server_management
from app.api import projects, auth, websockets
# --- Core App Dependencies ---
from app.config import settings
from app.lifecycle.lifespan import lifespan
# --- Model Imports ---
from app.models.schemas import (
	UIElement, InitialUIStateMessage, InitialUIStatePayload,
	PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
	ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage
)
from app.services.auth_service import authenticate_websocket
from app.services.connection_manager import ConnectionManager, get_connection_manager
from app.services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager
from app.services.project_service import ProjectViewsService
from app.utils.logging_config import configure_logging
from app.utils.websocket_logger import WebSocketLogger

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
configure_logging(log_level=log_level, log_to_file=settings.DEBUG, log_dir="logs")
logger = logging.getLogger(__name__)
logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")

# --- Initialize FastAPI App ---
app = FastAPI(
	title=settings.APP_NAME,
	description="Backend API using proactive MCP connections and server-driven UI.",
	version="0.9.1",
	lifespan=lifespan
)

# --- CORS Middleware ---
app.add_middleware(
	CORSMiddleware,
	allow_origins=settings.CORS_ORIGINS, allow_credentials=True,
	allow_methods=["*"], allow_headers=["*"],
)

# --- API Routers ---
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(projects.router, prefix="/api/projects", tags=["Projects/Servers"])
app.include_router(mcp_server_management.router, prefix="/api/mcp-servers", tags=["MCP Server Management"]) # New router
if hasattr(websockets, 'router'):
	app.include_router(websockets.router, prefix="/api/ws", tags=["WebSocket Utils"])


# --- Exception Handlers ---
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
	logger.warning(
		f"HTTP Exception: Status={exc.status_code}, Detail='{exc.detail}' for URL='{request.url}' from {request.client.host if request.client else 'unknown'}")
	return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail},
						headers=getattr(exc, "headers", None))


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
	logger.error(
		f"Unhandled exception during request to {request.url} from {request.client.host if request.client else 'unknown'}: {exc}",
		exc_info=True)
	return JSONResponse(status_code=500, content={"detail": "An internal server error occurred."})


# --- Health Check Endpoint ---
@app.get("/api/health", tags=["Health"])
async def health_check(mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)):
	# Overall application status
	app_status = "ok"  # Can be changed if critical dependencies are missing

	# MCPConnectionManager status
	mcp_manager_overall_status = "unavailable"
	if mcp_conn_manager:
		mcp_manager_overall_status = "initialized"  # Indicates the manager itself is up

	# ProjectViewsService status
	pvs_status = "unavailable"
	if hasattr(app.state, 'project_views_service') and app.state.project_views_service:
		pvs_status = "initialized"
	else:
		# If PVS is critical, you might change app_status
		# app_status = "degraded"
		pass

	# Individual MCP Server connection statuses
	mcp_server_statuses = {}
	if mcp_conn_manager and hasattr(mcp_conn_manager, 'sse_connections'):
		# Iterate over the keys of sse_connections, which are the server_id strings
		server_ids = list(mcp_conn_manager.sse_connections.keys())  # Get a static list of keys
		if not server_ids:
			logger.info("Health Check: No MCP servers configured in the manager.")

		for server_id_str in server_ids:
			details = await mcp_conn_manager.get_connection_details(server_id_str)
			if details:
				mcp_server_statuses[server_id_str] = {
					"name": details.get("name", server_id_str),  # Added server name
					"status": details.get("status"),
					"configured_active": details.get("configured_active"),  # From new details
					"url": details.get("url"),  # Added URL
					"error": details.get("error_message"),  # Renamed from "error_message" for consistency
					"ui_ready": details.get("ui_layout_retrieved", False),
					"tools_count": len(details.get("tools_available", [])),  # Example of more detail
					# You can add more fields from 'details' if useful for health monitoring
					# "ref_count": details.get("ref_count"),
					# "last_successful_connect": details.get("last_successful_connect")
				}
			else:
				# This case should be rare if server_id_str comes from sse_connections.keys()
				# unless a server was removed concurrently without proper locking (unlikely for health check read)
				mcp_server_statuses[server_id_str] = {
					"name": server_id_str,
					"status": "unknown",
					"error": "State missing or server recently removed",
					"ui_ready": False
				}
	else:
		if not mcp_conn_manager:
			logger.warning("Health Check: MCPConnectionManager not available.")
		else:
			logger.warning("Health Check: MCPConnectionManager available but sse_connections attribute is missing.")
	# If MCP connections are critical, you might change app_status
	# app_status = "degraded"

	return {
		"status": app_status,  # Overall app status
		"timestamp": datetime.now().isoformat(),
		"service_name": settings.APP_NAME,
		"service_version": settings.APP_VERSION,
		"dependencies": {
			"mcp_connection_manager": mcp_manager_overall_status,
			"project_views_service": pvs_status,
		},
		"mcp_server_connections": mcp_server_statuses
	}


# --- WebSocket Endpoint Definition (MODIFIED in ui_action result handling) ---
@app.websocket("/api/ws/stream/{stream_path_param:path}")
async def websocket_endpoint(
		websocket: WebSocket,
		stream_path_param: str = Path(..., description="Stream identifier, e.g., 'mcp:<database_server_id>'"),
		# For UI client connections, assuming get_ui_connection_manager_dependency is correctly set up
		connection_manager: ConnectionManager = Depends(get_connection_manager),
		# This now uses the get_mcp_connection_manager defined in main.py that pulls from app.state
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""Handles WebSocket connections for streaming UI updates and MCP interaction."""
	ws_client_info = WebSocketLogger.get_client_info(websocket)
	# The ws_stream_id IS stream_path_param, e.g., "mcp:1" where "1" is the DB ID.
	ws_stream_id = stream_path_param
	logger.info(f">>> [WS/{ws_stream_id}] Accepted connection from {ws_client_info} <<<")

	views_service: Optional[ProjectViewsService] = None
	try:
		views_service = websocket.app.state.project_views_service
		if not views_service:
			logger.error(f"[WS/{ws_stream_id}] ProjectViewsService not available. Cannot generate UI.")
			await websocket.close(code=1011, reason="Internal server error: UI Service unavailable")
			return
	except AttributeError:
		logger.error(f"[WS/{ws_stream_id}] 'project_views_service' not found on app state.")
		await websocket.close(code=1011, reason="Internal server error: UI Service not configured")
		return

	mcp_server_db_id: Optional[str] = None  # This will be the string representation of MCPServerConfig.id
	session_established_with_mcp = False  # Tracks if we have a live MCP session
	user_id = "<unauthenticated>"
	is_authenticated = False
	disconnect_reason = "Handler Exit"

	try:
		# --- WebSocket Authentication (remains the same) ---
		logger.debug(f"[WS/{ws_stream_id}] Attempting WebSocket authentication...")
		is_authenticated, user_data, error_message = await authenticate_websocket(websocket)
		if not is_authenticated:
			logger.warning(f"[WS/{ws_stream_id}] WebSocket Auth FAILED: {error_message}")
			disconnect_reason = f"Authentication Failed: {error_message}"
			await websocket.close(code=4003, reason=disconnect_reason)
			return
		user_id = user_data.get("id", "<error_id>") if isinstance(user_data, dict) else "<invalid_data>"
		logger.info(f"[WS/{ws_stream_id}] WebSocket Auth successful for user '{user_id}'.")

		# --- Parse Target MCP Server Database ID ---
		if not stream_path_param.startswith("mcp:"):
			logger.error(f"[WS/{ws_stream_id}] Invalid stream path format. Expected 'mcp:<server_db_id>'.")
			disconnect_reason = "Invalid target stream format"
			await websocket.close(code=1008, reason=disconnect_reason)
			return

		mcp_server_db_id = stream_path_param.split(":", 1)[1]
		if not mcp_server_db_id:  # Or if not mcp_server_db_id.isdigit() for basic validation if ID is integer
			logger.error(f"[WS/{ws_stream_id}] Missing or invalid MCP server database ID in path.")
			disconnect_reason = "Missing or invalid target server ID"
			await websocket.close(code=1008, reason=disconnect_reason)
			return
		logger.info(f"[WS/{ws_stream_id}] Target MCP Server Database ID: '{mcp_server_db_id}'")

		# --- Register WebSocket with UI ConnectionManager (remains the same) ---
		await connection_manager.connect(websocket, ws_stream_id)
		WebSocketLogger.log_connection(websocket, ws_stream_id, user_id)
		logger.debug(f"[WS/{ws_stream_id}] UI WebSocket registered with its ConnectionManager.")

		# --- Handshake: Receive Client Capabilities (remains the same) ---
		# ... (your existing handshake logic: register_capabilities, timeout, etc.) ...
		# This part is about the UI client's capabilities, not directly MCP.
		# (Assuming your handshake logic from original main.py is here)
		handshake_timeout = 10.0
		logger.info(f"[WS/{ws_stream_id}] Waiting for 'register_capabilities' (Timeout: {handshake_timeout}s)...")
		try:
			first_message_text = await asyncio.wait_for(websocket.receive_text(), timeout=handshake_timeout)
			WebSocketLogger.log_text_received(websocket, ws_stream_id, first_message_text)
			handshake_data = json.loads(first_message_text)
			if handshake_data.get("type") == "register_capabilities":
				supported_primitives = handshake_data.get("payload", {}).get("supported_primitives", [])
				if isinstance(supported_primitives, list):
					connection_manager.store_supported_primitives(websocket,
																  supported_primitives)  # Store against UI connection manager
					logger.info(f"[WS/{ws_stream_id}] Handshake OK. Client capabilities: {supported_primitives}")
				else:
					disconnect_reason = "Invalid capabilities format"
					raise WebSocketDisconnect(code=1003, reason=disconnect_reason)
			else:
				disconnect_reason = "Protocol error: Expected capabilities"
				raise WebSocketDisconnect(code=1002, reason=disconnect_reason)
		except asyncio.TimeoutError:
			disconnect_reason = "Handshake timeout"
			raise WebSocketDisconnect(code=1008, reason=disconnect_reason)
		except json.JSONDecodeError:
			disconnect_reason = "Invalid handshake JSON"
			raise WebSocketDisconnect(code=1003, reason=disconnect_reason)
		except WebSocketDisconnect as wsd:
			logger.warning(f"[WS/{ws_stream_id}] Disconnecting during handshake: {wsd.reason}")
			raise wsd
		except Exception as handshake_err:
			logger.error(f"[WS/{ws_stream_id}] Handshake error: {handshake_err}", exc_info=True)
			disconnect_reason = "Handshake error"
			raise WebSocketDisconnect(code=1011, reason=disconnect_reason)

		# --- Get MCP Session from MCPConnectionManager ---
		logger.info(f"[WS/{ws_stream_id}] Attempting to get MCP session for server DB ID '{mcp_server_db_id}'...")
		# mcp_conn_manager is the refactored instance.
		# get_or_create_session now takes the database ID string.
		# It returns Tuple[Optional[ClientSession], Optional[str]]
		mcp_client_session, mcp_session_error = await mcp_conn_manager.get_or_create_session(mcp_server_db_id)

		if mcp_session_error or not mcp_client_session:
			error_detail = f"Failed to get MCP session for server '{mcp_server_db_id}': {mcp_session_error or 'Session object is None.'}"
			logger.error(f"[WS/{ws_stream_id}] {error_detail}")
			try:
				# Try to get server name for a friendlier message
				server_details_for_error = await mcp_conn_manager.get_connection_details(mcp_server_db_id)
				server_name_for_error = server_details_for_error.get("name",
																	 mcp_server_db_id) if server_details_for_error else mcp_server_db_id

				await websocket.send_text(json.dumps({"type": "error", "payload": {
					"message": f"Target service '{server_name_for_error}' (ID: {mcp_server_db_id}) not available: {mcp_session_error}"
				}}))
			except Exception as send_err:
				logger.error(f"[WS/{ws_stream_id}] Error sending MCP session acquisition error to client: {send_err}")
			disconnect_reason = f"MCP Session Error: {mcp_session_error}"
			raise WebSocketDisconnect(code=1011, reason=disconnect_reason)  # Use 1011 for internal server error

		session_established_with_mcp = True
		# mcp_client_session is the actual session object from the MCP SDK, not just a wrapper dict.
		logger.info(f"[WS/{ws_stream_id}] Successfully obtained MCP session for server DB ID '{mcp_server_db_id}'.")

		# --- Generate Initial UI State & Send Tool Schemas (largely same, uses new mcp_server_db_id) ---
		logger.info(f"[WS/{ws_stream_id}] Generating initial UI state and sending tool schemas...")
		try:
			# get_project_ui_hierarchy might need to know how to use mcp_client_session or mcp_server_db_id
			# The stream_id passed is "mcp:<db_id>"
			root_element: Optional[UIElement] = await views_service.get_project_ui_hierarchy(
				websocket=websocket,  # For client capabilities
				stream_id=ws_stream_id,
				mcp_session=mcp_client_session
			)
			if root_element:
				initial_state_message = InitialUIStateMessage(payload=InitialUIStatePayload(rootElement=root_element))
				initial_state_json = initial_state_message.model_dump_json(exclude_none=True)
				await websocket.send_text(initial_state_json)
				WebSocketLogger.log_text_sent(ws_stream_id, initial_state_json)
				logger.info(f"[WS/{ws_stream_id}] Sent initial UI state to client.")

				logger.info(
					f"[{ws_stream_id}] Attempting to send tool schemas for server DB ID '{mcp_server_db_id}'...")
				# get_discovered_tools uses the DB ID string
				discovered_tools = mcp_conn_manager.get_discovered_tools(mcp_server_db_id)
				if discovered_tools:
					# ... (your existing logic to build ToolSchemasMessage from discovered_tools) ...
					# This part should be mostly the same as your original.
					tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
					for tool_name, tool_data in discovered_tools.items():
						# ... (validation and ToolSchemaInfo creation) ...
						tool_schemas_for_payload[tool_name] = ToolSchemaInfo(
							**tool_data)  # Assuming tool_data matches schema
					if tool_schemas_for_payload:
						schemas_payload = ToolSchemasPayload(server_id=mcp_server_db_id, tools=tool_schemas_for_payload)
						schemas_message = ToolSchemasMessage(payload=schemas_payload)
						schemas_json = schemas_message.model_dump_json(exclude_none=True, by_alias=True)
						await websocket.send_text(schemas_json)
						WebSocketLogger.log_text_sent(ws_stream_id, schemas_json)
						logger.info(
							f"[{ws_stream_id}] Successfully sent tool schemas for {len(tool_schemas_for_payload)} tools.")
				# ... (else clauses for no valid schemas)
				else:
					logger.warning(
						f"[{ws_stream_id}] No discovered tools found for server DB ID '{mcp_server_db_id}'. Cannot send schemas.")
			else:
				logger.warning(
					f"[{ws_stream_id}] No UI hierarchy generated for '{mcp_server_db_id}'. Informing client.")
			# ... (send status message to client)
		except Exception as ui_gen_err:
			# ... (handle UI generation/schema sending error, disconnect) ...
			logger.error(f"[WS/{ws_stream_id}] Error during UI generation/schema sending phase: {ui_gen_err}",
						 exc_info=True)
			disconnect_reason = "UI Generation/Schema Error";
			raise WebSocketDisconnect(code=1011, reason=disconnect_reason)

		# --- Main Message Loop (interactions use mcp_server_db_id) ---
		logger.info(f"[WS/{ws_stream_id}] Entering main message loop for user '{user_id}'...")
		while True:
			message_text = await websocket.receive_text()
			WebSocketLogger.log_text_received(websocket, ws_stream_id, message_text)
			# ... (JSON parsing of message_text) ...
			parsed_message_data = json.loads(message_text)  # Add try-except for JSONDecodeError
			message_type = parsed_message_data.get("type")

			if message_type == "ui_action":
				action_id = "<parsing_failed>"
				try:
					payload = parsed_message_data.get("payload", {})
					action_id = payload.get("actionId")  # This is the tool name
					# ... (extract sourceElementId, arguments) ...
					arguments_to_pass = payload.get("arguments", {})

					logger.info(
						f"[WS/{ws_stream_id}] Received ui_action '{action_id}'. Calling MCPConnectionManager.execute_tool for server DB ID '{mcp_server_db_id}'.")

					# execute_tool now takes server_id (db_id string), tool_name, params
					mcp_payload_content, mcp_error_obj = await mcp_conn_manager.execute_tool(
						server_id=mcp_server_db_id,  # Pass the DB ID
						tool_name=action_id,
						params=arguments_to_pass,
						ws_stream_id=ws_stream_id,  # For logging/correlation within manager
					)

					# --- Process mcp_payload_content and mcp_error_obj ---
					# Your "REFINED FIX LOGIC" for handling the result of execute_tool (mcp_payload_content)
					# and mcp_error_obj (which might be an ErrorData instance or a string message)
					# will largely remain the same.
					# Ensure it correctly checks the type of mcp_payload_content (e.g., list of SamplingMessage-like objects)
					# and mcp_error_obj.
					# Example snippet (adapt your full logic here):
					update_binding = f"mcp_stream:{mcp_server_db_id}:{action_id}_result"  # Determine binding
					if mcp_error_obj:
						logger.error(f"[WS/{ws_stream_id}] MCP tool error '{action_id}': {mcp_error_obj}")
						error_text_for_ui = "Error executing action."
						if isinstance(mcp_error_obj, str):
							error_text_for_ui = mcp_error_obj
						elif hasattr(mcp_error_obj, 'message'):
							error_text_for_ui = mcp_error_obj.message  # For ErrorData
						elif isinstance(mcp_payload_content, dict) and 'message' in mcp_payload_content:
							error_text_for_ui = mcp_payload_content['message']  # Fallback if error was in payload

						error_ui_content = {"role": "error", "text": error_text_for_ui}
						# ... (construct PrimitiveContentUpdateMessage and send)
						error_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																	  content=error_ui_content, updateType="append")
						error_msg_to_send = PrimitiveContentUpdateMessage(payload=error_payload)
						await websocket.send_text(error_msg_to_send.model_dump_json(exclude_none=True))
					else:
						# Your logic for processing successful mcp_payload_content
						# (the "REFINED FIX LOGIC" part from your original code)
						# This expects mcp_payload_content to be structured as per the tool's output,
						# e.g., List[SamplingMessage] or similar that your refined logic parses.
						actual_text_from_tool = "Processed: " + str(
							mcp_payload_content)  # Placeholder for your complex extraction
						result_role_from_tool = "assistant"  # Placeholder
						# ... (your logic to extract actual_text_from_tool and result_role_from_tool from mcp_payload_content)
						result_ui_content = {"role": result_role_from_tool, "text": actual_text_from_tool}
						result_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																	   content=result_ui_content, updateType="append")
						result_msg_to_send = PrimitiveContentUpdateMessage(payload=result_payload)
						await websocket.send_text(result_msg_to_send.model_dump_json(exclude_none=True))

				except ValidationError as e:  # Pydantic validation for incoming ui_action
					# ... (handle validation error)
					pass
				except Exception as action_err:
					# ... (handle general action error)
					pass

			elif message_type == "notify_roots_changed":
				# ... (parse RootsChangedMessage) ...
				# payload = roots_msg.payload
				# target_server_id_from_msg = payload.server_id # This ID from client *must* be a DB ID string
				# if target_server_id_from_msg == mcp_server_db_id:
				#    await mcp_conn_manager.notify_roots_changed(mcp_server_db_id, payload.roots)
				# ... (else log warning)
				pass  # Implement full logic as in your original

			elif message_type == "notify_cancelled":
				# ... (parse CancelRequestMessage) ...
				# payload = cancel_msg.payload
				# target_server_id_from_msg = payload.server_id # DB ID string
				# request_id_to_cancel = payload.requestId
				# if target_server_id_from_msg == mcp_server_db_id:
				#    await mcp_conn_manager.notify_cancelled(mcp_server_db_id, request_id_to_cancel)
				# ... (else log warning)
				pass  # Implement full logic as in your original

			elif message_type == 'ping':
				# ... (send pong, remains the same)
				pass
			else:
				logger.warning(f"[WS/{ws_stream_id}] Received unknown message type: '{message_type}'")

	except WebSocketDisconnect as e:
		disconnect_reason = e.reason or "Client disconnected"
		logger.info(f"[WS/{ws_stream_id}] WebSocket disconnected (Code: {e.code}, Reason: '{disconnect_reason}')")
	except ConnectionRefusedError as cr_err:  # Should be caught earlier during session get typically
		disconnect_reason = "Connection Refused (likely MCP)"
		logger.error(f"[WS/{ws_stream_id}] Connection Refused Error: {cr_err}", exc_info=False)
	except Exception as e:
		disconnect_reason = "Internal Server Error in WS Handler"
		logger.error(f"[WS/{ws_stream_id}] Unhandled error in WebSocket handler: {e}", exc_info=True)
		if websocket.client_state == WebSocketState.CONNECTED:
			try:
				await websocket.close(code=1011, reason=disconnect_reason)
			except Exception:
				pass  # Ignore errors during close after another error
	finally:
		logger.info(f"[WS/{ws_stream_id}] Cleaning up WebSocket for user '{user_id}' (Reason: {disconnect_reason})...")
		# Disconnect from UI ConnectionManager
		connection_manager.disconnect(websocket, ws_stream_id)
		WebSocketLogger.log_disconnection(websocket, ws_stream_id, disconnect_reason)

		# Release MCP session reference if it was established
		if session_established_with_mcp and mcp_server_db_id:
			if mcp_conn_manager:  # Check if manager is available (it should be)
				logger.info(
					f"[WS/{ws_stream_id}] Releasing MCP session reference for server DB ID '{mcp_server_db_id}'...")
				try:
					# release_session uses the DB ID string
					await mcp_conn_manager.release_session(mcp_server_db_id)
				except Exception as release_err:
					logger.error(
						f"[WS/{ws_stream_id}] Error releasing MCP session ref for '{mcp_server_db_id}': {release_err}",
						exc_info=True)
			else:  # Should not happen with Depends
				logger.warning(
					f"[WS/{ws_stream_id}] MCPConnectionManager unavailable during cleanup for MCP session release.")
		elif mcp_server_db_id:  # Path was parsed but session wasn't established
			logger.debug(
				f"[WS/{ws_stream_id}] MCP session was not established for '{mcp_server_db_id}', no release needed by this handler.")

		logger.info(f"[WS/{ws_stream_id}] Finished WebSocket cleanup for user '{user_id}'.")


# --- Main execution block ---
if __name__ == "__main__":
	import uvicorn

	if not logger.hasHandlers():
		logging.basicConfig(level=logging.INFO)
		logger = logging.getLogger(__name__)
	logger.info(f"Starting Uvicorn server for {settings.APP_NAME} on {settings.HOST}:{settings.PORT}")
	logger.info(f"Config: Debug={settings.DEBUG}, Reload={settings.DEBUG}, Env={settings.APP_ENV}")
	logger.info(f"Access API docs at http://{settings.HOST}:{settings.PORT}/docs")
	log_level_name_main = os.getenv("LOG_LEVEL", "DEBUG").upper()
	uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=settings.DEBUG,
				log_level=log_level_name_main.lower(), log_config=None)
