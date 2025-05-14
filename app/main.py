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
from starlette import status
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
from app.services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager, \
	get_mcp_connection_manager_ws
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
   allow_origins=["*"], # <--- MODIFIED: Allow all origins for development
   allow_credentials=True,
   allow_methods=["*"],
   allow_headers=["*"],
)
# app.add_middleware(
# 	CORSMiddleware,
# 	allow_origins=settings.CORS_ORIGINS, allow_credentials=True,
# 	allow_methods=["*"], allow_headers=["*"],
# )

# --- API Routers ---
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(projects.router, prefix="/api/projects", tags=["Projects/Servers"])
app.include_router(mcp_server_management.router, prefix="/api/mcp-servers", tags=["MCP Server Management"])  # New router
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
		stream_path_param: str = Path(..., description="Stream identifier, e.g., 'mcp:<server_id>'"),
		connection_manager: ConnectionManager = Depends(get_connection_manager),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager_ws)
):
	"""Handles WebSocket connections for streaming UI updates and MCP interaction."""
	ws_client_info = WebSocketLogger.get_client_info(websocket)
	ws_stream_id = stream_path_param
	logger.info(f">>> [WS/{ws_stream_id}] Accepted connection from {ws_client_info} <<<")

	views_service: Optional[ProjectViewsService] = None
	try:
		# Accessing ProjectViewsService from app.state (initialized in main.py lifespan)
		views_service = websocket.app.state.project_views_service
		if not views_service:
			logger.error(f"[WS/{ws_stream_id}] ProjectViewsService not available. Cannot generate UI.")
			await websocket.close(code=status.WS_1011_INTERNAL_ERROR,
								  reason="Internal server error: UI Service unavailable")
			return
	except AttributeError:
		logger.error(f"[WS/{ws_stream_id}] 'project_views_service' not found on app state.")
		await websocket.close(code=status.WS_1011_INTERNAL_ERROR,
							  reason="Internal server error: UI Service not configured")
		return

	mcp_server_db_id: Optional[str] = None
	session_established_with_mcp = False
	user_id = "<unauthenticated>"
	is_authenticated = False  # Default, will be set by authenticate_websocket
	disconnect_reason = "Handler normal exit"  # Default reason

	try:
		logger.debug(f"[WS/{ws_stream_id}] WebSocket authentication SKIPPED for development.")
		# is_authenticated, user_data, auth_error_message = await authenticate_websocket(websocket) # <--- COMMENTED OUT/MODIFIED
		# For development, we'll simulate successful authentication:
		user_data = {"id": user_id, "username": "dev_user"}  # Mock user_data
		auth_error_message = None

		# if not is_authenticated: # This block will now be skipped
		#    logger.warning(f"[WS/{ws_stream_id}] WebSocket Auth FAILED: {auth_error_message}")
		#    disconnect_reason = f"Authentication Failed: {auth_error_message}"
		#    await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
		#    return
		# user_id = user_data.get("id", "<error_id>") if isinstance(user_data, dict) else "<invalid_data>" # <--- MODIFIED ABOVE
		logger.info(f"[WS/{ws_stream_id}] WebSocket Auth SKIPPED. Proceeding as user '{user_id}'.")

		if not stream_path_param.startswith("mcp:"):
			logger.error(f"[WS/{ws_stream_id}] Invalid stream path format. Expected 'mcp:<server_db_id>'.")
			disconnect_reason = "Invalid target stream format"
			await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
			return

		mcp_server_db_id = stream_path_param.split(":", 1)[1]
		if not mcp_server_db_id:  # Add more validation if IDs have specific format (e.g., numeric)
			logger.error(f"[WS/{ws_stream_id}] Missing or invalid MCP server database ID in path.")
			disconnect_reason = "Missing or invalid target server ID"
			await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
			return
		logger.info(f"[WS/{ws_stream_id}] Target MCP Server Database ID: '{mcp_server_db_id}'")

		await connection_manager.connect(websocket, ws_stream_id)
		WebSocketLogger.log_connection(websocket, ws_stream_id, user_id)
		logger.debug(f"[WS/{ws_stream_id}] UI WebSocket registered with its ConnectionManager.")

		handshake_timeout = 10.0
		logger.info(f"[WS/{ws_stream_id}] Waiting for 'register_capabilities' (Timeout: {handshake_timeout}s)...")
		try:
			first_message_text = await asyncio.wait_for(websocket.receive_text(), timeout=handshake_timeout)
			WebSocketLogger.log_text_received(websocket, ws_stream_id, first_message_text)
			handshake_data = json.loads(first_message_text)
			if handshake_data.get("type") == "register_capabilities":
				supported_primitives = handshake_data.get("payload", {}).get("supported_primitives", [])
				if isinstance(supported_primitives, list):
					connection_manager.store_supported_primitives(websocket, supported_primitives)
					logger.info(f"[WS/{ws_stream_id}] Handshake OK. Client capabilities: {supported_primitives}")
				else:
					disconnect_reason = "Invalid capabilities format (not a list)"
					raise WebSocketDisconnect(code=status.WS_1003_UNSUPPORTED_DATA, reason=disconnect_reason)
			else:
				disconnect_reason = "Protocol error: Expected 'register_capabilities' message"
				raise WebSocketDisconnect(code=status.WS_1002_PROTOCOL_ERROR, reason=disconnect_reason)
		except asyncio.TimeoutError:
			disconnect_reason = "Handshake timeout"
			logger.warning(f"[WS/{ws_stream_id}] {disconnect_reason}")
			raise WebSocketDisconnect(code=status.WS_1008_POLICY_VIOLATION,
									  reason=disconnect_reason)  # Or another appropriate code
		except json.JSONDecodeError:
			disconnect_reason = "Invalid handshake JSON"
			logger.warning(f"[WS/{ws_stream_id}] {disconnect_reason}")
			raise WebSocketDisconnect(code=status.WS_1003_UNSUPPORTED_DATA, reason=disconnect_reason)
		except WebSocketDisconnect as wsd:  # Re-raise to be caught by outer handler
			logger.warning(f"[WS/{ws_stream_id}] Handshake failed: {wsd.reason}")
			disconnect_reason = wsd.reason
			raise
		except Exception as handshake_err:  # Catch any other unexpected errors
			disconnect_reason = "Unexpected handshake error"
			logger.error(f"[WS/{ws_stream_id}] {disconnect_reason}: {handshake_err}", exc_info=True)
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR, reason=disconnect_reason)

		logger.info(f"[WS/{ws_stream_id}] Attempting to get MCP session for server DB ID '{mcp_server_db_id}'...")
		mcp_client_session, mcp_session_error = await mcp_conn_manager.get_or_create_session(mcp_server_db_id)

		if mcp_session_error or not mcp_client_session:
			error_detail = f"Failed to get MCP session for server '{mcp_server_db_id}': {mcp_session_error or 'Session object is None.'}"
			logger.error(f"[WS/{ws_stream_id}] {error_detail}")
			server_name_for_error = mcp_server_db_id  # Default
			try:
				server_details_for_error = await mcp_conn_manager.get_connection_details(mcp_server_db_id)
				if server_details_for_error:
					server_name_for_error = server_details_for_error.get("name", mcp_server_db_id)
				await websocket.send_text(json.dumps({"type": "error", "payload": {
					"message": f"Target service '{server_name_for_error}' (ID: {mcp_server_db_id}) not available: {mcp_session_error}"
				}}))
			except Exception as send_err:
				logger.error(f"[WS/{ws_stream_id}] Error sending MCP session acquisition error to client: {send_err}")
			disconnect_reason = f"MCP Session Error: {mcp_session_error}"
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR, reason=disconnect_reason)

		session_established_with_mcp = True
		logger.info(f"[WS/{ws_stream_id}] Successfully obtained MCP session for server DB ID '{mcp_server_db_id}'.")

		logger.info(f"[WS/{ws_stream_id}] Generating initial UI state and sending tool schemas...")
		try:
			root_element: Optional[UIElement] = await views_service.get_project_ui_hierarchy(
				websocket=websocket, stream_id=ws_stream_id, mcp_session=mcp_client_session
			)
			if root_element:
				initial_state_message = InitialUIStateMessage(payload=InitialUIStatePayload(rootElement=root_element))
				initial_state_json = initial_state_message.model_dump_json(exclude_none=True)
				await websocket.send_text(initial_state_json)
				WebSocketLogger.log_text_sent(ws_stream_id, initial_state_json)
				logger.info(f"[WS/{ws_stream_id}] Sent initial UI state to client.")

				logger.info(
					f"[{ws_stream_id}] Attempting to send tool schemas for server DB ID '{mcp_server_db_id}'...")
				discovered_tools = await mcp_conn_manager.get_discovered_tools(mcp_server_db_id)  # This is synchronous
				if discovered_tools:
					tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
					for tool_name, tool_data in discovered_tools.items():
						try:
							# Assuming tool_data structure matches ToolSchemaInfo or has necessary fields
							tool_schemas_for_payload[tool_name] = ToolSchemaInfo(
								name=tool_data.get('name', tool_name),
								description=tool_data.get('description'),
								input_schema=tool_data.get('input_schema'),
								# Pydantic alias 'inputSchema' handled by model
								output_schema=tool_data.get('output_schema')  # Pydantic alias 'outputSchema'
							)
						except Exception as e_schema:
							logger.error(f"Error processing tool schema for {tool_name}: {e_schema}")
					if tool_schemas_for_payload:
						schemas_payload = ToolSchemasPayload(server_id=mcp_server_db_id, tools=tool_schemas_for_payload)
						schemas_message = ToolSchemasMessage(payload=schemas_payload)
						schemas_json = schemas_message.model_dump_json(exclude_none=True, by_alias=True)
						await websocket.send_text(schemas_json)
						WebSocketLogger.log_text_sent(ws_stream_id, schemas_json)
						logger.info(
							f"[{ws_stream_id}] Successfully sent tool schemas for {len(tool_schemas_for_payload)} tools.")
					else:
						logger.warning(
							f"[{ws_stream_id}] No valid tool schemas processed to send for server '{mcp_server_db_id}'.")
				else:
					logger.warning(
						f"[{ws_stream_id}] No discovered tools for '{mcp_server_db_id}'. Sending empty tool list.")
					schemas_payload = ToolSchemasPayload(server_id=mcp_server_db_id, tools={})  # Send empty
					schemas_message = ToolSchemasMessage(payload=schemas_payload)
					await websocket.send_text(schemas_message.model_dump_json(exclude_none=True, by_alias=True))
			else:
				logger.warning(
					f"[{ws_stream_id}] No UI hierarchy generated for '{mcp_server_db_id}'. Informing client.")
				await websocket.send_text(
					json.dumps({"type": "status", "payload": {"message": "UI could not be generated for the target."}}))

		except Exception as ui_gen_err:
			logger.error(f"[WS/{ws_stream_id}] Error during UI generation/schema sending: {ui_gen_err}", exc_info=True)
			disconnect_reason = "UI Generation/Schema Error"
			# Consider sending an error to client before disconnecting
			try:
				await websocket.send_text(json.dumps({"type": "error", "payload": {"message": disconnect_reason}}))
			except Exception:
				pass  # Ignore if send fails
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR, reason=disconnect_reason)

		logger.info(f"[WS/{ws_stream_id}] Entering main message loop for user '{user_id}'...")
		while True:
			message_text = await websocket.receive_text()
			WebSocketLogger.log_text_received(websocket, ws_stream_id, message_text)
			try:
				parsed_message_data = json.loads(message_text)
				message_type = parsed_message_data.get("type")

				if message_type == "ui_action":
					action_id = "<parsing_failed>";
					arguments_to_pass = {}
					try:
						payload = parsed_message_data.get("payload", {})
						action_id = payload.get("actionId")
						arguments_to_pass = payload.get("arguments", {})
						if not action_id:
							logger.warning(f"[WS/{ws_stream_id}] 'ui_action' received with no actionId.")
							continue  # Or send an error response

						logger.info(
							f"[WS/{ws_stream_id}] ui_action '{action_id}'. Calling execute_tool for server DB ID '{mcp_server_db_id}'.")
						mcp_payload_content, mcp_error_obj = await mcp_conn_manager.execute_tool(
							server_id=mcp_server_db_id, tool_name=action_id,
							params=arguments_to_pass, ws_stream_id=ws_stream_id,
						)
						update_binding = f"mcp_stream:{mcp_server_db_id}:{action_id}_result"  # Example binding
						if mcp_error_obj:
							logger.error(f"[WS/{ws_stream_id}] MCP tool error '{action_id}': {mcp_error_obj}")
							error_text_for_ui = "Error executing action."
							if isinstance(mcp_error_obj, str):
								error_text_for_ui = mcp_error_obj
							elif hasattr(mcp_error_obj, 'message'):
								error_text_for_ui = mcp_error_obj.message
							elif isinstance(mcp_payload_content, dict) and 'message' in mcp_payload_content:
								error_text_for_ui = mcp_payload_content['message']
							error_ui_content = {"role": "error", "text": error_text_for_ui}
							error_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																		  content=error_ui_content, updateType="append")
							error_msg_to_send = PrimitiveContentUpdateMessage(payload=error_payload)
							await websocket.send_text(error_msg_to_send.model_dump_json(exclude_none=True))
						else:
							result_role_from_tool = "assistant"  # Default role

							if mcp_payload_content and isinstance(mcp_payload_content, list) and len(
									mcp_payload_content) > 0:
								first_item = mcp_payload_content[0]
								# Check if the first item looks like a TextContent object
								# (has 'text' and 'type' attributes, and type is 'text')
								if hasattr(first_item, 'text') and hasattr(first_item, 'type') and getattr(first_item, 'type') == 'text':
									actual_text_from_tool = getattr(first_item, 'text', "Error: Missing text in response.")
								# You might also want to get the role if the tool could return different roles,
								# but chatbot_query implies an assistant response.
								else:
									logger.warning(
										f"[WS/{ws_stream_id}] Tool '{action_id}' returned list, but item is not TextContent: {first_item}")
									actual_text_from_tool = f"Tool '{action_id}' returned unexpected item structure."
							elif mcp_payload_content is not None:  # It's not a list or not the expected list
								logger.warning(
									f"[WS/{ws_stream_id}] Tool '{action_id}' returned unexpected response format: {type(mcp_payload_content)} - {str(mcp_payload_content)[:200]}")
								actual_text_from_tool = f"Tool '{action_id}' returned unparsable data."
							else:  # mcp_payload_content is None
								logger.warning(f"[WS/{ws_stream_id}] Tool '{action_id}' returned no content (None).")
								actual_text_from_tool = f"Tool '{action_id}' returned no response."

							result_ui_content = {"role": result_role_from_tool, "text": actual_text_from_tool}
							result_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																		   content=result_ui_content,
																		   updateType="append")
							result_msg_to_send = PrimitiveContentUpdateMessage(payload=result_payload)
							await websocket.send_text(result_msg_to_send.model_dump_json(exclude_none=True))
					except ValidationError as e_val:  # Pydantic validation for incoming ui_action
						logger.warning(f"[WS/{ws_stream_id}] Invalid ui_action payload for '{action_id}': {e_val}")
					# Optionally send error back to client
					except Exception as action_err:
						logger.error(f"[WS/{ws_stream_id}] Error processing ui_action '{action_id}': {action_err}",
									 exc_info=True)
					# Optionally send error back to client

				elif message_type == "notify_roots_changed":
					# Implement your logic based on original example, ensuring parsing and error handling
					# e.g. from app.models.schemas import RootsChangedMessage
					# roots_msg = RootsChangedMessage(**parsed_message_data)
					# if roots_msg.payload.server_id == mcp_server_db_id:
					#    await mcp_conn_manager.notify_roots_changed(mcp_server_db_id, roots_msg.payload.roots)
					logger.info(f"[WS/{ws_stream_id}] Received 'notify_roots_changed'. (Implement full logic)")
					pass

				elif message_type == "notify_cancelled":
					# Implement your logic
					# e.g. from app.models.schemas import CancelRequestMessage
					# cancel_msg = CancelRequestMessage(**parsed_message_data)
					# if cancel_msg.payload.server_id == mcp_server_db_id:
					#    await mcp_conn_manager.notify_cancelled(mcp_server_db_id, cancel_msg.payload.requestId)
					logger.info(f"[WS/{ws_stream_id}] Received 'notify_cancelled'. (Implement full logic)")
					pass

				elif message_type == 'ping':
					await websocket.send_text(json.dumps({"type": "pong"}))
					logger.debug(f"[WS/{ws_stream_id}] Responded to ping with pong.")
				else:
					logger.warning(f"[WS/{ws_stream_id}] Received unknown message type: '{message_type}'")

			except json.JSONDecodeError:
				logger.warning(f"[WS/{ws_stream_id}] Received invalid JSON from client: {message_text[:200]}")
			# Optionally send an error message back to the client
			except Exception as loop_err:  # Catch unexpected errors in the loop
				logger.error(f"[WS/{ws_stream_id}] Error in WebSocket message loop: {loop_err}", exc_info=True)
			# Decide if this error should break the loop and disconnect the client

	except WebSocketDisconnect as e:
		disconnect_reason = e.reason or "Client initiated disconnect"
		logger.info(f"[WS/{ws_stream_id}] WebSocket disconnected (Code: {e.code}, Reason: '{disconnect_reason}')")
	except ConnectionRefusedError as cr_err:
		disconnect_reason = f"Connection Refused by target MCP server ({mcp_server_db_id})"
		logger.error(f"[WS/{ws_stream_id}] {disconnect_reason}: {cr_err}", exc_info=False)
		if websocket.client_state == WebSocketState.CONNECTED:
			try:
				await websocket.send_text(json.dumps({"type": "error", "payload": {"message": disconnect_reason}}))
			except Exception:
				pass
	except Exception as e:  # Catch-all for unhandled exceptions in the main try block
		disconnect_reason = "Internal Server Error in WS Handler"
		logger.error(f"[WS/{ws_stream_id}] Unhandled error in WebSocket handler: {e}", exc_info=True)
	finally:
		logger.info(
			f"[WS/{ws_stream_id}] Cleaning up WebSocket for user '{user_id}' (Reason: '{disconnect_reason}')...")
		connection_manager.disconnect(websocket, ws_stream_id)  # Disconnect from UI manager
		WebSocketLogger.log_disconnection(websocket, ws_stream_id, disconnect_reason)

		if session_established_with_mcp and mcp_server_db_id and mcp_conn_manager:
			logger.info(f"[WS/{ws_stream_id}] Releasing MCP session reference for server DB ID '{mcp_server_db_id}'...")
			try:
				await mcp_conn_manager.release_session(mcp_server_db_id)
			except Exception as release_err:
				logger.error(
					f"[WS/{ws_stream_id}] Error releasing MCP session ref for '{mcp_server_db_id}': {release_err}",
					exc_info=True)
		elif mcp_server_db_id:
			logger.debug(
				f"[WS/{ws_stream_id}] MCP session not established or manager unavailable for '{mcp_server_db_id}', no release needed by this handler.")

		# Ensure WebSocket is closed if not already
		if websocket.client_state == WebSocketState.CONNECTED:
			try:
				await websocket.close(code=status.WS_1000_NORMAL_CLOSURE, reason=disconnect_reason)
			except Exception:
				logger.debug(f"[WS/{ws_stream_id}] Error sending final close, ws might be already closed.")
				pass
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
