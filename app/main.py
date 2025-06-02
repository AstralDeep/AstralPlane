import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict

import httpx
from fastapi import (
	FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, Path, Request, UploadFile, File
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
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
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
	allow_origins=settings.CORS_ORIGINS,
	allow_credentials=True,
	allow_methods=["*"],
	allow_headers=["*"]
)

# --- API Routers ---
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(projects.router, prefix="/api/projects", tags=["Projects/Servers"])
app.include_router(mcp_server_management.router, prefix="/api/mcp-servers", tags=["MCP Server Management"])
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
	app_status = "ok"
	mcp_manager_overall_status = "unavailable"
	if mcp_conn_manager:
		mcp_manager_overall_status = "initialized"

	pvs_status = "unavailable"
	if hasattr(app.state, 'project_views_service') and app.state.project_views_service:
		pvs_status = "initialized"
	else:
		pass

	mcp_server_statuses = {}
	if mcp_conn_manager and hasattr(mcp_conn_manager, 'sse_connections'):
		server_ids = list(mcp_conn_manager.sse_connections.keys())
		if not server_ids:
			logger.info("Health Check: No MCP servers configured in the manager.")

		for server_id_str in server_ids:
			details = await mcp_conn_manager.get_connection_details(server_id_str)
			if details:
				mcp_server_statuses[server_id_str] = {
					"name": details.get("name", server_id_str),
					"status": details.get("status"),
					"configured_active": details.get("configured_active"),
					"url": details.get("url"),
					"error": details.get("error_message"),
					"ui_ready": details.get("ui_layout_retrieved", False),
					"tools_count": len(details.get("tools_available", [])),
				}
			else:
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

	return {
		"status": app_status,
		"timestamp": datetime.now().isoformat(),
		"service_name": settings.APP_NAME,
		"service_version": settings.APP_VERSION,
		"dependencies": {
			"mcp_connection_manager": mcp_manager_overall_status,
			"project_views_service": pvs_status,
		},
		"mcp_server_connections": mcp_server_statuses
	}


# --- Generic File Upload Proxy Endpoint ---
@app.post("/api/upload-file/{server_id}", tags=["File Upload"])
async def proxy_file_upload(
		server_id: str,
		file: UploadFile = File(...),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""
	Receives a file from the frontend and proxies it to the appropriate MCP server's
	designated upload endpoint. This endpoint is generic for all file types.
	"""
	details = await mcp_conn_manager.get_connection_details(server_id)

	# Assumes 'upload_path' (e.g., '/upload-audio') is part of the server's config
	mcp_url = details.get("url")
	upload_path = "/upload-audio"

	if not mcp_url:
		raise HTTPException(
			status_code=404,
			detail=f"MCP server '{server_id}' not found or is not configured for file uploads."
		)

	mcp_upload_url = f"{mcp_url.rstrip('/')}{upload_path}"
	logger.info(f"Proxying file '{file.filename}' to generic endpoint at {mcp_upload_url}")

	async with httpx.AsyncClient() as client:
		try:
			# Use the generic 'file' key for all forwarded uploads
			forwarded_files = {'file': (file.filename, await file.read(), file.content_type)}
			response = await client.post(mcp_upload_url, files=forwarded_files, timeout=60.0)
			response.raise_for_status()
			return JSONResponse(content=response.json(), status_code=response.status_code)
		except httpx.RequestError as exc:
			logger.error(f"Could not connect while proxying to {mcp_upload_url}: {exc}")
			raise HTTPException(status_code=502, detail="Bad Gateway: Cannot connect to the underlying service.")
		except httpx.HTTPStatusError as exc:
			logger.error(f"The target service at {mcp_upload_url} returned an error: {exc.response.status_code}")
			return JSONResponse(content=exc.response.json(), status_code=exc.response.status_code)


# --- WebSocket Endpoint Definition ---
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
		views_service = websocket.app.state.project_views_service
		if not views_service:
			logger.error(f"[WS/{ws_stream_id}] ProjectViewsService not available.")
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
	is_authenticated = False
	disconnect_reason = "Handler normal exit"

	try:
		logger.debug(f"[WS/{ws_stream_id}] WebSocket authentication SKIPPED for development.")
		user_data = {"id": user_id, "username": "dev_user"}
		auth_error_message = None
		logger.info(f"[WS/{ws_stream_id}] WebSocket Auth SKIPPED. Proceeding as user '{user_id}'.")

		if not stream_path_param.startswith("mcp:"):
			logger.error(f"[WS/{ws_stream_id}] Invalid stream path format.")
			disconnect_reason = "Invalid target stream format"
			await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
			return

		mcp_server_db_id = stream_path_param.split(":", 1)[1]
		if not mcp_server_db_id:
			logger.error(f"[WS/{ws_stream_id}] Missing MCP server database ID in path.")
			disconnect_reason = "Missing or invalid target server ID"
			await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
			return
		logger.info(f"[WS/{ws_stream_id}] Target MCP Server Database ID: '{mcp_server_db_id}'")

		await connection_manager.connect(websocket, ws_stream_id)
		WebSocketLogger.log_connection(websocket, ws_stream_id, user_id)

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
			raise WebSocketDisconnect(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
		except json.JSONDecodeError:
			disconnect_reason = "Invalid handshake JSON"
			raise WebSocketDisconnect(code=status.WS_1003_UNSUPPORTED_DATA, reason=disconnect_reason)
		except WebSocketDisconnect as wsd:
			disconnect_reason = wsd.reason
			raise

		logger.info(f"[WS/{ws_stream_id}] Attempting to get MCP session for server DB ID '{mcp_server_db_id}'...")
		mcp_client_session, mcp_session_error = await mcp_conn_manager.get_or_create_session(mcp_server_db_id)

		if mcp_session_error or not mcp_client_session:
			error_detail = f"Failed to get MCP session for server '{mcp_server_db_id}': {mcp_session_error or 'Session object is None.'}"
			logger.error(f"[WS/{ws_stream_id}] {error_detail}")
			server_name_for_error = mcp_server_db_id
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
				await websocket.send_text(initial_state_message.model_dump_json(exclude_none=True))
				logger.info(f"[WS/{ws_stream_id}] Sent initial UI state to client.")

				discovered_tools = await mcp_conn_manager.get_discovered_tools(mcp_server_db_id)
				if discovered_tools:
					tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
					for tool_name, tool_data in discovered_tools.items():
						tool_schemas_for_payload[tool_name] = ToolSchemaInfo(
							name=tool_data.get('name', tool_name),
							description=tool_data.get('description'),
							input_schema=tool_data.get('input_schema'),
							output_schema=tool_data.get('output_schema')
						)
					if tool_schemas_for_payload:
						schemas_payload = ToolSchemasPayload(server_id=mcp_server_db_id, tools=tool_schemas_for_payload)
						schemas_message = ToolSchemasMessage(payload=schemas_payload)
						await websocket.send_text(schemas_message.model_dump_json(exclude_none=True, by_alias=True))
						logger.info(f"[{ws_stream_id}] Successfully sent {len(tool_schemas_for_payload)} tool schemas.")
				else:
					logger.warning(f"[{ws_stream_id}] No discovered tools for '{mcp_server_db_id}'.")
					schemas_payload = ToolSchemasPayload(server_id=mcp_server_db_id, tools={})
					await websocket.send_text(
						ToolSchemasMessage(payload=schemas_payload).model_dump_json(exclude_none=True, by_alias=True))
			else:
				logger.warning(f"[{ws_stream_id}] No UI hierarchy generated for '{mcp_server_db_id}'.")
				await websocket.send_text(
					json.dumps({"type": "status", "payload": {"message": "UI could not be generated."}}))
		except Exception as ui_gen_err:
			logger.error(f"[WS/{ws_stream_id}] Error during UI generation/schema sending: {ui_gen_err}", exc_info=True)
			disconnect_reason = "UI Generation/Schema Error"
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR, reason=disconnect_reason)

		logger.info(f"[WS/{ws_stream_id}] Entering main message loop for user '{user_id}'...")
		while True:
			message_text = await websocket.receive_text()
			WebSocketLogger.log_text_received(websocket, ws_stream_id, message_text)
			try:
				parsed_message_data = json.loads(message_text)
				message_type = parsed_message_data.get("type")

				if message_type == "ui_action":
					action_id = "<parsing_failed>"
					try:
						payload = parsed_message_data.get("payload", {})
						action_id = payload.get("actionId")
						arguments_to_pass = payload.get("arguments", {})
						if not action_id:
							logger.warning(f"[WS/{ws_stream_id}] 'ui_action' received with no actionId.")
							continue

						mcp_payload_content, mcp_error_obj = await mcp_conn_manager.execute_tool(
							server_id=mcp_server_db_id, tool_name=action_id,
							params=arguments_to_pass, ws_stream_id=ws_stream_id,
						)
						update_binding = f"mcp_stream:{mcp_server_db_id}:{action_id}_result"
						if mcp_error_obj:
							logger.error(f"[WS/{ws_stream_id}] MCP tool error '{action_id}': {mcp_error_obj}")
							error_text_for_ui = "Error executing action."  # Default
							if isinstance(mcp_error_obj, str):
								error_text_for_ui = mcp_error_obj
							elif hasattr(mcp_error_obj, 'message'):
								error_text_for_ui = mcp_error_obj.message
							error_ui_content = {"role": "error", "text": error_text_for_ui}
							error_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																		  content=error_ui_content, updateType="append")
							await websocket.send_text(
								PrimitiveContentUpdateMessage(payload=error_payload).model_dump_json(exclude_none=True))
						else:
							# --- MODIFIED: Smart result parsing ---
							result_role_from_tool = "assistant"
							actual_text_from_tool = f"Tool '{action_id}' returned no response."

							if mcp_payload_content and isinstance(mcp_payload_content, list) and len(
									mcp_payload_content) > 0:
								first_item = mcp_payload_content[0]
								if hasattr(first_item, 'text') and hasattr(first_item, 'type') and getattr(first_item,
																										   'type') == 'text':
									tool_response_text = getattr(first_item, 'text', '{}')
									try:
										# Try to parse the text as JSON
										parsed_data = json.loads(tool_response_text)
										# Use 'display_text' if available, otherwise fall back to the raw text
										actual_text_from_tool = parsed_data.get('display_text', tool_response_text)
									except json.JSONDecodeError:
										# If it's not JSON, just use the text as-is
										actual_text_from_tool = tool_response_text
							# --- END OF MODIFICATION ---

							result_ui_content = {"role": result_role_from_tool, "text": actual_text_from_tool}
							result_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																		   content=result_ui_content,
																		   updateType="append")
							await websocket.send_text(
								PrimitiveContentUpdateMessage(payload=result_payload).model_dump_json(
									exclude_none=True))
					except Exception as action_err:
						logger.error(f"[WS/{ws_stream_id}] Error processing ui_action '{action_id}': {action_err}",
									 exc_info=True)

				elif message_type == 'ping':
					await websocket.send_text(json.dumps({"type": "pong"}))
				else:
					logger.warning(f"[WS/{ws_stream_id}] Received unknown message type: '{message_type}'")

			except json.JSONDecodeError:
				logger.warning(f"[WS/{ws_stream_id}] Received invalid JSON from client: {message_text[:200]}")
			except Exception as loop_err:
				logger.error(f"[WS/{ws_stream_id}] Error in WebSocket message loop: {loop_err}", exc_info=True)

	except WebSocketDisconnect as e:
		disconnect_reason = e.reason or "Client initiated disconnect"
		logger.info(f"[WS/{ws_stream_id}] WebSocket disconnected (Code: {e.code}, Reason: '{disconnect_reason}')")
	except Exception as e:
		disconnect_reason = "Internal Server Error in WS Handler"
		logger.error(f"[WS/{ws_stream_id}] Unhandled error in WebSocket handler: {e}", exc_info=True)
	finally:
		logger.info(
			f"[WS/{ws_stream_id}] Cleaning up WebSocket for user '{user_id}' (Reason: '{disconnect_reason}')...")
		connection_manager.disconnect(websocket, ws_stream_id)

		if session_established_with_mcp and mcp_server_db_id and mcp_conn_manager:
			logger.info(f"[WS/{ws_stream_id}] Releasing MCP session reference for server DB ID '{mcp_server_db_id}'...")
			try:
				await mcp_conn_manager.release_session(mcp_server_db_id)
			except Exception as release_err:
				logger.error(
					f"[WS/{ws_stream_id}] Error releasing MCP session ref for '{mcp_server_db_id}': {release_err}",
					exc_info=True)

		if websocket.client_state == WebSocketState.CONNECTED:
			try:
				await websocket.close(code=status.WS_1000_NORMAL_CLOSURE, reason=disconnect_reason)
			except Exception:
				pass
		logger.info(f"[WS/{ws_stream_id}] Finished WebSocket cleanup for user '{user_id}'.")


# --- Main execution block ---
if __name__ == "__main__":
	import uvicorn

	if not logger.hasHandlers():
		logging.basicConfig(level=logging.DEBUG)
		logger = logging.getLogger(__name__)
	logger.info(f"Starting Uvicorn server for {settings.APP_NAME} on {settings.HOST}:{settings.PORT}")
	logger.info(f"Config: Debug={settings.DEBUG}, Reload={settings.DEBUG}, Env={settings.APP_ENV}")
	logger.info(f"Access API docs at http://{settings.HOST}:{settings.PORT}/docs")
	log_level_name_main = os.getenv("LOG_LEVEL", "DEBUG").upper()
	uvicorn.run("app.main:app", host=settings.HOST, port=settings.PORT, reload=settings.DEBUG,
				log_level=log_level_name_main.lower(), log_config=None)