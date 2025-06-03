import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel

import httpx
from fastapi import (
	FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, Path, Request, UploadFile, File
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette import status
from starlette.websockets import WebSocketState

from app.api import mcp_server_management
from app.api import projects, auth, websockets
# --- Core App Dependencies ---
from app.config import settings
from app.lifecycle.lifespan import lifespan
# --- Model Imports ---
from app.models.schemas import (
	InitialUIStateMessage, InitialUIStatePayload,
	PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
	ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage
)
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


class ToolExecutionRequest(BaseModel):
	tool_name: str
	params: Dict[str, Any]

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

	mcp_url = details.get("url")
	upload_path = "/api/upload-file"

	if not mcp_url:
		raise HTTPException(
			status_code=404,
			detail=f"MCP server '{server_id}' not found or is not configured for file uploads."
		)

	mcp_upload_url = f"{mcp_url.rstrip('/sse')}{upload_path}"
	logger.info(f"Proxying file '{file.filename}' to generic endpoint at {mcp_upload_url}")

	async with httpx.AsyncClient() as client:
		try:
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


@app.get("/api/mcp-servers/{server_id}", tags=["MCP Server Management"])
async def get_mcp_server_details(
    server_id: str,
    mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    """
    Retrieves the current state and details of a specific MCP server,
    including its list of available tools with full schemas.
    """
    logger.info(f"[HTTP-GET] Received request for details of server '{server_id}'")

    # Get the basic details (status, url, etc.)
    details = await mcp_conn_manager.get_connection_details(server_id)

    if not details:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MCP Server with ID '{server_id}' not found or registered."
        )

    # --- START OF FIX ---
    # Explicitly fetch the full tool schemas, which have the descriptions.
    # The WebSocket endpoint code shows that `get_discovered_tools` returns a dictionary.
    discovered_tools_dict = await mcp_conn_manager.get_discovered_tools(server_id)

    # The orchestrator expects a LIST of tool objects. Let's convert the dictionary.
    if discovered_tools_dict:
        tools_as_list = []
        for name, schema in discovered_tools_dict.items():
            # Create a new dictionary for each tool that includes its name
            tool_dict = {"name": name}
            tool_dict.update(schema) # Add the rest of the schema (description, etc.)
            tools_as_list.append(tool_dict)

        # Replace the simplified list in the details with our new detailed list.
        details["tools_available"] = tools_as_list
    else:
        # Ensure it's an empty list if for some reason no tools were found
        details["tools_available"] = []
    # --- END OF FIX ---

    return details


@app.post("/api/mcp-servers/{server_id}/execute", tags=["MCP Server Management"])
async def execute_mcp_tool(
	server_id: str,
	request: ToolExecutionRequest,
	mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""
	Allows an external service (like an orchestrator) to execute a tool on a
	specific MCP server via a simple HTTP request.
	"""
	logger.info(f"[HTTP-EXEC] Received request to run tool '{request.tool_name}' on server '{server_id}'")
	logger.debug(f"[HTTP-EXEC] Parameters: {request.params}")

	# Check if the server is even connected
	server_details = await mcp_conn_manager.get_connection_details(server_id)
	if not server_details or server_details.get("status").lower() != "connected":
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail=f"MCP Server '{server_id}' is not available or connected."
		)

	try:
		# Use the existing tool execution logic from the connection manager
		# We provide a simple string for the stream_id as it's not a real websocket stream
		mcp_payload_content, mcp_error_obj = await mcp_conn_manager.execute_tool(
			server_id=server_id,
			tool_name=request.tool_name,
			params=request.params,
			ws_stream_id=f"http-execution-for-{server_id}"
		)

		if mcp_error_obj:
			# If the tool execution itself resulted in a known error
			logger.error(f"[HTTP-EXEC] Tool '{request.tool_name}' failed: {mcp_error_obj}")
			raise HTTPException(
				status_code=status.HTTP_400_BAD_REQUEST,
				detail=str(mcp_error_obj)
			)

		logger.info(f"[HTTP-EXEC] Tool '{request.tool_name}' executed successfully.")
		return mcp_payload_content

	except Exception as e:
		logger.error(
			f"[HTTP-EXEC] An unexpected error occurred while trying to execute tool '{request.tool_name}' on '{server_id}': {e}",
			exc_info=True
		)
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
			detail="An internal error occurred while executing the tool."
		)


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
	disconnect_reason = "Handler normal exit"

	try:
		logger.debug(f"[WS/{ws_stream_id}] WebSocket authentication SKIPPED for development.")
		user_id = "dev_user"

		if not stream_path_param.startswith("mcp:"):
			disconnect_reason = "Invalid target stream format"
			await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason=disconnect_reason)
			return

		mcp_server_db_id = stream_path_param.split(":", 1)[1]
		if not mcp_server_db_id:
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
				connection_manager.store_supported_primitives(websocket, supported_primitives)
				logger.info(f"[WS/{ws_stream_id}] Handshake OK. Client capabilities: {supported_primitives}")
			else:
				raise WebSocketDisconnect(code=status.WS_1002_PROTOCOL_ERROR,
										  reason="Protocol error: Expected 'register_capabilities' message")
		except asyncio.TimeoutError:
			raise WebSocketDisconnect(code=status.WS_1008_POLICY_VIOLATION, reason="Handshake timeout")
		except json.JSONDecodeError:
			raise WebSocketDisconnect(code=status.WS_1003_UNSUPPORTED_DATA, reason="Invalid handshake JSON")

		logger.info(f"[WS/{ws_stream_id}] Attempting to get MCP session for server DB ID '{mcp_server_db_id}'...")
		mcp_client_session, mcp_session_error = await mcp_conn_manager.get_or_create_session(mcp_server_db_id)

		if mcp_session_error or not mcp_client_session:
			error_detail = f"Failed to get MCP session for server '{mcp_server_db_id}': {mcp_session_error or 'Session object is None.'}"
			logger.error(f"[WS/{ws_stream_id}] {error_detail}")
			server_name_for_error = mcp_server_db_id
			try:
				server_details_for_error = await mcp_conn_manager.get_connection_details(mcp_server_db_id)
				if server_details_for_error: server_name_for_error = server_details_for_error.get("name",
																								  mcp_server_db_id)
				await websocket.send_text(json.dumps({"type": "error", "payload": {
					"message": f"Target service '{server_name_for_error}' not available: {mcp_session_error}"}}))
			except Exception as send_err:
				logger.error(f"[WS/{ws_stream_id}] Error sending MCP session acquisition error to client: {send_err}")
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR,
									  reason=f"MCP Session Error: {mcp_session_error}")

		session_established_with_mcp = True
		logger.info(f"[WS/{ws_stream_id}] Successfully obtained MCP session for server DB ID '{mcp_server_db_id}'.")

		logger.info(f"[WS/{ws_stream_id}] Generating initial UI state and sending tool schemas...")
		try:
			root_element = await views_service.get_project_ui_hierarchy(websocket=websocket, stream_id=ws_stream_id,
																		mcp_session=mcp_client_session)
			if root_element:
				await websocket.send_text(
					InitialUIStateMessage(payload=InitialUIStatePayload(rootElement=root_element)).model_dump_json(
						exclude_none=True))
				logger.info(f"[WS/{ws_stream_id}] Sent initial UI state to client.")

				discovered_tools = await mcp_conn_manager.get_discovered_tools(mcp_server_db_id)
				tool_schemas_for_payload = {
					tool_name: ToolSchemaInfo(**tool_data) for tool_name, tool_data in discovered_tools.items()
				} if discovered_tools else {}
				await websocket.send_text(ToolSchemasMessage(payload=ToolSchemasPayload(server_id=mcp_server_db_id,
																						tools=tool_schemas_for_payload)).model_dump_json(
					exclude_none=True, by_alias=True))
				logger.info(f"[{ws_stream_id}] Successfully sent {len(tool_schemas_for_payload)} tool schemas.")
			else:
				logger.warning(f"[{ws_stream_id}] No UI hierarchy generated for '{mcp_server_db_id}'.")
				await websocket.send_text(
					json.dumps({"type": "status", "payload": {"message": "UI could not be generated."}}))
		except Exception as ui_gen_err:
			raise WebSocketDisconnect(code=status.WS_1011_INTERNAL_ERROR,
									  reason=f"UI Generation/Schema Error: {ui_gen_err}")

		logger.info(f"[WS/{ws_stream_id}] Entering main message loop for user '{user_id}'...")
		while True:
			message_text = await websocket.receive_text()
			WebSocketLogger.log_text_received(websocket, ws_stream_id, message_text)
			try:
				parsed_message_data = json.loads(message_text)
				message_type = parsed_message_data.get("type")

				### START OF FIX ###
				if message_type == "ui_action":
					action_id = "<parsing_failed>"
					try:
						payload = parsed_message_data.get("payload", {})
						action_id = payload.get("actionId")
						arguments_list_from_ui: List[Any] = payload.get("arguments", [])

						if not action_id:
							logger.warning(f"[WS/{ws_stream_id}] 'ui_action' received with no actionId.")
							continue

						logger.debug(
							f"[WS/{ws_stream_id}] Translating args for tool '{action_id}'. Received list: {arguments_list_from_ui}")

						discovered_tools = await mcp_conn_manager.get_discovered_tools(mcp_server_db_id)
						tool_schema = discovered_tools.get(action_id)

						arguments_to_pass: Dict[str, Any] = {}

						if not tool_schema:
							logger.error(
								f"[WS/{ws_stream_id}] Cannot find schema for tool '{action_id}'. Cannot translate arguments.")
							# Fallback: Send an empty dictionary, which will likely cause a validation error on the MCP server,
							# but is safer than guessing. The MCP server will report the missing arguments.
							arguments_to_pass = {}
						elif 'input_schema' in tool_schema and tool_schema['input_schema'] and 'properties' in \
								tool_schema['input_schema']:
							param_names = list(tool_schema['input_schema']['properties'].keys())
							arguments_to_pass = dict(zip(param_names, arguments_list_from_ui))
							logger.info(f"[WS/{ws_stream_id}] Translated args for '{action_id}': {arguments_to_pass}")
						else:
							logger.info(
								f"[WS/{ws_stream_id}] Tool '{action_id}' has no input properties. Passing empty dict.")
							arguments_to_pass = {}

						mcp_payload_content, mcp_error_obj = await mcp_conn_manager.execute_tool(
							server_id=mcp_server_db_id, tool_name=action_id,
							params=arguments_to_pass,  # This is now a dictionary
							ws_stream_id=ws_stream_id,
						)
						update_binding = f"mcp_stream:{mcp_server_db_id}:{action_id}_result"
						if mcp_error_obj:
							logger.error(f"[WS/{ws_stream_id}] MCP tool error '{action_id}': {mcp_error_obj}")
							error_text_for_ui = "Error executing action."  # Default
							if isinstance(mcp_error_obj, str):
								error_text_for_ui = mcp_error_obj
							elif hasattr(mcp_error_obj, 'message'):
								error_text_for_ui = mcp_error_obj.message
							error_ui_content = {"role": "error", "text": str(error_text_for_ui)}
							error_payload = PrimitiveContentUpdatePayload(targetBinding=update_binding,
																		  content=error_ui_content, updateType="append")
							await websocket.send_text(
								PrimitiveContentUpdateMessage(payload=error_payload).model_dump_json(exclude_none=True))
						else:
							### START OF FIX ###
							final_content_for_ui: Any = f"Tool '{action_id}' returned no response."
							update_type = "append"  # Default update type

							if mcp_payload_content and isinstance(mcp_payload_content, list) and len(
									mcp_payload_content) > 0:
								first_item = mcp_payload_content[0]
								if hasattr(first_item, 'text') and hasattr(first_item, 'type') and getattr(first_item,
																										   'type') == 'text':
									tool_response_text = getattr(first_item, 'text', '{}')

									if action_id == 'transcribe_audio_action':
										# For transcription, the content is the raw JSON string itself.
										# This is what the other tools expect to receive as input.
										final_content_for_ui = tool_response_text
										# We replace the content of the TextView, not append to it.
										update_type = "replace"
									else:
										# For other actions like Q&A, parse the display text for the UI
										# and wrap it in a chat-style message dictionary for appending.
										try:
											parsed_data = json.loads(tool_response_text)
											display_text = parsed_data.get('display_text', tool_response_text)
										except (json.JSONDecodeError, TypeError):
											display_text = tool_response_text
										final_content_for_ui = {"role": "assistant", "text": display_text}
										update_type = "append"

							# Construct and send the correct update message
							result_payload = PrimitiveContentUpdatePayload(
								targetBinding=update_binding,
								content=final_content_for_ui,
								updateType=update_type
							)
							await websocket.send_text(
								PrimitiveContentUpdateMessage(payload=result_payload).model_dump_json(exclude_none=True)
							)
					except Exception as action_err:
						logger.error(f"[WS/{ws_stream_id}] Error processing ui_action '{action_id}': {action_err}",
									 exc_info=True)
				### END OF FIX ###

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
