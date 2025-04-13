# app/main.py
# --- VERSION MODIFIED TO CORRECTLY HANDLE TOOL RESULT SERIALIZATION ---

import os
import json
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Tuple, Any
import pprint
from datetime import datetime

from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, Query, Path, Request
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocketState
from pydantic import ValidationError

# --- Core App Dependencies ---
from app.config import settings
from app.services.connection_manager import ConnectionManager, get_connection_manager
from app.services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager
from app.services.project_service import ProjectService, ProjectViewsService
from app.services.auth_service import authenticate_websocket

# --- Model Imports ---
from app.models.schemas import (
    UIElement, InitialUIStateMessage, InitialUIStatePayload,
    PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
    UIActionMessage,
    ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage,
    RootsChangedMessage, CancelRequestMessage
)

# --- MCP Types Import ---
try:
    import mcp.types as mcp_types
    print("Successfully imported mcp.types in app/main.py")
except ImportError:
    logging.warning("MCP SDK types (mcp.types) not found. Type checking for MCP results will be skipped.")
    mcp_types = None # Set to None if import fails

# --- Logging Setup ---
try:
    from app.utils.logging_config import configure_logging
    from app.utils.websocket_logger import WebSocketLogger
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    configure_logging(log_level=log_level, log_to_file=settings.DEBUG, log_dir="logs")
    logger = logging.getLogger(__name__)
    logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")
except ImportError as log_import_err:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.warning(f"Enhanced logging modules not found ({log_import_err}), using basic logging.")
    class WebSocketLogger: # type: ignore
        @staticmethod
        def get_client_info(ws): return f"{ws.client.host}:{ws.client.port}" if ws.client else "unknown"
        @staticmethod
        def log_connection(ws, sid, uid=None): logger.info(f"WS CONNECT: {sid} User: {uid} Client: {WebSocketLogger.get_client_info(ws)}")
        @staticmethod
        def log_disconnection(ws, sid, reason=None): logger.info(f"WS DISCONNECT: {sid} Reason: {reason} Client: {WebSocketLogger.get_client_info(ws)}")
        @staticmethod
        def log_text_received(ws, sid, msg): logger.debug(f"WS RECV RAW: {sid} Text: {msg[:200]}...")
        @staticmethod
        def log_text_sent(sid, msg, count=1): logger.debug(f"WS SENT: {sid} ({count} clients) Text: {msg[:200]}...")
        @staticmethod
        def log_error(sid, msg, exc=None): logger.error(f"WS ERROR: {sid} {msg}", exc_info=exc)
        @staticmethod
        async def log(ws, msg, level='debug'): # async stub if called with await
            if hasattr(logger, level): getattr(logger, level)(f"[WS/{ws.scope.get('path', '?')}] {msg}")


# --- Lifespan Event Handler --- (Unchanged)
@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info(f"Starting up {settings.APP_NAME}...")

    # --- Initialize Managers ---
    logger.info("Initializing ConnectionManager (WebSocket)...")
    connection_manager_instance = ConnectionManager()
    app_instance.state.connection_manager = connection_manager_instance
    logger.info("ConnectionManager instance created and stored in app.state.")

    logger.info("Initializing MCPConnectionManager (Proactive Connect Mode)...")
    mcp_conn_manager_instance: Optional[MCPConnectionManager] = None
    try:
        mcp_conn_manager_instance = MCPConnectionManager(
            settings=settings,
            connection_manager=connection_manager_instance
        )
        app_instance.state.mcp_connection_manager = mcp_conn_manager_instance
        logger.info("MCPConnectionManager instance created (with ConnectionManager) and stored in app.state.")

        logger.info("Attempting proactive connection to configured MCP servers...")
        connect_tasks = []
        if mcp_conn_manager_instance:
            for server_id, server_config in mcp_conn_manager_instance.server_configs.items():
                logger.info(f"Scheduling proactive connection attempt for server: {server_id}")
                connect_tasks.append(
                    asyncio.create_task(
                        mcp_conn_manager_instance.connect_and_prepare_server(server_id, server_config),
                        name=f"mcp_connect_{server_id}"
                    )
                )
        if connect_tasks:
            connect_timeout = 30.0
            logger.info(f"Waiting up to {connect_timeout}s for initial MCP server connections...")
            done, pending = await asyncio.wait(connect_tasks, timeout=connect_timeout)
            success_count = 0; fail_count = 0; timeout_count = 0
            for task in done:
                task_name = task.get_name()
                try:
                    result = task.result()
                    if result: success_count += 1; logger.info(f"Proactive connection task '{task_name}' succeeded.")
                    else: fail_count += 1; logger.warning(f"Proactive connection task '{task_name}' failed (returned False).")
                except Exception as task_exc: fail_count += 1; logger.error(f"Error during proactive connection task '{task_name}': {task_exc}", exc_info=False)
            for task in pending:
                task_name = task.get_name(); timeout_count += 1; logger.warning(f"Proactive connection task '{task_name}' timed out after {connect_timeout}s. Cancelling."); task.cancel()
            logger.info(f"Proactive connection attempts complete: {success_count} succeeded, {fail_count} failed, {timeout_count} timed out.")
        else: logger.info("No MCP servers configured for proactive connection.")

    except Exception as mcp_init_err:
         logger.critical(f"FAILED to initialize MCPConnectionManager: {mcp_init_err}", exc_info=True)
         app_instance.state.mcp_connection_manager = None # Mark as failed

    # --- Initialize ProjectViewsService ---
    logger.info("Attempting to initialize ProjectViewsService instance explicitly...")
    try:
         cm = app_instance.state.connection_manager
         mcp_cm = app_instance.state.mcp_connection_manager
         if cm and mcp_cm:
             logger.debug("Dependencies found for PVS. Instantiating ProjectViewsService...")
             pvs_instance = ProjectViewsService(mcp_conn_manager=mcp_cm, connection_manager=cm)
             app_instance.state.project_views_service = pvs_instance
             logger.info("ProjectViewsService initialized explicitly and stored in app.state.")
         elif not cm: logger.error("Cannot initialize PVS: ConnectionManager missing."); app_instance.state.project_views_service = None
         else: logger.error("Cannot initialize PVS: MCPConnectionManager instance missing."); app_instance.state.project_views_service = None
    except Exception as pvs_init_err:
         logger.error(f"Error during explicit ProjectViewsService initialization: {pvs_init_err}", exc_info=True)
         app_instance.state.project_views_service = None

    logger.info(f"{settings.APP_NAME} startup complete.")
    yield # Application runs here
    logger.info(f"Shutting down {settings.APP_NAME}...")

    # --- Cleanup logic ---
    if hasattr(app_instance.state, 'mcp_connection_manager') and app_instance.state.mcp_connection_manager:
        logger.info("Cleaning up MCPConnectionManager connections...")
        mcp_manager: MCPConnectionManager = app_instance.state.mcp_connection_manager
        try: await mcp_manager.cleanup_all_connections()
        except Exception as mcp_cleanup_err: logger.error(f"Error during MCPConnectionManager cleanup: {mcp_cleanup_err}", exc_info=True)
    if hasattr(app_instance.state, "connection_manager"):
        cm: ConnectionManager = app_instance.state.connection_manager
        connection_count = cm.get_connection_count()
        if connection_count > 0:
             logger.info(f"Closing {connection_count} active WebSocket connections...")
             # Add graceful closing logic if needed
        else: logger.info("No active WebSocket connections found during shutdown.")

    logger.info(f"{settings.APP_NAME} shutdown complete.")


# --- Initialize FastAPI App --- (Unchanged)
app = FastAPI(
    title=settings.APP_NAME,
    description="Backend API using proactive MCP connections and server-driven UI.",
    version="0.9.1",
    lifespan=lifespan
)

# --- CORS Middleware --- (Unchanged)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# --- API Routers --- (Unchanged)
from app.api import projects, auth, websockets
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(projects.router, prefix="/api/projects", tags=["Projects/Servers"])
if hasattr(websockets, 'router'):
    app.include_router(websockets.router, prefix="/api/ws", tags=["WebSocket Utils"])


# --- Exception Handlers --- (Unchanged)
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.warning(f"HTTP Exception: Status={exc.status_code}, Detail='{exc.detail}' for URL='{request.url}' from {request.client.host if request.client else 'unknown'}")
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=getattr(exc, "headers", None))

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception during request to {request.url} from {request.client.host if request.client else 'unknown'}: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "An internal server error occurred."})

# --- Health Check Endpoint --- (Unchanged)
@app.get("/api/health", tags=["Health"])
async def health_check(mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)):
    mcp_manager_status = "unavailable"; pvs_status = "unavailable"; mcp_server_statuses = {}
    if mcp_conn_manager:
         mcp_manager_status = "initialized"
         for server_id in mcp_conn_manager.server_configs.keys():
              details = await mcp_conn_manager.get_connection_details(server_id)
              if details: mcp_server_statuses[server_id] = { "status": details.get("status"), "error": details.get("error_message"), "ui_ready": details.get("ui_layout_retrieved", False) }
              else: mcp_server_statuses[server_id] = {"status": "unknown", "error": "State missing"}
    if hasattr(app.state, 'project_views_service') and app.state.project_views_service: pvs_status = "initialized"
    return { "status": "ok", "timestamp": datetime.now().isoformat(), "service": settings.APP_NAME, "version": app.version, "mcp_manager_status": mcp_manager_status, "project_views_service_status": pvs_status, "mcp_server_connections": mcp_server_statuses }


# --- WebSocket Endpoint Definition (MODIFIED in ui_action result handling) ---
@app.websocket("/api/ws/stream/{stream_path_param:path}")
async def websocket_endpoint(
    websocket: WebSocket,
    stream_path_param: str = Path(..., description="Stream identifier, e.g., 'mcp:<server_id>'"),
    connection_manager: ConnectionManager = Depends(get_connection_manager),
    mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    """Handles WebSocket connections for streaming UI updates and MCP interaction."""
    ws_client_info = WebSocketLogger.get_client_info(websocket)
    logger.info(f">>> [WS/{stream_path_param}] Accepted connection from {ws_client_info} <<<")

    # --- Retrieve ProjectViewsService --- (Unchanged)
    views_service: Optional[ProjectViewsService] = None
    try:
        views_service = websocket.app.state.project_views_service
        if not views_service:
             logger.error(f"[WS/{stream_path_param}] ProjectViewsService not available. Cannot generate UI.")
             await websocket.close(code=1011, reason="Internal server error: UI Service unavailable")
             return
    except AttributeError:
         logger.error(f"[WS/{stream_path_param}] 'project_views_service' not found on app state.")
         await websocket.close(code=1011, reason="Internal server error: UI Service not configured")
         return

    mcp_server_id: Optional[str] = None
    session_established = False
    session_key_for_cleanup = None
    ws_stream_id = stream_path_param
    user_id = "<unauthenticated>"
    is_authenticated = False
    disconnect_reason = "Handler Exit"

    try:
        # --- WebSocket Authentication --- (Unchanged)
        logger.debug(f"[WS/{ws_stream_id}] Attempting WebSocket authentication...")
        is_authenticated, user_data, error_message = await authenticate_websocket(websocket)
        if not is_authenticated:
            logger.warning(f"[WS/{ws_stream_id}] WebSocket Auth FAILED: {error_message}")
            disconnect_reason = f"Authentication Failed: {error_message}"
            await websocket.close(code=4003, reason=disconnect_reason)
            return
        user_id = user_data.get("id", "<error_id>") if isinstance(user_data, dict) else "<invalid_data>"
        logger.info(f"[WS/{ws_stream_id}] WebSocket Auth successful for user '{user_id}'.")

        # --- Parse Target MCP Server ID --- (Unchanged)
        if not stream_path_param.startswith("mcp:"):
             logger.error(f"[WS/{ws_stream_id}] Invalid stream path format.")
             disconnect_reason = "Invalid target stream format"; await websocket.close(code=1008, reason=disconnect_reason); return
        mcp_server_id = stream_path_param.split(":", 1)[1]
        if not mcp_server_id:
             logger.error(f"[WS/{ws_stream_id}] Missing MCP server ID."); disconnect_reason = "Missing target server ID"; await websocket.close(code=1008, reason=disconnect_reason); return
        logger.info(f"[WS/{ws_stream_id}] Target MCP Server ID: '{mcp_server_id}'")
        session_key_for_cleanup = mcp_server_id

        # --- Register WebSocket with ConnectionManager --- (Unchanged)
        logger.debug(f"[WS/{ws_stream_id}] Connecting WebSocket via ConnectionManager...")
        await connection_manager.connect(websocket, ws_stream_id)
        if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_connection'):
            WebSocketLogger.log_connection(websocket, ws_stream_id, user_id)
        logger.debug(f"[WS/{ws_stream_id}] WebSocket registered.")

        # --- Handshake: Receive Client Capabilities --- (Unchanged)
        handshake_timeout = 10.0
        logger.info(f"[WS/{ws_stream_id}] Waiting for 'register_capabilities' (Timeout: {handshake_timeout}s)...")
        try:
            first_message_text = await asyncio.wait_for(websocket.receive_text(), timeout=handshake_timeout)
            if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_text_received'): WebSocketLogger.log_text_received(websocket, ws_stream_id, first_message_text)
            else: logger.debug(f"[WS/{ws_stream_id}] RECV HANDSHAKE TEXT: {first_message_text[:100]}...")
            handshake_data = json.loads(first_message_text)
            if handshake_data.get("type") == "register_capabilities":
                supported_primitives = handshake_data.get("payload", {}).get("supported_primitives", [])
                if isinstance(supported_primitives, list): connection_manager.store_supported_primitives(websocket, supported_primitives); logger.info(f"[WS/{ws_stream_id}] Handshake OK. Client capabilities: {supported_primitives}")
                else: disconnect_reason = "Invalid capabilities format"; raise WebSocketDisconnect(code=1003, reason=disconnect_reason)
            else: disconnect_reason = "Protocol error: Expected capabilities"; raise WebSocketDisconnect(code=1002, reason=disconnect_reason)
        except asyncio.TimeoutError: disconnect_reason = "Handshake timeout"; raise WebSocketDisconnect(code=1008, reason=disconnect_reason)
        except json.JSONDecodeError: disconnect_reason = "Invalid handshake JSON"; raise WebSocketDisconnect(code=1003, reason=disconnect_reason)
        except WebSocketDisconnect as wsd: logger.warning(f"[WS/{ws_stream_id}] Disconnecting during handshake: {wsd.reason}"); raise wsd
        except Exception as handshake_err: logger.error(f"[WS/{ws_stream_id}] Handshake error: {handshake_err}", exc_info=True); disconnect_reason = "Handshake error"; raise WebSocketDisconnect(code=1011, reason=disconnect_reason)

        # --- Get Pre-established MCP Session --- (Unchanged)
        logger.info(f"[WS/{ws_stream_id}] Getting existing MCP session for server '{mcp_server_id}'...")
        mcp_session, mcp_error = await mcp_conn_manager.get_or_create_session(mcp_server_id)
        if mcp_error or not mcp_session:
            error_detail = f"Failed to get MCP session for '{mcp_server_id}': {mcp_error}"
            logger.error(f"[WS/{ws_stream_id}] {error_detail}")
            try: await websocket.send_text(json.dumps({ "type": "error", "payload": {"message": f"Target service '{mcp_server_id}' not available: {mcp_error}"} }))
            except Exception as send_err: logger.error(f"[WS/{ws_stream_id}] Error sending MCP session error: {send_err}")
            disconnect_reason = f"MCP Session Error: {mcp_error}"; raise WebSocketDisconnect(code=1011, reason=disconnect_reason)
        session_established = True
        logger.info(f"[WS/{ws_stream_id}] Obtained pre-established MCP session for '{mcp_server_id}'.")

        # --- Generate Initial UI State & Send Tool Schemas --- (Unchanged)
        logger.info(f"[WS/{ws_stream_id}] Generating initial UI state and sending tool schemas...")
        try:
            root_element: Optional[UIElement] = await views_service.get_project_ui_hierarchy(websocket=websocket, stream_id=ws_stream_id)
            if root_element:
                initial_state_message = InitialUIStateMessage(payload=InitialUIStatePayload(rootElement=root_element))
                initial_state_json = initial_state_message.model_dump_json(exclude_none=True)
                await websocket.send_text(initial_state_json)
                if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_text_sent'): WebSocketLogger.log_text_sent(ws_stream_id, initial_state_json)
                else: logger.debug(f"[WS/{ws_stream_id}] SENT Initial UI State.")
                logger.info(f"[WS/{ws_stream_id}] Sent initial UI state to client.")

                logger.info(f"[{ws_stream_id}] Attempting to send tool schemas for server '{mcp_server_id}'...")
                discovered_tools = mcp_conn_manager.get_discovered_tools(mcp_server_id)
                if discovered_tools:
                    tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
                    for tool_name, tool_data in discovered_tools.items():
                        if not isinstance(tool_data, dict): logger.warning(f"[{ws_stream_id}] Skipping invalid tool data for '{tool_name}'."); continue
                        try:
                            tool_info = ToolSchemaInfo( name=tool_data.get('name', tool_name), description=tool_data.get('description'), input_schema=tool_data.get('input_schema'), output_schema=tool_data.get('output_schema') )
                            tool_schemas_for_payload[tool_name] = tool_info
                        except ValidationError as schema_err: logger.error(f"[{ws_stream_id}] Failed to validate/create ToolSchemaInfo for '{tool_name}': {schema_err}")
                        except Exception as parse_err: logger.error(f"[{ws_stream_id}] Error processing tool schema data for '{tool_name}': {parse_err}")
                    if tool_schemas_for_payload:
                        schemas_payload = ToolSchemasPayload(server_id=mcp_server_id, tools=tool_schemas_for_payload)
                        schemas_message = ToolSchemasMessage(payload=schemas_payload)
                        try:
                            schemas_json = schemas_message.model_dump_json(exclude_none=True, by_alias=True)
                            await websocket.send_text(schemas_json)
                            if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_text_sent'): WebSocketLogger.log_text_sent(ws_stream_id, schemas_json)
                            else: logger.debug(f"[WS/{ws_stream_id}] SENT Tool Schemas.")
                            logger.info(f"[{ws_stream_id}] Successfully sent tool schemas for {len(tool_schemas_for_payload)} tools.")
                        except Exception as send_schema_err: logger.error(f"[{ws_stream_id}] Failed to serialize or send tool schemas: {send_schema_err}", exc_info=True)
                    else: logger.warning(f"[{ws_stream_id}] No valid tool schemas processed to send for server '{mcp_server_id}'.")
                else: logger.warning(f"[{ws_stream_id}] No discovered tools found for server '{mcp_server_id}'. Cannot send schemas.")
            else:
                logger.warning(f"[{ws_stream_id}] No UI hierarchy generated by ProjectViewsService for '{mcp_server_id}'. Informing client.")
                await websocket.send_text(json.dumps({ "type": "status", "payload": {"message": f"Could not generate UI for server '{mcp_server_id}'."} }))
        except Exception as ui_gen_err:
             logger.error(f"[WS/{ws_stream_id}] Error during UI generation/schema sending phase: {ui_gen_err}", exc_info=True)
             try: await websocket.send_text(json.dumps({"type": "error", "payload": {"message": "Failed to generate initial UI/send schemas"}}))
             except Exception as send_err: logger.error(f"[WS/{ws_stream_id}] Error sending UI gen error: {send_err}")
             disconnect_reason = "UI Generation Error"; raise WebSocketDisconnect(code=1011, reason=disconnect_reason)


        # --- Main Message Loop ---
        logger.info(f"[WS/{ws_stream_id}] Entering main message loop for user '{user_id}'...")
        while True:
            message_text = await websocket.receive_text()
            if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_text_received'): WebSocketLogger.log_text_received(websocket, ws_stream_id, message_text)
            else: logger.debug(f"[WS/{ws_stream_id}] RECV TEXT: {message_text[:100]}...")

            try:
                parsed_message_data = json.loads(message_text)
                logger.debug(f"[WS/{ws_stream_id}] DIAGNOSTIC - Received parsed data: {parsed_message_data!r}")
                message_type = parsed_message_data.get("type")

            except json.JSONDecodeError:
                logger.warning(f"[WS/{ws_stream_id}] Received non-JSON: {message_text[:100]}...");
                await websocket.send_text(json.dumps({"type":"error", "payload": {"message": "Invalid JSON"}}));
                continue

            # --- ui_action handling (MODIFIED) ---
            if message_type == "ui_action":
                 action_id = "<parsing_failed>"
                 try:
                    payload = parsed_message_data.get("payload", {})
                    logger.debug(f"[WS/{ws_stream_id}] DIAGNOSTIC - Extracted payload: {payload!r}")

                    action_id = payload.get("actionId")
                    source_element_id = payload.get("sourceElementId")

                    if not action_id: raise ValueError("Received ui_action message with missing 'actionId' in payload")

                    arguments_to_pass = payload.get("arguments", {})
                    logger.info(f"[WS/{ws_stream_id}] DIAGNOSTIC - Extracted arguments for tool '{action_id}': {arguments_to_pass!r}")
                    logger.info(f"[WS/{ws_stream_id}] Received ui_action '{action_id}' from element '{source_element_id}'")

                    if not mcp_conn_manager: raise RuntimeError("MCP Service connection manager unavailable.")

                    # Call the tool
                    mcp_payload, mcp_error_details = await mcp_conn_manager.execute_tool(
                        server_id=mcp_server_id,
                        tool_name=action_id,
                        params=arguments_to_pass,
                        ws_stream_id=ws_stream_id,
                    )

                    # Send result/error back to UI
                    # Convention: Define target binding based on action ID
                    update_binding = f"mcp_stream:{mcp_server_id}:{action_id}_result"
                    logger.debug(f"[WS/{ws_stream_id}] Determined result update binding: {update_binding}")

                    if mcp_error_details:
                        # (Error handling - keep as is)
                        logger.error(f"[WS/{ws_stream_id}] MCP tool error '{action_id}': {mcp_error_details}")
                        error_text = f"Error: {str(mcp_error_details)}"
                        if isinstance(mcp_payload, dict):
                            error_text = mcp_payload.get('message', error_text)
                        elif mcp_types and hasattr(mcp_types, 'ErrorData') and isinstance(mcp_payload,
                                                                                          mcp_types.ErrorData):
                            error_text = mcp_payload.message or error_text
                        error_ui_content = {"role": "error", "text": error_text}
                        error_payload_obj = PrimitiveContentUpdatePayload(targetBinding=update_binding,
                                                                          content=error_ui_content, updateType="append")
                        error_msg = PrimitiveContentUpdateMessage(type="primitive_content_update",
                                                                  payload=error_payload_obj)
                        await websocket.send_text(error_msg.model_dump_json(exclude_none=True))
                        WebSocketLogger.log_text_sent(ws_stream_id, f"Error result for {action_id}")
                    else:
                        # --- START REFINED FIX LOGIC ---
                        actual_text = None  # Initialize as None
                        result_role = "assistant"  # Default role

                        logger.debug(
                            f"[WS/{ws_stream_id}] DEBUG: Processing successful tool result. Payload type: {type(mcp_payload)}")

                        # Check if mcp_payload is a list and contains expected object type
                        if isinstance(mcp_payload, list) and len(mcp_payload) > 0:
                            tool_result_object = mcp_payload[0]
                            logger.debug(
                                f"[WS/{ws_stream_id}] DEBUG: Type of tool_result_object (payload[0]): {type(tool_result_object)}")

                            # Check for SamplingMessage structure using hasattr for robustness
                            if (hasattr(tool_result_object, 'role') and
                                    hasattr(tool_result_object, 'content') and
                                    tool_result_object.content is not None and
                                    hasattr(tool_result_object.content, 'text')):

                                # *** THIS IS THE KEY EXTRACTION ***
                                actual_text = tool_result_object.content.text
                                result_role = tool_result_object.role
                                logger.debug(
                                    f"[WS/{ws_stream_id}] DEBUG: Extracted text via SamplingMessage structure.")

                            # Fallback check for TextContent directly
                            elif (mcp_types and isinstance(tool_result_object, mcp_types.TextContent) and
                                  hasattr(tool_result_object, 'text')):
                                actual_text = tool_result_object.text
                                logger.debug(f"[WS/{ws_stream_id}] DEBUG: Extracted text via TextContent structure.")

                            else:
                                logger.warning(
                                    f"[WS/{ws_stream_id}] Tool '{action_id}' returned list item of unexpected type: {type(tool_result_object)}")

                        else:
                            logger.warning(
                                f"[WS/{ws_stream_id}] Tool '{action_id}' returned unexpected payload format (not list or empty): {type(mcp_payload)}")

                        # If extraction failed, use a clear error message
                        if actual_text is None:
                            actual_text = "Error: Failed to extract text content from tool result."
                            result_role = "error"
                            logger.warning(
                                f"[WS/{ws_stream_id}] Failed to extract text, payload type was: {type(mcp_payload)}")  # Log type

                        # Construct the UI update payload
                        logger.debug(
                            f"[WS/{ws_stream_id}] Sending result: Role='{result_role}', Text='{actual_text[:100]}...'")
                        result_ui_content = {"role": result_role,
                                             "text": actual_text}  # Use the extracted/processed values
                        result_payload_obj = PrimitiveContentUpdatePayload(targetBinding=update_binding,
                                                                           content=result_ui_content,
                                                                           updateType="append")
                        result_msg = PrimitiveContentUpdateMessage(type="primitive_content_update",
                                                                   payload=result_payload_obj)
                        await websocket.send_text(result_msg.model_dump_json(exclude_none=True))
                        WebSocketLogger.log_text_sent(ws_stream_id, f"Success result for {action_id}")
                        # --- END REFINED FIX LOGIC ---

                 except ValidationError as e:
                    err_msg = f"Invalid ui_action format: {e}"
                    logger.warning(f"[WS/{ws_stream_id}] {err_msg}")
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": err_msg}}))
                 except Exception as action_err:
                    err_msg = f"Error processing action '{action_id}'"
                    logger.error(f"[WS/{ws_stream_id}] {err_msg}: {action_err}", exc_info=True)
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": err_msg}}))

            # --- Handle notify_roots_changed --- (Unchanged)
            elif message_type == "notify_roots_changed":
                 logger.debug(f"[WS/{ws_stream_id}] Received 'notify_roots_changed' message.")
                 try:
                    roots_msg = RootsChangedMessage(**parsed_message_data)
                    payload = roots_msg.payload
                    target_server_id = payload.server_id
                    if target_server_id == mcp_server_id:
                        logger.info(f"[WS/{ws_stream_id}] Relaying roots_changed to MCP server '{target_server_id}'. Roots count: {len(payload.roots)}")
                        if mcp_conn_manager: await mcp_conn_manager.notify_roots_changed(target_server_id, payload.roots)
                        else: logger.error(f"[WS/{ws_stream_id}] MCPConnectionManager not available.")
                    else: logger.warning(f"[WS/{ws_stream_id}] Received notify_roots_changed for wrong server ID (Expected: {mcp_server_id}, Got: {target_server_id}). Ignoring.")
                 except ValidationError as e:
                    logger.warning(f"[WS/{ws_stream_id}] Invalid 'notify_roots_changed' format: {e}")
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": f"Invalid roots changed format: {e}"}}))
                 except Exception as e:
                    logger.error(f"[WS/{ws_stream_id}] Error processing notify_roots_changed: {e}", exc_info=True)
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": "Error processing roots changed notification"}}))

            # --- Handle notify_cancelled --- (Unchanged)
            elif message_type == "notify_cancelled":
                 logger.debug(f"[WS/{ws_stream_id}] Received 'notify_cancelled' message.")
                 try:
                    cancel_msg = CancelRequestMessage(**parsed_message_data)
                    payload = cancel_msg.payload
                    target_server_id = payload.server_id
                    request_id = payload.requestId
                    if target_server_id == mcp_server_id:
                        logger.info(f"[WS/{ws_stream_id}] Relaying cancellation for request '{request_id}' to MCP server '{target_server_id}'.")
                        if mcp_conn_manager: await mcp_conn_manager.notify_cancelled(target_server_id, request_id)
                        else: logger.error(f"[WS/{ws_stream_id}] MCPConnectionManager not available.")
                    else: logger.warning(f"[WS/{ws_stream_id}] Received notify_cancelled for wrong server ID (Expected: {mcp_server_id}, Got: {target_server_id}). Ignoring.")
                 except ValidationError as e:
                    logger.warning(f"[WS/{ws_stream_id}] Invalid 'notify_cancelled' format: {e}")
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": f"Invalid cancel request format: {e}"}}))
                 except Exception as e:
                    logger.error(f"[WS/{ws_stream_id}] Error processing notify_cancelled: {e}", exc_info=True)
                    await websocket.send_text(json.dumps({"type":"error", "payload": {"message": "Error processing cancel request"}}))

            # --- Existing ping/unknown handling --- (Unchanged)
            elif message_type == 'ping':
                 await websocket.send_text(json.dumps({"type":"pong", "timestamp": datetime.now().isoformat()}))
            else:
                 logger.warning(f"[WS/{ws_stream_id}] Received unknown message type: '{message_type}'")


    # --- Disconnect / Error Handling --- (Unchanged)
    except WebSocketDisconnect as e:
        disconnect_reason = e.reason or "Client disconnected"
        logger.info(f"[WS/{ws_stream_id}] WebSocket disconnected (Code: {e.code}, Reason: '{disconnect_reason}')")
    except ConnectionRefusedError as cr_err:
        disconnect_reason = "Connection Refused"
        logger.error(f"[WS/{ws_stream_id}] Connection Refused Error: {cr_err}", exc_info=False)
    except Exception as e:
        disconnect_reason = "Internal Server Error"
        logger.error(f"[WS/{ws_stream_id}] Unhandled error in WebSocket handler: {e}", exc_info=True)
        if websocket.client_state == WebSocketState.CONNECTED:
            try: await websocket.close(code=1011, reason=disconnect_reason)
            except Exception as close_err: logger.warning(f"[WS/{ws_stream_id}] Error closing WebSocket after exception: {close_err}")
    finally:
        # --- Cleanup Resources ---
        logger.info(f"[WS/{ws_stream_id}] Cleaning up WebSocket connection for user '{user_id}' (Reason: {disconnect_reason})...")
        if 'connection_manager' in locals() and connection_manager:
             connection_manager.disconnect(websocket, ws_stream_id)
             if 'WebSocketLogger' in locals() and hasattr(WebSocketLogger, 'log_disconnection'):
                 WebSocketLogger.log_disconnection(websocket, ws_stream_id, disconnect_reason)
        else: logger.warning(f"[WS/{ws_stream_id}] ConnectionManager unavailable during cleanup.")

        if session_established and session_key_for_cleanup:
             if 'mcp_conn_manager' in locals() and mcp_conn_manager:
                logger.info(f"[WS/{ws_stream_id}] Releasing MCP session reference for server '{session_key_for_cleanup}'...")
                try: await mcp_conn_manager.release_session(session_key_for_cleanup)
                except Exception as release_err: logger.error(f"[WS/{ws_stream_id}] Error releasing MCP session ref for '{session_key_for_cleanup}': {release_err}", exc_info=True)
             else: logger.warning(f"[WS/{ws_stream_id}] MCPConnectionManager unavailable during cleanup.")
        elif session_key_for_cleanup: logger.debug(f"[WS/{ws_stream_id}] MCP session was not established for '{session_key_for_cleanup}', no release needed.")

        logger.info(f"[WS/{ws_stream_id}] Finished WebSocket cleanup for user '{user_id}'.")


# --- Main execution block --- (Unchanged)
if __name__ == "__main__":
    import uvicorn
    if not logger.hasHandlers(): logging.basicConfig(level=logging.INFO); logger = logging.getLogger(__name__)
    logger.info(f"Starting Uvicorn server for {settings.APP_NAME} on {settings.HOST}:{settings.PORT}")
    logger.info(f"Config: Debug={settings.DEBUG}, Reload={settings.DEBUG}, Env={settings.APP_ENV}")
    logger.info(f"Access API docs at http://{settings.HOST}:{settings.PORT}/docs")
    log_level_name_main = os.getenv("LOG_LEVEL", "DEBUG").upper()
    uvicorn.run( "app.main:app", host=settings.HOST, port=settings.PORT, reload=settings.DEBUG, log_level=log_level_name_main.lower(), log_config=None )