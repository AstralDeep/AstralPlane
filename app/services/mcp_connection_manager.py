# app/services/mcp_connection_manager.py
# --- VERSION ADDING CONNECTION CHECK BEFORE SENDING NOTIFICATIONS ---

import asyncio
import logging
from typing import Dict, Any, Optional, List, Tuple, Union, Set
import copy
import time
import anyio
from contextlib import AsyncExitStack
import json
from datetime import datetime
from functools import partial

# --- MCP SDK Imports (Direct - No Fallbacks) ---
from mcp.client.session import ClientSession
from mcp.shared.context import RequestContext
from mcp.client.sse import sse_client
import mcp.types as mcp_types
from mcp.types import (
    INTERNAL_ERROR, METHOD_NOT_FOUND, INVALID_REQUEST, PARSE_ERROR, INVALID_PARAMS, CallToolResult,
    NotificationParams, ServerNotification,
    ProgressNotificationParams, LoggingMessageNotificationParams,
    ResourceUpdatedNotificationParams,
    CancelledNotificationParams
)
from mcp.shared.exceptions import McpError
from pydantic import ValidationError

# --- App Specific Imports (Direct - No Fallbacks) ---
from app.config import Settings
from app.services.connection_manager import ConnectionManager
from app.models.schemas import (
    PrimitiveContentUpdateMessage, PrimitiveContentUpdatePayload,
    MCPProgressMessage, MCPProgressPayload,
    MCPNotificationMessage, MCPNotificationPayload,
    MCPLogEntry,
    ToolSchemaInfo, ToolSchemasPayload, ToolSchemasMessage
)

# --- Setup Logger ---
logger = logging.getLogger(__name__)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('mcp.client.sse').setLevel(logging.INFO)

# --- Helper Function for Error Creation ---
def _create_error_content(message: str, code: int = INTERNAL_ERROR, code_str: Optional[str] = None) -> Union[Dict[str, Any], Any]:
    """Creates an error object, attempting MCP format first, falling back to dict."""
    if mcp_types and hasattr(mcp_types, 'ErrorData'):
        logger.debug(f"Creating mcp.types.ErrorData with code: {code}, message: {message}")
        return mcp_types.ErrorData(code=code, message=message)
    else:
        logger.warning("mcp_types.ErrorData not available (SDK might be minimal or type missing), falling back to dictionary for error content.")
        error_key = code_str or "ERROR"
        return {error_key: code, "message": message}


class MCPConnectionManager:
    """
    Manages connections to external MCP servers via SSE using a central message_handler.
    """
    def __init__(self, settings: Settings, connection_manager: ConnectionManager):
        logger.info("Initializing MCPConnectionManager (Proactive Connect Mode)...")
        self.settings = settings
        self.connection_manager = connection_manager
        self.server_configs: Dict[str, Dict[str, Any]] = {
            cfg['id']: cfg for cfg in settings.MCP_SERVERS
            if cfg.get('transport') == 'sse' and cfg.get('id') and cfg.get('url')
        }
        self._connection_lock = asyncio.Lock()
        self.sse_connections: Dict[str, Dict[str, Any]] = {}
        for server_id, config in self.server_configs.items():
            self.sse_connections[server_id] = {
                "server_id": server_id, "status": "pending", "ref_count": 0, "config": config,
                "session": None, "exit_stack": None, "tools": None, "ui_layout": None,
                "required_primitives": set(), "error_message": None,
                "last_connect_attempt": None, "last_successful_connect": None,
            }
        logger.info(f"MCPConnectionManager initialized for SSE servers: {list(self.server_configs.keys())}")

    # --- Core Connection and Preparation Logic ---
    async def connect_and_prepare_server(self, server_id: str, server_config: Dict[str, Any]) -> bool:
        logger.info(f"[{server_id}] Proactive connection attempt starting...")
        server_url = server_config.get("url")
        if not server_url:
            logger.error(f"[{server_id}] Proactive connect failed: Missing 'url' in config.")
            await self._update_connection_state(server_id, {"status": "error", "error_message": "Missing URL"})
            return False

        exit_stack = AsyncExitStack()
        session: Optional[ClientSession] = None
        ui_layout: Optional[dict] = None
        processed_tools: Optional[dict] = None
        required_primitives: Set[str] = set()
        connect_time = time.monotonic()

        await self._update_connection_state(server_id, { "status": "connecting", "last_connect_attempt": connect_time, "error_message": None })

        try:
            await exit_stack.__aenter__()
            read_stream, write_stream = await exit_stack.enter_async_context(sse_client(server_url))
            message_handler_with_id = partial(self._handle_incoming_message, server_id=server_id)
            session = await exit_stack.enter_async_context(
                ClientSession(
                    read_stream, write_stream,
                    sampling_callback=self._default_sampling_callback,
                    message_handler=message_handler_with_id
                )
            )
            init_timeout = 15.0; init_result = await asyncio.wait_for(session.initialize(), timeout=init_timeout)
            self._check_mcp_result_for_error(init_result, "Initialize")
            logger.info(f"[{server_id}] Proactive: Initialized.")
            tools_timeout = 15.0; tools_result = await asyncio.wait_for(session.list_tools(), timeout=tools_timeout)
            self._check_mcp_result_for_error(tools_result, "ListTools")
            processed_tools = self._process_discovered_tools(tools_result.tools if tools_result and hasattr(tools_result,'tools') else [])
            logger.info(f"[{server_id}] Proactive: Discovered tools: {list(processed_tools.keys())}")
            ui_layout_timeout = 20.0; ui_layout = await asyncio.wait_for(self._get_server_ui_layout(session, server_id), timeout=ui_layout_timeout)
            if ui_layout: required_primitives = self._extract_required_primitives(ui_layout); logger.info(f"[{server_id}] Proactive: Extracted required primitives: {required_primitives}")
            else: logger.warning(f"[{server_id}] Proactive: UI layout not retrieved.")
            await self._update_connection_state(server_id, { "status": "connected", "session": session, "exit_stack": exit_stack, "tools": processed_tools, "ui_layout": ui_layout, "required_primitives": required_primitives, "error_message": None, "last_successful_connect": time.monotonic(), })
            logger.info(f"[{server_id}] Proactive connection successful. UI Layout Retrieved: {ui_layout is not None}")
            return True
        except (asyncio.TimeoutError, McpError, Exception) as e:
            if isinstance(e, asyncio.TimeoutError): error_msg = f"Proactive connection timed out: {e}"
            elif isinstance(e, McpError): error_msg = f"MCP Error during proactive connection: {e.error}"
            else: error_msg = f"Proactive connection failed: {e}"
            logger.error(f"[{server_id}] {error_msg}", exc_info=not isinstance(e, (McpError, asyncio.TimeoutError)))
            await self._safe_aclose(exit_stack, server_id, "proactive failure cleanup")
            await self._update_connection_state(server_id, { "status": "error", "error_message": str(e), "session": None, "exit_stack": None, "ui_layout": None, "required_primitives": set(), })
            return False

    # --- Sampling Callback (Server -> Backend Request) ---
    async def _default_sampling_callback( self, context: Union[RequestContext["ClientSession", None], Any], params: Union[mcp_types.CreateMessageRequestParams, Any] ) -> Union[mcp_types.CreateMessageResult, mcp_types.ErrorData, Dict[str, Any]]:
        server_id = "unknown"
        if context and hasattr(context, 'session'):
             details = await self._find_details_by_session(context.session)
             if details: server_id = details.get("server_id", "unknown")
        logger.info(f"[{server_id}] Received sampling_callback request (createMessage) from server.")
        logger.debug(f"[{server_id}] Sampling Params: {params}")
        server_message_text = "No message found in server request"
        if mcp_types and params and hasattr(params, 'messages') and params.messages:
             last_message = params.messages[-1]
             if hasattr(last_message, 'content') and isinstance(last_message.content, mcp_types.TextContent): server_message_text = last_message.content.text
        mock_response_text = f"Backend received: '{server_message_text}'. This is a mocked callback response."
        logger.info(f"[{server_id}] Sending mocked sampling response: '{mock_response_text}'")
        if mcp_types and hasattr(mcp_types, 'CreateMessageResult') and hasattr(mcp_types, 'TextContent'):
            return mcp_types.CreateMessageResult(role="assistant", content=mcp_types.TextContent(type="text", text=mock_response_text), model="mock-backend-callback-model", stopReason="endTurn")
        else: return { "role": "assistant", "content": {"type": "text", "text": mock_response_text}, "model": "mock-backend-callback-model-dict", "stopReason": "endTurn" }

    # In app/services/mcp_connection_manager.py -> MCPConnectionManager class

    # In app/services/mcp_connection_manager.py -> MCPConnectionManager class

    async def _handle_incoming_message(self, message: Any, server_id: str):
        """
        Routes notifications received from the MCP server session to specific handlers.
        Refactored to check method name *before* attempting strict Pydantic validation
        for standard types, allowing custom methods to pass through to their handlers.
        """
        # --- Use types from mcp.types if available ---
        # (Keep existing type imports or add as needed)
        ServerNotification = getattr(mcp_types, 'ServerNotification', None)
        ProgressNotificationParams = getattr(mcp_types, 'ProgressNotificationParams', None)
        LoggingMessageNotificationParams = getattr(mcp_types, 'LoggingMessageNotificationParams', None)
        UpdateBindingNotificationParams = getattr(mcp_types, 'UpdateBindingNotificationParams', None)
        NotificationParams = getattr(mcp_types, 'NotificationParams', None)  # Generic base?
        ResourceUpdatedNotificationParams = getattr(mcp_types, 'ResourceUpdatedNotificationParams', None)
        CancelledNotificationParams = getattr(mcp_types, 'CancelledNotificationParams', None)

        # Basic check if it looks like a notification structure
        method_name = None
        params = None
        is_valid_structure = False

        if ServerNotification and isinstance(message, ServerNotification):
            notification_root = message.root
            method_name = getattr(notification_root, 'method', None)
            params = getattr(notification_root, 'params', None)
            is_valid_structure = True
            logger.debug(f"[{server_id}] Received ServerNotification object. Method: {method_name}")
        elif isinstance(message, dict):  # Handle raw dict if SDK passes it directly sometimes
            method_name = message.get('method')
            params = message.get('params')
            is_valid_structure = True
            logger.debug(f"[{server_id}] Received raw dictionary message. Method: {method_name}")
        elif hasattr(message, 'method') and hasattr(message, 'params'):  # Handle generic object
            method_name = getattr(message, 'method', None)
            params = getattr(message, 'params', None)
            is_valid_structure = True
            logger.debug(f"[{server_id}] Received generic object message. Method: {method_name}")

        if not is_valid_structure:
            logger.warning(f"[{server_id}] Received unknown type via message_handler: {type(message)} - {message!r}")
            return

        if not method_name:
            logger.warning(f"[{server_id}] Received message without a 'method' field: {message!r}")
            return

        logger.info(f"[{server_id}] Routing incoming notification. Method: '{method_name}'")

        # --- Route based on method name FIRST ---
        try:
            if method_name == "app/streaming_log_update":
                # Route directly to the custom handler, passing raw params
                # Let the handler do specific parsing/validation if needed
                logger.debug(f"[{server_id}] Routing to custom handler _handle_streaming_update.")
                await self._handle_streaming_update(server_id, params or {})  # Pass params directly

            elif method_name == "notifications/progress":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_progress.")
                # Now attempt validation *within* the specific route
                if (ProgressNotificationParams and isinstance(params, ProgressNotificationParams)) or isinstance(params,
                                                                                                                 dict):
                    await self._handle_progress(server_id, params or {})
                else:
                    logger.warning(f"[{server_id}] Invalid params type for progress: {type(params)}")

            elif method_name == "notifications/message":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_mcp_log_message.")
                if (LoggingMessageNotificationParams and isinstance(params,
                                                                    LoggingMessageNotificationParams)) or isinstance(
                        params, dict):
                    await self._handle_mcp_log_message(server_id, params or {})
                else:
                    logger.warning(f"[{server_id}] Invalid params type for message: {type(params)}")

            elif method_name == "notifications/update_binding":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_update_binding.")
                if (UpdateBindingNotificationParams and isinstance(params,
                                                                   UpdateBindingNotificationParams)) or isinstance(
                        params, dict):
                    await self._handle_update_binding(server_id, params or {})
                else:
                    logger.warning(f"[{server_id}] Invalid params type for update_binding: {type(params)}")

            elif method_name == "notifications/tools/list_changed":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_tool_list_changed.")
                # Assuming generic NotificationParams or dict is acceptable
                await self._handle_tool_list_changed(server_id, params)

            elif method_name == "notifications/resources/updated":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_resource_updated.")
                if (ResourceUpdatedNotificationParams and isinstance(params,
                                                                     ResourceUpdatedNotificationParams)) or isinstance(
                        params, dict):
                    await self._handle_resource_updated(server_id, params or {})
                else:
                    logger.warning(f"[{server_id}] Invalid params type for resources/updated: {type(params)}")

            elif method_name == "notifications/resources/list_changed":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_resource_list_changed.")
                await self._handle_resource_list_changed(server_id, params)

            elif method_name == "notifications/prompts/list_changed":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_prompt_list_changed.")
                await self._handle_prompt_list_changed(server_id, params)

            elif method_name == "notifications/cancelled":
                logger.debug(f"[{server_id}] Routing to standard handler _handle_cancelled_by_server.")
                if (CancelledNotificationParams and isinstance(params, CancelledNotificationParams)) or isinstance(
                        params, dict):
                    await self._handle_cancelled_by_server(server_id, params or {})
                else:
                    logger.warning(f"[{server_id}] Invalid params type for cancelled: {type(params)}")

            else:
                # Log unhandled methods after specific checks
                logger.warning(f"[{server_id}] Received unhandled notification method: {method_name}")

        except Exception as handler_exc:
            # Catch errors within the specific handlers
            logger.error(f"[{server_id}] Error executing handler for method '{method_name}': {handler_exc}",
                         exc_info=True)

    async def _handle_mcp_log_message(self, server_id: str, params: Union[LoggingMessageNotificationParams, Dict, Any]):
        """
        Handles 'notifications/message'. Routes to specific UI bindings if the 'logger'
        field contains a binding path ('mcp_stream:...'), otherwise routes to the default log view.

        *** CORRECTED FIELD ACCESS LOGIC FOR PYDANTIC OBJECTS ***
        """
        logger.debug(
            f"[{server_id}] Handling MCP Log Notification ('notifications/message'). Params Type: {type(params)}, Value: {params!r}")
        stream_id = f"mcp:{server_id}"
        default_log_binding = f"mcp_stream:{server_id}:log_messages"
        TARGETED_BINDING_PREFIX = f"mcp_stream:{server_id}:"
        expected_stream_logger_binding = f"{TARGETED_BINDING_PREFIX}raw_llm_stream"

        target_binding = default_log_binding
        is_targeted_update = False
        is_raw_stream_chunk = False
        final_content = None
        update_type = "append"

        try:
            # --- Safely extract fields using getattr (works for objects) ---
            # Provide default values in case the attributes are missing (though they shouldn't be for valid logs)
            log_level_raw = getattr(params, 'level', 'log')  # Default level 'log'
            log_data = getattr(params, 'data', None)  # Default data None
            logger_name = getattr(params, 'logger', None)  # Default logger None
            # --- End Safe Extraction ---

            if log_data is None:  # Check if data is missing early
                logger.warning(
                    f"[{server_id}] Log/Update message has None or missing 'data'. Ignoring. Logger='{logger_name}'")
                return

            # --- Determine the actual target binding (Logic remains the same) ---
            if isinstance(logger_name, str) and logger_name.startswith(TARGETED_BINDING_PREFIX):
                if logger_name == expected_stream_logger_binding:
                    is_raw_stream_chunk = True
                    target_binding = logger_name
                    final_content = str(log_data)  # Ensure string for raw stream
                    update_type = "append"
                    logger.debug(f"[{server_id}] Detected raw stream chunk via specific logger name: '{logger_name}'.")
                else:
                    is_targeted_update = True
                    target_binding = logger_name
                    final_content = log_data  # Keep original type (dict, str, etc.)
                    update_type = "replace"
                    logger.debug(f"[{server_id}] Detected targeted UI update via logger name: '{target_binding}'.")
            else:
                target_binding = default_log_binding
                final_content = log_data
                update_type = "append"
                logger.debug(
                    f"[{server_id}] Treating as standard log message (logger='{logger_name}'). Targeting default: '{target_binding}'")

            # --- Construct and send the update message (Logic remains the same) ---

            # Prepare payload based on target type
            content_to_send = None
            if is_targeted_update or is_raw_stream_chunk:
                content_to_send = final_content
                logger.info(
                    f"[{server_id}] Sending '{update_type}' update to binding '{target_binding}'. Data Type: {type(content_to_send).__name__}")
            else:  # Standard log message for the default view
                log_level = str(log_level_raw).lower()
                valid_levels = {"error", "warning", "info", "debug", "log"}
                if log_level not in valid_levels: log_level = "log"
                log_data_str = str(final_content)
                try:
                    # Ensure MCPLogEntry is imported if used
                    from app.models.schemas import MCPLogEntry
                    log_entry = MCPLogEntry(level=log_level, message=log_data_str, timestamp=datetime.now())
                    content_to_send = log_entry.model_dump(exclude_none=True)
                    logger.debug(f"[{server_id}] Sending structured log entry to '{target_binding}'.")
                except ImportError:
                    content_to_send = f"[{log_level.upper()}] {log_data_str}"
                    logger.warning(
                        f"[{server_id}] MCPLogEntry schema not found, sending plain text log to '{target_binding}'.")
                except Exception as log_entry_err:
                    content_to_send = f"[LOG_ERROR] {log_data_str}"  # Fallback if MCPLogEntry fails
                    logger.error(f"[{server_id}] Error creating MCPLogEntry: {log_entry_err}", exc_info=True)

            if content_to_send is None:
                logger.error(
                    f"[{server_id}] Failed to determine content_to_send for notification. Params: {params!r}, Target: {target_binding}")
                return

            # Create the final message
            update_payload = PrimitiveContentUpdatePayload(
                targetBinding=target_binding,
                content=content_to_send,
                updateType=update_type
            )
            update_message = PrimitiveContentUpdateMessage(payload=update_payload)

            # Send to frontend if connected
            if self.connection_manager.get_connection_count(stream_id) > 0:
                json_message = update_message.model_dump_json(exclude_none=True)
                await self.connection_manager.send_text(json_message, stream_id)
                logger.debug(f"[{server_id}] Sent primitive_content_update to binding '{target_binding}'.")
            else:
                logger.debug(
                    f"[{server_id}] No clients for stream {stream_id}, skipping send for binding '{target_binding}'.")

        except AttributeError as ae:
            logger.error(
                f"[{server_id}] AttributeError processing 'notifications/message': {ae}. Params Type: {type(params)}, Params: {params!r}",
                exc_info=True)
        except ValidationError as ve:
            logger.error(
                f"[{server_id}] ValidationError processing 'notifications/message': {ve}. Params Type: {type(params)}, Params: {params!r}",
                exc_info=True)
        except Exception as e:
            logger.error(
                f"[{server_id}] Unexpected error processing 'notifications/message': {e}. Params Type: {type(params)}, Params: {params!r}",
                exc_info=True)

    async def _handle_streaming_update(self, server_id: str, params: Union[Dict, Any]):
        """
        Handles the custom 'app/streaming_log_update' notification.
        EXPECTS params to be structured like the custom StreamingChunkParams model
        (or a dictionary resembling it) with 'targetBinding' and 'chunk' fields.
        Parses this structure and sends a PrimitiveContentUpdateMessage to the frontend.
        """
        # Ensure self.connection_manager is available before proceeding
        if not hasattr(self, 'connection_manager') or self.connection_manager is None:
            logger.error(f"[{server_id}] ConnectionManager not available in _handle_streaming_update. Cannot proceed.")
            return

        logger.debug(
            f"[{server_id}] Handling 'app/streaming_log_update' (expecting Custom Model payload). Params type: {type(params)}, Value: {params!r}")
        # Construct the WebSocket stream ID for routing messages back to the frontend
        stream_id = f"mcp:{server_id}"

        try:
            # <<< Extract targetBinding and chunk from the custom structure >>>
            target_binding = None
            chunk_text = None

            # Check if params is an object with the expected attributes (like StreamingChunkParams)
            if hasattr(params, 'targetBinding') and hasattr(params, 'chunk'):
                target_binding = getattr(params, 'targetBinding', None)
                chunk_text = getattr(params, 'chunk', None)
                logger.debug(f"[{server_id}] Extracted data via attributes (targetBinding, chunk).")
            # Check if params is a dictionary with the expected keys (SDK might deserialize to dict)
            elif isinstance(params, dict):
                target_binding = params.get('targetBinding')
                chunk_text = params.get('chunk')
                logger.debug(f"[{server_id}] Extracted data via dictionary keys (targetBinding, chunk).")
            # Log a warning if the structure is neither an expected object nor a dictionary
            else:
                logger.warning(
                    f"[{server_id}] Received params of unexpected type {type(params)} for 'app/streaming_log_update'. Cannot extract targetBinding/chunk. Ignoring.")
                return

            # <<< Validate extracted data >>>
            # Ensure targetBinding was found and is a non-empty string
            if not target_binding or not isinstance(target_binding, str):
                logger.warning(
                    f"[{server_id}] 'app/streaming_log_update' received invalid or missing 'targetBinding'. Params: {params!r}. Ignoring.")
                return

            # Handle missing chunk: log a warning and default to empty string
            if chunk_text is None:
                logger.warning(
                    f"[{server_id}] 'app/streaming_log_update' received missing 'chunk' for binding '{target_binding}'. Params: {params!r}. Sending empty string.")
                chunk_text = ""  # Default to empty string if chunk is missing

            # --- Send raw chunk to the parsed targetBinding ---
            # Convert chunk_text to string just in case it wasn't already
            chunk_text_str = str(chunk_text)
            logger.info(
                f"[{server_id}] Sending raw chunk to binding '{target_binding}': '{chunk_text_str[:100]}...'")

            # Construct the PrimitiveContentUpdatePayload for the frontend message
            # Ensure PrimitiveContentUpdatePayload/Message are imported from app.models.schemas
            raw_payload = PrimitiveContentUpdatePayload(
                targetBinding=target_binding,  # The frontend element ID to update
                content=chunk_text_str,  # The actual data chunk
                updateType="append"  # Append the chunk to existing content
            )
            # Wrap the payload in the standard message structure
            raw_message_obj = PrimitiveContentUpdateMessage(payload=raw_payload)

            # Check if there are any active WebSocket connections for this stream before sending
            if self.connection_manager.get_connection_count(stream_id) > 0:
                # Serialize the message object to JSON, excluding None values
                json_message = raw_message_obj.model_dump_json(exclude_none=True)
                # Send the JSON message via the WebSocket connection manager
                await self.connection_manager.send_text(json_message, stream_id)
                logger.debug(
                    f"[{server_id}] Relayed raw chunk via PrimitiveContentUpdate to binding '{target_binding}'.")
            else:
                # Log if no clients are connected to the target stream
                logger.debug(
                    f"[{server_id}] No clients for stream {stream_id}, skipping raw chunk relay to '{target_binding}'.")

        # Catch any unexpected errors during the processing of this notification
        except Exception as e:
            logger.error(f"[{server_id}] Error processing 'app/streaming_log_update' with Custom Model payload: {e}",
                         exc_info=True)  # Log the full traceback for debugging

    async def _handle_update_binding(self, server_id: str, params: Union[Dict, Any]):  # Use generic type hint
        """Handles the 'notifications/update_binding' message from the MCP server."""
        logger.debug(f"[{server_id}] Handling Update Binding Notification: {params}")
        stream_id = f"mcp:{server_id}"
        try:
            # Use .get() for dictionaries, getattr() as fallback if params is an object
            target_binding = params.get('binding') if isinstance(params, dict) else getattr(params, 'binding', None)
            content_payload = params.get('payload') if isinstance(params, dict) else getattr(params, 'payload', None)

            if not target_binding:
                logger.error(
                    f"[{server_id}] Received update_binding notification with missing 'binding'. Params: {params!r}")
                return

            logger.info(f"[{server_id}] Relaying update for binding '{target_binding}' to frontend.")

            # Ensure content_payload is serializable
            serializable_content = content_payload
            if hasattr(content_payload, 'model_dump'):  # Check if it's a Pydantic model
                serializable_content = content_payload.model_dump(exclude_none=True)
            elif not isinstance(content_payload, (str, int, float, bool, list, dict, type(None))):
                # If it's not a basic type or None, try converting to string as fallback
                logger.warning(
                    f"[{server_id}] update_binding payload type {type(content_payload)} might not be directly serializable. Converting to str().")
                serializable_content = str(content_payload)

            update_payload_obj = PrimitiveContentUpdatePayload(
                targetBinding=target_binding,
                content=serializable_content,
                updateType="replace"  # Assuming replace is desired for binding updates
            )
            update_msg = PrimitiveContentUpdateMessage(payload=update_payload_obj)

            # Send to frontend via ConnectionManager
            if self.connection_manager.get_connection_count(stream_id) > 0:
                await self.connection_manager.send_text(update_msg.model_dump_json(exclude_none=True), stream_id)
            else:
                logger.debug(
                    f"[{server_id}] No clients for stream {stream_id}, skipping update_binding send to frontend.")

        except Exception as e:
            logger.error(f"[{server_id}] Error processing/sending update_binding notification: {e}", exc_info=True)

    # --- Specific Notification Handlers ---

    async def _handle_progress(self, server_id: str, params: Union[ProgressNotificationParams, Dict]):
        logger.debug(f"[{server_id}] Handling Progress Notification: {params}")
        stream_id = f"mcp:{server_id}"
        # --- ADDED CHECK ---
        if self.connection_manager.get_connection_count(stream_id) == 0:
            logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping progress send.")
            return
        # --- END CHECK ---
        try:
            payload = MCPProgressPayload(server_id=server_id, token=getattr(params, 'progressToken', None), percentage=getattr(params, 'percentage', None), message=getattr(params, 'message', None), title=getattr(params, 'title', None))
            message = MCPProgressMessage(payload=payload)
            await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
        except Exception as e: logger.error(f"[{server_id}] Error processing/sending progress notification: {e}", exc_info=True)



    async def _refresh_tools_and_notify_frontend(self, server_id: str):
        """
        Fetches the latest tool list from the server and notifies the frontend.
        Designed to be run as a background task.
        """
        logger.info(f"[{server_id}] Background task started: Refreshing tool list.")
        session, error = await self.get_or_create_session(server_id)  # Get session reference for the task
        if error or not session:
            logger.error(f"[{server_id}] Background task: Cannot refresh tools, session not available ({error})")
            # No session reference was acquired, so no release needed here
            return

        processed_tools = None  # Define before try
        try:
            tools_timeout = 15.0  # Keep a reasonable timeout for the background task
            tools_result = await asyncio.wait_for(session.list_tools(), timeout=tools_timeout)
            self._check_mcp_result_for_error(tools_result, f"ListTools (Background Update for {server_id})")

            # Ensure tools_result is valid and has 'tools' attribute before accessing
            tools_list = getattr(tools_result, 'tools', []) if tools_result else []
            processed_tools = self._process_discovered_tools(tools_list)

            await self._update_connection_state(server_id, {"tools": processed_tools})
            logger.info(
                f"[{server_id}] Background task: Updated tools after notification: {list(processed_tools.keys())}")

            # --- Send update to frontend ---
            stream_id = f"mcp:{server_id}"
            if self.connection_manager.get_connection_count(stream_id) > 0:
                if processed_tools:
                    tool_schemas_for_payload: Dict[str, ToolSchemaInfo] = {}
                    for tool_name, tool_data in processed_tools.items():
                        if not isinstance(tool_data, dict): continue
                        try:
                            tool_info = ToolSchemaInfo(
                                name=tool_data.get('name', tool_name),
                                description=tool_data.get('description'),
                                input_schema=tool_data.get('inputSchema', tool_data.get('input_schema')),
                                # Check both cases
                                output_schema=tool_data.get('outputSchema', tool_data.get('output_schema'))
                                # Check both cases
                            )
                            tool_schemas_for_payload[tool_name] = tool_info
                        except Exception as schema_err:
                            logger.error(
                                f"[{server_id}] Background task: Error creating ToolSchemaInfo for '{tool_name}': {schema_err}")

                    if tool_schemas_for_payload:
                        schemas_payload = ToolSchemasPayload(server_id=server_id, tools=tool_schemas_for_payload)
                        schemas_message = ToolSchemasMessage(payload=schemas_payload)
                        try:
                            schemas_json = schemas_message.model_dump_json(exclude_none=True, by_alias=True)
                            await self.connection_manager.send_text(schemas_json, stream_id)
                            logger.info(f"[{server_id}] Background task: Sent updated tool schemas to frontend.")
                        except Exception as send_err:
                            logger.error(
                                f"[{server_id}] Background task: Failed to send updated tool schemas: {send_err}",
                                exc_info=True)
                    else:
                        logger.warning(f"[{server_id}] Background task: No valid tool schemas processed to send.")
                else:
                    # Optionally send a generic notification if tools are empty after refresh
                    logger.warning(f"[{server_id}] Background task: Processed tools list is empty after refresh.")
                    # payload = MCPNotificationPayload(server_id=server_id, notification_type="ToolListChanged")
                    # message = MCPNotificationMessage(payload=payload)
                    # await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
            else:
                logger.debug(
                    f"[{server_id}] Background task: No clients connected to stream {stream_id}, skipping tool list update send.")

        except asyncio.TimeoutError:
            logger.error(f"[{server_id}] Background task: Timeout error while re-fetching tools.")
        except McpError as e:
            logger.error(f"[{server_id}] Background task: MCP Error re-fetching tools: {e.error!r}")
        except Exception as e:
            logger.error(f"[{server_id}] Background task: Unexpected error re-fetching tools: {e}", exc_info=True)
        finally:
            # --- Release session reference acquired by this task ---
            await self.release_session(server_id)
            logger.info(f"[{server_id}] Background task finished: Refreshing tool list.")

    # Inside MCPConnectionManager class in app/services/mcp_connection_manager.py

    # --- MODIFY THIS EXISTING HANDLER ---
    async def _handle_tool_list_changed(self, server_id: str, params: Union[NotificationParams, Dict, None]):
        """
        Handles the 'notifications/tools/list_changed' message from the MCP server
        by scheduling a background task to refresh the tool list and notify the frontend.
        """
        logger.info(f"[{server_id}] Handling ToolListChanged Notification. Scheduling background refresh...")

        # --- Schedule the refresh task instead of doing it directly ---
        # This creates the task but doesn't wait for it to complete here.
        asyncio.create_task(self._refresh_tools_and_notify_frontend(server_id))

        # --- The handler now finishes immediately ---
        logger.debug(f"[{server_id}] Tool list refresh task created and handler finished.")
        # --- NO session acquisition/release or await needed in the handler itself anymore ---

    async def _handle_resource_updated(self, server_id: str, params: Union[ResourceUpdatedNotificationParams, Dict]):
        # --- Simplified access ---
        resource_uri = "<unknown_uri>"  # Default
        if hasattr(params, 'uri'):
            resource_uri = str(getattr(params, 'uri'))  # Use getattr and ensure string conversion
        elif isinstance(params, dict) and 'uri' in params:
            resource_uri = str(params.get('uri'))  # Fallback for dict, ensure string
        else:
            logger.warning(f"[{server_id}] Could not extract 'uri' from ResourceUpdatedNotification params: {params!r}")
        # --- End Simplification ---

        stream_id = f"mcp:{server_id}"
        logger.info(
            f"[{server_id}] Handling ResourceUpdated Notification for URI: {resource_uri}")  # Log the extracted URI

        # --- ADDED CHECK ---
        if self.connection_manager.get_connection_count(stream_id) == 0:
            logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping resource updated send.")
            return
        # --- END CHECK ---
        try:
            # Ensure resource_uri passed to payload is a string
            payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceUpdated",
                                             details={"uri": resource_uri})
            message = MCPNotificationMessage(payload=payload)
            await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
        except Exception as e:
            logger.error(f"[{server_id}] Error processing/sending ResourceUpdated notification: {e}", exc_info=True)


    async def _handle_resource_list_changed(self, server_id: str, params: Union[NotificationParams, Dict, None]):
        logger.info(f"[{server_id}] Handling ResourceListChanged Notification.")
        stream_id = f"mcp:{server_id}"
        # --- ADDED CHECK ---
        if self.connection_manager.get_connection_count(stream_id) == 0:
            logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping resource list changed send.")
            return
        # --- END CHECK ---
        try: payload = MCPNotificationPayload(server_id=server_id, notification_type="ResourceListChanged"); message = MCPNotificationMessage(payload=payload); await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
        except Exception as e: logger.error(f"[{server_id}] Error processing/sending ResourceListChanged notification: {e}", exc_info=True)

    async def _handle_prompt_list_changed(self, server_id: str, params: Union[NotificationParams, Dict, None]):
        logger.info(f"[{server_id}] Handling PromptListChanged Notification.")
        stream_id = f"mcp:{server_id}"
        # --- ADDED CHECK ---
        if self.connection_manager.get_connection_count(stream_id) == 0:
            logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping prompt list changed send.")
            return
        # --- END CHECK ---
        try: payload = MCPNotificationPayload(server_id=server_id, notification_type="PromptListChanged"); message = MCPNotificationMessage(payload=payload); await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
        except Exception as e: logger.error(f"[{server_id}] Error processing/sending PromptListChanged notification: {e}", exc_info=True)

    async def _handle_cancelled_by_server(self, server_id: str, params: Union[CancelledNotificationParams, Dict]):
        request_id = getattr(params, 'requestId', params.get('requestId', '<unknown_request>'))
        logger.info(f"[{server_id}] Handling Cancelled Notification FROM SERVER for request ID: {request_id}")
        stream_id = f"mcp:{server_id}"
        # --- ADDED CHECK ---
        if self.connection_manager.get_connection_count(stream_id) == 0:
            logger.debug(f"[{server_id}] No clients connected to stream {stream_id}, skipping server cancellation send.")
            return
        # --- END CHECK ---
        try: payload = MCPNotificationPayload(server_id=server_id, notification_type="CancelledByServer", details={"requestId": request_id}); message = MCPNotificationMessage(payload=payload); await self.connection_manager.send_text(message.model_dump_json(exclude_none=True), stream_id)
        except Exception as e: logger.error(f"[{server_id}] Error processing/sending server cancellation notification: {e}", exc_info=True)


    # --- Sending Notifications (Backend -> Server) ---
    async def get_session_for_notify(self, server_id: str, purpose: str) -> Tuple[Optional[ClientSession], Optional[str]]:
        session, error = await self.get_or_create_session(server_id)
        if error or not session: logger.error(f"[{server_id}] Cannot send {purpose}: Session not available ({error})"); return None, error or "Session not available"
        return session, None
    async def notify_roots_changed(self, server_id: str, roots: List[Dict[str, Any]]):
        method_name = "notifications/roots/list_changed"; logger.info(f"[{server_id}] Requesting to send {method_name} notification.")
        session, error = await self.get_session_for_notify(server_id, method_name)
        if error or not session: return
        try: params = {"roots": roots}; await session.notify(method_name, params=params); logger.info(f"[{server_id}] Sent {method_name} notification.")
        except McpError as mcp_err: logger.error(f"[{server_id}] MCP Error sending {method_name}: {mcp_err.error}", exc_info=False)
        except Exception as e: logger.error(f"[{server_id}] Unexpected error sending {method_name}: {e}", exc_info=True)
        finally: await self.release_session(server_id)
    async def notify_cancelled(self, server_id: str, request_id: str):
        method_name = "notifications/cancelled"; logger.info(f"[{server_id}] Requesting to send {method_name} notification for request ID '{request_id}'.")
        session, error = await self.get_session_for_notify(server_id, method_name)
        if error or not session: return
        try: params = {"requestId": request_id}; await session.notify(method_name, params=params); logger.info(f"[{server_id}] Sent {method_name} notification for request {request_id}.")
        except McpError as mcp_err: logger.error(f"[{server_id}] MCP Error sending {method_name}: {mcp_err.error}", exc_info=False)
        except Exception as e: logger.error(f"[{server_id}] Unexpected error sending {method_name}: {e}", exc_info=True)
        finally: await self.release_session(server_id)


    # --- Tool Execution ---
    async def execute_tool(self, server_id: str, tool_name: str, params: Dict[str, Any], ws_stream_id: str) -> Tuple[Any, Optional[str]]:
        tool_exec_start_time = time.monotonic(); logger.info(f"[{server_id}] Attempting tool '{tool_name}' for WS '{ws_stream_id}'."); logger.debug(f"Params: {params}")
        session, error = await self.get_or_create_session(server_id)
        if error or not session: return _create_error_content(error or "Session not available", INTERNAL_ERROR), error or "Session not available"
        try:
            tools = await self.get_discovered_tools_internal(server_id)
            if tools is None: error_msg = f"Tool discovery data missing or server not ready for {server_id}."; logger.error(f"[{server_id}] {error_msg}"); return _create_error_content(error_msg, INTERNAL_ERROR), error_msg
            if tool_name not in tools: error_msg = f"Tool '{tool_name}' not available on server {server_id}"; logger.error(f"[{server_id}] {error_msg}. Available: {list(tools.keys())}"); return _create_error_content(error_msg, METHOD_NOT_FOUND, "METHOD_NOT_FOUND"), error_msg
            logger.debug(f"[{server_id}] Calling session.call_tool('{tool_name}')...")
            tool_call_timeout = 120.0
            tool_result = await asyncio.wait_for(session.call_tool(name=tool_name, arguments=params), timeout=tool_call_timeout)
            tool_exec_duration = (time.monotonic() - tool_exec_start_time) * 1000; logger.info(f"[{server_id}] Tool '{tool_name}' completed. (Took {tool_exec_duration:.2f} ms).")
            self._check_mcp_result_for_error(tool_result, f"ExecuteTool({tool_name})")
            result_content = getattr(tool_result, 'content', None) if tool_result else None; logger.debug(f"[{server_id}] Tool '{tool_name}' successful. Content type: {type(result_content)}"); return result_content, None
        except asyncio.TimeoutError: error_message = f"Tool call '{tool_name}' timed out after {tool_call_timeout}s."; logger.error(f"[{server_id}] {error_message}"); return _create_error_content(error_message, INTERNAL_ERROR, "TIMEOUT"), error_message
        except anyio.ClosedResourceError as closed_err: error_message = f"MCP connection closed during tool execution: {closed_err}"; logger.error(f"[{server_id}] {error_message}", exc_info=False); await self._update_connection_state(server_id, {"status": "error", "error_message": error_message}); return _create_error_content(error_message, INTERNAL_ERROR), error_message
        except McpError as mcp_err: error_message = f"MCP protocol error during tool execution: {mcp_err.error}"; logger.error(f"[{server_id}] {error_message}", exc_info=False); error_content = getattr(mcp_err, 'error', error_message); return error_content, error_message
        except Exception as e: error_message = f"Unexpected error during tool execution '{tool_name}': {e}"; logger.error(f"[{server_id}] {error_message}", exc_info=True); return _create_error_content(error_message, INTERNAL_ERROR), error_message
        finally: await self.release_session(server_id)


    # --- Session Management ---
    async def get_or_create_session(self, server_id: str) -> Tuple[Optional[ClientSession], Optional[str]]:
        logger.debug(f"[{server_id}] Request received for existing session.")
        async with self._connection_lock:
            connection_details = self.sse_connections.get(server_id)
            if not connection_details: error_msg = f"Server '{server_id}' not configured."; logger.error(error_msg); return None, error_msg
            if connection_details.get("status") == "connected":
                session = connection_details.get("session")
                if session: connection_details["ref_count"] += 1; logger.info(f"[{server_id}] Providing existing session. Ref count: {connection_details['ref_count']}"); return session, None
                else: error_msg = f"Server '{server_id}' is connected but session object is missing (internal error)."; logger.error(f"[{server_id}] {error_msg}"); return None, error_msg
            else: status = connection_details.get("status", "unknown"); error = connection_details.get("error_message", "N/A"); error_msg = f"Server '{server_id}' connection not ready (Status: {status}, Error: {error}). Cannot provide session."; logger.warning(f"[{server_id}] {error_msg}"); return None, error_msg
    async def release_session(self, server_id: str):
        logger.debug(f"[{server_id}] Received request to release session reference.")
        async with self._connection_lock:
            connection_details = self.sse_connections.get(server_id)
            if connection_details and connection_details.get("status") == "connected":
                ref_count = connection_details.get("ref_count", 0)
                if ref_count > 0: ref_count -= 1; connection_details["ref_count"] = ref_count; logger.info(f"[{server_id}] Decremented session reference. New ref count: {ref_count}")
                else: logger.warning(f"[{server_id}] Attempted to release session with ref_count already at 0.")


    # --- State Management and Accessors ---
    async def _update_connection_state(self, server_id: str, updates: Dict[str, Any]):
        async with self._connection_lock:
            if server_id in self.sse_connections: self.sse_connections[server_id].update(updates); logger.debug(f"[{server_id}] Updated connection state: {updates}")
            else: logger.error(f"Attempted to update state for unknown server_id: {server_id}")
    async def get_connection_details(self, server_id: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"[{server_id}] Getting connection details.")
        async with self._connection_lock:
            details = self.sse_connections.get(server_id);
            if not details: return None
            details_copy = { "id": server_id, "status": details.get("status"), "error_message": details.get("error_message"), "ref_count": details.get("ref_count"), "config": details.get("config"), "tools_available": list(details.get("tools", {}).keys()) if details.get("tools") is not None else None, "ui_layout_retrieved": details.get("ui_layout") is not None, "required_primitives": list(details.get("required_primitives", set())), "last_connect_attempt": details.get("last_connect_attempt"), "last_successful_connect": details.get("last_successful_connect"), }; return details_copy
    def get_discovered_tools(self, server_id: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"[{server_id}] Getting discovered tools."); details = self.sse_connections.get(server_id)
        if details and details.get("status") == "connected": tools = details.get("tools"); return copy.deepcopy(tools) if tools is not None else None
        return None
    async def get_discovered_tools_internal(self, server_id: str) -> Optional[Dict[str, Any]]:
        logger.debug(f"[{server_id}] Getting discovered tools state (internal).")
        async with self._connection_lock: details = self.sse_connections.get(server_id);
        if details and details.get("status") == "connected" and details.get("tools") is not None: return details.get("tools")
        return None
    async def get_retrieved_ui_layout(self, server_id: str) -> Optional[dict]:
        logger.debug(f"[{server_id}] Accessing retrieved UI layout.")
        async with self._connection_lock: details = self.sse_connections.get(server_id);
        if details and details.get("status") == "connected": layout = details.get("ui_layout"); return copy.deepcopy(layout) if layout else None
        return None
    async def get_required_primitives(self, server_id: str) -> Optional[Set[str]]:
        logger.debug(f"[{server_id}] Accessing required primitives.")
        async with self._connection_lock: details = self.sse_connections.get(server_id);
        if details and details.get("status") == "connected" and details.get("ui_layout"): return details.get("required_primitives", set()).copy()
        return None
    async def is_server_ui_ready(self, server_id: str) -> bool:
        logger.debug(f"[{server_id}] Checking UI readiness.")
        async with self._connection_lock: details = self.sse_connections.get(server_id);
        return bool(details and details.get("status") == "connected" and details.get("ui_layout") is not None)


    # --- Cleanup Logic ---
    async def cleanup_all_connections(self):
        shutdown_start_time = time.monotonic(); logger.info("Initiating shutdown cleanup for all MCP connections...")
        server_ids_to_clean = list(self.sse_connections.keys()); tasks = []; details_to_clean = {}
        async with self._connection_lock:
            for server_id in server_ids_to_clean: details = self.sse_connections.get(server_id);
            if details and (details.get("exit_stack") or details.get("session")): details["status"] = "disconnecting"; details["ref_count"] = 0; details_to_clean[server_id] = details
        for server_id, details in details_to_clean.items(): logger.info(f"Scheduling shutdown cleanup task for server: {server_id}"); tasks.append(self._cleanup_sse_connection(server_id, details))
        if tasks: results = await asyncio.gather(*tasks, return_exceptions=True);
        for i, result in enumerate(results):
            if isinstance(result, Exception): server_id_err = list(details_to_clean.keys())[i]; logger.error(f"[{server_id_err}] Error during bulk shutdown cleanup task: {result}")
        shutdown_duration = (time.monotonic() - shutdown_start_time) * 1000; logger.info(f"MCPConnectionManager shutdown cleanup initiated for {len(tasks)} connections (Total time: {shutdown_duration:.2f} ms).")
    async def _cleanup_sse_connection(self, server_id: str, connection_details: Dict[str, Any]):
        logger.info(f"[{server_id}] Starting SSE connection cleanup task..."); exit_stack: Optional[AsyncExitStack] = connection_details.get("exit_stack"); await self._safe_aclose(exit_stack, server_id, "shutdown cleanup"); current_error = connection_details.get("error_message")
        await self._update_connection_state(server_id, { "status": "disconnected" if not current_error else "error", "session": None, "exit_stack": None, "ref_count": 0, "error_message": current_error }); logger.info(f"[{server_id}] Marked connection state after cleanup attempt: {self.sse_connections.get(server_id, {}).get('status')}")


    # --- Internal Helper Methods ---
    async def _safe_aclose(self, resource: Optional[AsyncExitStack], server_id: str, context: str):
        if resource and hasattr(resource, 'aclose'):
            try: await resource.aclose(); logger.debug(f"[{server_id}] Successfully closed resource during {context}.")
            except Exception as e: logger.error(f"[{server_id}] Error closing resource during {context}: {e}", exc_info=True)

    def _check_mcp_result_for_error(self, result: Any, operation_name: str):
        """Checks MCP result for errors and raises McpError or Exception if found."""
        # --- This version includes robustness checks from response #19 ---
        if not result:
            return  # No result to check

        error_content = None
        is_error = False
        ErrorData = getattr(mcp_types, 'ErrorData', None)  # Get ErrorData type safely
        TextContent = getattr(mcp_types, 'TextContent', None)  # Get TextContent type safely

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

            # --- Robust Error Handling ---
            if ErrorData and isinstance(error_content, ErrorData):
                # If it's already ErrorData, pass it directly to McpError
                raise McpError(error=error_content)
            elif isinstance(error_content, dict) and 'message' in error_content and 'code' in error_content:
                # If it's a dictionary with expected fields, try creating McpError
                try:
                    mcp_error_data = ErrorData(**error_content) if ErrorData else error_content
                    raise McpError(error=mcp_error_data)
                except Exception as construct_err:
                    logger.error(f"Failed to construct McpError from dict, raising generic exception: {construct_err}")
                    raise Exception(f"MCP Operation '{operation_name}' failed: {error_content}")
            elif isinstance(error_content, list) and error_content and TextContent:
                # Handle the list[TextContent] case observed earlier
                first_item = error_content[0]
                if hasattr(first_item, 'text') and isinstance(first_item, TextContent):
                    error_message_text = first_item.text
                    logger.warning(f"Raising generic exception for list-based MCP error content.")
                    # Raise standard Exception as McpError expects specific structure
                    raise Exception(f"MCP Operation '{operation_name}' failed: {error_message_text}")
                else:
                    logger.warning(f"Raising generic exception for unknown list-based MCP error content.")
                    raise Exception(f"MCP Operation '{operation_name}' failed with list content: {error_content!r}")
            else:
                # Fallback for other types or if McpError construction failed
                logger.warning(f"Raising generic exception for unknown MCP error content type: {type(error_content)}")
                # Raise standard Exception as McpError expects specific structure
                raise Exception(f"MCP Operation '{operation_name}' failed: {error_content!r}")

    def _process_discovered_tools(self, tools_list: Optional[List[Any]]) -> Dict[str, Any]:
        processed_tools: Dict[str, Any] = {};
        if not mcp_types: logger.warning("Cannot process tools: mcp_types missing."); return processed_tools
        if not tools_list: logger.debug("Tool list is empty or None."); return processed_tools
        logger.debug(f"Processing {len(tools_list)} discovered tools..."); tool_class_to_check = None
        if tools_list: first_item = tools_list[0];
        if hasattr(mcp_types, 'Tool') and isinstance(first_item, mcp_types.Tool): tool_class_to_check = mcp_types.Tool
        elif hasattr(mcp_types, 'ToolInfo') and isinstance(first_item, mcp_types.ToolInfo): tool_class_to_check = mcp_types.ToolInfo
        else: logger.warning(f"Unexpected tool type in list: {type(first_item)}. Cannot process.") ; return processed_tools
        for tool_info in tools_list:
            if not isinstance(tool_info, tool_class_to_check): logger.warning(f"Skipping unexpected type in tool list: {type(tool_info)}"); continue
            tool_name = getattr(tool_info, 'name', None);
            if not tool_name: logger.warning(f"Skipping tool missing 'name': {tool_info!r}"); continue
            input_schema = getattr(tool_info, 'inputSchema', getattr(tool_info, 'input_schema', None)); output_schema = getattr(tool_info, 'outputSchema', getattr(tool_info, 'output_schema', None))
            tool_data = { "name": tool_name, "description": getattr(tool_info, 'description', ''), "input_schema": input_schema, "output_schema": output_schema }; processed_tools[tool_name] = tool_data
        logger.debug(f"Finished processing tools: Found {len(processed_tools)} valid tools."); return processed_tools
    async def _get_server_ui_layout(self, session: ClientSession, server_id_for_log: str) -> Optional[dict]:
        tool_name = "get_ui_layout"; logger.info(f"[{server_id_for_log}] Attempting to retrieve UI layout using tool: '{tool_name}'")
        try:
            tool_result: CallToolResult = await session.call_tool(name=tool_name, arguments=None);
            self._check_mcp_result_for_error(tool_result, tool_name) # Check for errors first
            if tool_result and hasattr(tool_result, 'content') and isinstance(tool_result.content, list) and len(tool_result.content) == 1:
                content_item = tool_result.content[0]; ui_layout = None
                if isinstance(content_item, dict): ui_layout = content_item; logger.info(f"[{server_id_for_log}] Retrieved UI layout as dictionary.")
                elif mcp_types and isinstance(content_item, mcp_types.TextContent):
                    logger.debug(f"[{server_id_for_log}] Received UI layout as TextContent. Parsing JSON.")
                    try: ui_layout = json.loads(content_item.text); logger.info(f"[{server_id_for_log}] Parsed UI layout from TextContent.")
                    except json.JSONDecodeError as json_err: logger.error(f"[{server_id_for_log}] Failed to parse JSON from '{tool_name}' TextContent: {json_err}. Content: {content_item.text[:200]}..."); return None
                else: logger.error(f"[{server_id_for_log}] Unexpected content item type ({type(content_item)}) from '{tool_name}'."); return None
                if not isinstance(ui_layout, dict) or 'id' not in ui_layout: logger.error(f"[{server_id_for_log}] Retrieved or parsed UI layout is invalid."); return None
                return ui_layout
            else: logger.error(f"[{server_id_for_log}] Unexpected content format from '{tool_name}': {getattr(tool_result, 'content', 'N/A')!r}"); return None
        except McpError as e:
            err_code = getattr(getattr(e, 'error', None), 'code', None)
            if err_code == METHOD_NOT_FOUND: logger.warning(f"[{server_id_for_log}] UI layout tool '{tool_name}' not found on server.")
            else: logger.error(f"[{server_id_for_log}] MCP protocol error calling '{tool_name}': {e.error!r}", exc_info=False)
            return None # Return None on McpError
        except Exception as e:
             logger.error(f"[{server_id_for_log}] Unexpected exception calling '{tool_name}': {e}", exc_info=True)
             return None # Return None on other exceptions
    def _extract_required_primitives(self, layout: Optional[Dict[str, Any]]) -> Set[str]:
        primitives = set();
        if not layout or not isinstance(layout, dict): return primitives
        primitive_type = layout.get('type');
        if primitive_type and isinstance(primitive_type, str): primitives.add(primitive_type)
        children = layout.get('children');
        if children and isinstance(children, list):
            for child in children:
                if isinstance(child, dict): primitives.update(self._extract_required_primitives(child))
                else: logger.warning(f"Invalid child type ({type(child)}) found in UI layout children list for element ID '{layout.get('id', 'unknown')}'. Skipping.")
        return primitives
    async def _find_details_by_session(self, session_instance: ClientSession) -> Optional[Dict[str, Any]]:
         """Finds the connection state dictionary associated with a session instance."""
         async with self._connection_lock:
             for details in self.sse_connections.values():
                 if details.get("session") is session_instance:
                     return details
         return None


# --- Dependency function for FastAPI ---
# --- CORRECTED VERSION PROVIDED BY USER ---
def get_mcp_connection_manager() -> "MCPConnectionManager":
    logger.debug("Dependency: get_mcp_connection_manager called.")
    try:
        from app.main import app # Import happens *inside* the function
        if not hasattr(app.state, 'mcp_connection_manager') or app.state.mcp_connection_manager is None:
            critical_error_msg = "CRITICAL: MCPConnectionManager not found/initialized in app.state!"
            logger.critical(critical_error_msg); raise RuntimeError(critical_error_msg)
        instance = app.state.mcp_connection_manager
        logger.debug("Dependency: Returning MCPConnectionManager instance."); return instance
    except ImportError: critical_error_msg = "Could not import app state for MCPConnectionManager."; logger.critical(critical_error_msg); raise RuntimeError(critical_error_msg)
    except Exception as e: critical_error_msg = f"Unexpected error getting MCPConnectionManager: {e}"; logger.critical(critical_error_msg, exc_info=True); raise RuntimeError(critical_error_msg)

