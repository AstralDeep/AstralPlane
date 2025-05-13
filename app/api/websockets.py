import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query, Path
from typing import Dict, List, Any, Optional, Set, Literal
import json
import time
import logging

from ..config import settings
from ..services.connection_manager import ConnectionManager, get_connection_manager
from ..services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager_ws
from app.main import websocket_endpoint as main_mcp_websocket_handler

router = APIRouter()

logger = logging.getLogger(__name__)

# --- NOTE: The primary WebSocket endpoint for MCP streams is now defined in app/main.py ---
# The 'websocket_endpoint' function below is likely legacy code related to the old
# dataplane manager and is commented out to resolve the StreamConfig import error.
# If you need a separate WebSocket endpoint defined here for other purposes,
# ensure it doesn't rely on the removed StreamConfig or dataplane_manager.

# Optional endpoint for WebSocket status information
@router.get("/status", tags=["WebSocket Utils"])
async def get_websocket_status(
    connection_manager: ConnectionManager = Depends(get_connection_manager)
):
    """Get information about active WebSocket connections managed by ConnectionManager."""
    active_streams = connection_manager.get_active_streams()
    total_connections = connection_manager.get_connection_count()

    stream_info = []
    for stream_id in active_streams:
        stream_info.append({
            "stream_id": stream_id,
            "connection_count": connection_manager.get_connection_count(stream_id)
        })

    logger.debug(f"WebSocket status requested: {total_connections} total connections across {len(active_streams)} streams.")
    return {
        "total_connections": total_connections,
        "active_streams": len(active_streams),
        "streams": stream_info
    }


class WebSocketMonitor:
    """Class to monitor WebSocket messages (for debugging/admin)."""
    def __init__(self, max_messages=1000):
        self.max_messages = max_messages
        self.messages: List[Dict[str, Any]] = [] # Store recent messages
        self.monitors: Set[WebSocket] = set()   # Active monitoring clients
        self.lock = asyncio.Lock()             # Protect access to messages/monitors

    async def add_message(self, message: Dict[str, Any]):
        """Add a message to the monitor log and broadcast it."""
        async with self.lock:
            # Add timestamp if not present
            if 'timestamp' not in message:
                message['timestamp'] = time.time()

            # Add the message
            self.messages.append(message)

            # Trim message history if it exceeds max length
            if len(self.messages) > self.max_messages:
                self.messages = self.messages[-self.max_messages:] # Keep only the most recent

            # Broadcast to all connected monitor clients
            await self._broadcast_message_locked(message) # Use internal locked broadcast

    async def _broadcast_message_locked(self, message: Dict[str, Any]):
        """Broadcast a message to all monitoring connections (must hold lock)."""
        if not self.monitors: # Skip if no monitors are connected
            return

        message_json = json.dumps(message) # Encode once
        disconnected = set()

        # Iterate over a copy of the set to allow modification during iteration
        for monitor_ws in list(self.monitors):
            try:
                await monitor_ws.send_text(message_json)
            except Exception as e:
                 # If sending fails, assume the monitor disconnected
                 logger.warning(f"Failed to send message to WebSocket monitor {monitor_ws.client}: {e}. Marking for removal.")
                 disconnected.add(monitor_ws)

        # Remove disconnected monitors from the active set
        self.monitors -= disconnected
        if disconnected:
             logger.info(f"Removed {len(disconnected)} disconnected monitors. Remaining: {len(self.monitors)}")


    def get_recent_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get the most recent messages from the log."""
        # No lock needed for read if list access is atomic enough, but lock is safer
        # async with self.lock: # If using async lock
        with self.lock: # If using threading lock or assuming list access is safe
            safe_limit = min(limit, self.max_messages)
            return list(self.messages[-safe_limit:]) # Return a copy

    async def add_monitor(self, websocket: WebSocket):
        """Add a new WebSocket monitor client."""
        async with self.lock:
            self.monitors.add(websocket)
            client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
            logger.info(f"WebSocket monitor connected: {client_info}. Total monitors: {len(self.monitors)}")

    async def remove_monitor(self, websocket: WebSocket):
        """Remove a WebSocket monitor client."""
        async with self.lock:
            if websocket in self.monitors:
                self.monitors.remove(websocket)
                client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
                logger.info(f"WebSocket monitor disconnected: {client_info}. Remaining monitors: {len(self.monitors)}")


# Create a global WebSocket monitor instance
# This instance will be shared across requests via the dependency injector
websocket_monitor = WebSocketMonitor()


# Dependency function to get the global monitor instance
def get_websocket_monitor():
    """FastAPI dependency to provide the WebSocketMonitor instance."""
    return websocket_monitor


# WebSocket endpoint for monitoring traffic
@router.websocket("/monitor")
async def websocket_monitor_endpoint(
        websocket: WebSocket,
        monitor: WebSocketMonitor = Depends(get_websocket_monitor),
        # --- Authentication ---
        # Require a token/password for accessing the monitor, especially in production.
        # Example: Get token from query param
        token: Optional[str] = Query(None, description="Authentication token for monitor access")
        # Example: Use a simple password from settings (less secure)
        # password: Optional[str] = Query(None)
):
    """Endpoint for real-time monitoring of WebSocket traffic events."""
    client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
    logger.debug(f"Monitor connection attempt from: {client_info}")

    # --- Simple Authentication Example ---
    # Replace with proper token validation (e.g., JWT check) in a real app
    is_authorized = False
    if settings.DEBUG and token == "debug_token": # Allow simple token in debug
        is_authorized = True
    elif settings.ADMIN_PASSWORD and token == settings.ADMIN_PASSWORD: # Check against admin password
        is_authorized = True
    # Add more robust auth checks here using your auth service if needed

    if not is_authorized:
        logger.warning(f"Unauthorized monitor connection attempt from {client_info}.")
        await websocket.close(code=1008, reason="Unauthorized") # 1008: Policy Violation
        return

    # Accept the authorized connection
    await websocket.accept()
    logger.info(f"Monitor connection accepted from: {client_info}")

    try:
        # Register this client with the monitor
        await monitor.add_monitor(websocket)

        # Send initial batch of recent messages upon connection
        recent_messages = monitor.get_recent_messages(100) # Send last 100 messages
        if recent_messages:
            initial_data = {
                "type": "monitor_initial", # Indicate this is the initial batch
                "messages": recent_messages
            }
            await websocket.send_text(json.dumps(initial_data))
            logger.debug(f"Sent initial {len(recent_messages)} messages to monitor {client_info}")


        # Keep the monitor connection alive and listen for commands (e.g., ping)
        while True:
            # Receive messages from the monitor client (optional)
            # Example: Handle ping from client to keep connection alive
            try:
                 data = await websocket.receive_text()
                 message = json.loads(data)
                 if message.get("type") == "ping":
                     await websocket.send_text(json.dumps({"type": "pong"}))
            except WebSocketDisconnect:
                 # Client disconnected gracefully
                 break
            except json.JSONDecodeError:
                 logger.warning(f"Monitor client {client_info} sent non-JSON message: {data[:100]}")
            except Exception as recv_err:
                 logger.error(f"Error receiving from monitor client {client_info}: {recv_err}")
                 break # Assume connection is broken

            # Add a small sleep to prevent tight loop if not receiving messages
            await asyncio.sleep(0.1)

    except WebSocketDisconnect:
        # Log graceful disconnection
        logger.info(f"Monitor client {client_info} disconnected.")
    except Exception as monitor_err:
        # Log unexpected errors in the monitor loop
        logger.error(f"Error in monitor endpoint for {client_info}: {monitor_err}", exc_info=True)
    finally:
        # Ensure the monitor client is removed from the active set on disconnect/error
        await monitor.remove_monitor(websocket)


# --- Helper function to log events to the monitor ---
# Call this function from various points in your WebSocket logic (e.g., main.py)
# to send events to connected monitor clients.
async def log_to_monitor(
    message_type: str,
    stream_id: str,
    direction: Literal["incoming", "outgoing", "connection", "error"], # Type hint for direction
    content: Any,
    metadata: Optional[Dict[str, Any]] = None,
    client_info: Optional[str] = None
):
    """
    Log a WebSocket event to the central WebSocket monitor.

    Args:
        message_type: Type of event (e.g., "text", "binary", "connect", "disconnect", "error").
        stream_id: The WebSocket stream identifier (e.g., "mcp:sse_server_1").
        direction: Direction of the message or type of event.
        content: Message content (string preview), binary size, or error details.
        metadata: Additional context (e.g., user ID, source element).
        client_info: Identifier for the specific client involved.
    """
    global websocket_monitor # Access the global monitor instance
    if not websocket_monitor:
        logger.warning("WebSocket monitor not initialized, cannot log event.")
        return

    monitor_data = {
        "type": "ws_event", # Consistent type for monitor events
        "event_type": message_type, # Specific type of event/message
        "stream_id": stream_id,
        "direction": direction,
        "timestamp": time.time(),
    }

    # Add client info if available
    if client_info:
        monitor_data["client_info"] = client_info

    # Format content based on its type
    content_preview = {}
    if isinstance(content, str):
        try:
            # Attempt to parse JSON for better preview
            json_content = json.loads(content)
            content_preview = {
                "type": json_content.get("type", "json"), # Get message type from JSON payload if possible
                "preview": content[:200] + ('...' if len(content) > 200 else '') # Increased preview length
            }
            monitor_data["is_json"] = True
        except json.JSONDecodeError:
            # Plain text
            content_preview = {
                "type": "text",
                "preview": content[:200] + ('...' if len(content) > 200 else '')
            }
            monitor_data["is_json"] = False
    elif isinstance(content, bytes):
        content_preview = {
            "type": "binary",
            "size_bytes": len(content)
        }
        monitor_data["is_binary"] = True
    elif isinstance(content, Exception):
         content_preview = {
            "type": "exception",
            "class": type(content).__name__,
            "message": str(content)
         }
    elif isinstance(content, dict) or isinstance(content, list):
         # Handle dict/list content (e.g., parsed JSON before sending)
         try:
             preview_str = json.dumps(content)
             content_preview = {
                 "type": "object",
                 "preview": preview_str[:200] + ('...' if len(preview_str) > 200 else '')
             }
             monitor_data["is_json"] = True # Indicate it's structured data
         except Exception:
              content_preview = {"type": "object", "preview": str(content)[:200]} # Fallback
    else:
        # Other types (e.g., connection status messages)
        content_preview = {
            "type": "other",
            "preview": str(content)[:200]
        }

    monitor_data["content"] = content_preview

    # Add additional metadata if provided
    if metadata:
        monitor_data["metadata"] = metadata

    # Add the formatted event to the monitor
    try:
        await websocket_monitor.add_message(monitor_data)
    except Exception as log_err:
         # Prevent logging errors from crashing the main application flow
         logger.error(f"Failed to add message to WebSocket monitor: {log_err}", exc_info=True)