# app/services/connection_manager.py (Add Capability Storage)
from fastapi import WebSocket
from typing import Dict, List, Any, Optional
import logging
import asyncio
from app.utils.websocket_logger import WebSocketLogger # Assuming this utility exists

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    WebSocket connection manager

    Handles WebSocket connections, message routing, and stores client capabilities.
    """

    def __init__(self):
        # stream_id -> list of WebSockets
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # websocket -> connection metadata (including capabilities)
        self.connection_info: Dict[WebSocket, Dict[str, Any]] = {}
        # Track recently closed connections to avoid race conditions on send
        self.closed_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket, stream_id: str):
        """
        Connect a new WebSocket to a stream and initialize its info storage.

        Args:
            websocket: The WebSocket connection
            stream_id: Identifier for the stream
        """
        await websocket.accept()

        if stream_id not in self.active_connections:
            self.active_connections[stream_id] = []
        self.active_connections[stream_id].append(websocket)

        # Store connection metadata, initializing supported_primitives
        client_info = WebSocketLogger.get_client_info(websocket) # Use helper if available
        self.connection_info[websocket] = {
            'stream_id': stream_id,
            'client_info': client_info,
            'connected_at': asyncio.get_event_loop().time(),
            'messages_sent': 0,
            'bytes_sent': 0,
            'is_open': True,
            'supported_primitives': [] # Initialize as empty list
        }

        logger.info(
            f"Client {client_info} connected to stream: {stream_id}, total connections for stream: {len(self.active_connections[stream_id])}")

    def disconnect(self, websocket: WebSocket, stream_id: str):
        """
        Disconnect a WebSocket from a stream and clean up info.

        Args:
            websocket: The WebSocket connection
            stream_id: Identifier for the stream
        """
        client_info = "unknown"
        if websocket in self.connection_info:
            client_info = self.connection_info[websocket].get('client_info', 'unknown')
            self.connection_info[websocket]['is_open'] = False # Mark as closed

        # Add to recently closed list
        if websocket not in self.closed_connections:
             self.closed_connections.append(websocket)
        # Trim closed connections list periodically if needed
        if len(self.closed_connections) > 100:
            self.closed_connections = self.closed_connections[-50:]

        # Remove from active connections list for the stream
        if stream_id in self.active_connections:
            try:
                if websocket in self.active_connections[stream_id]:
                    self.active_connections[stream_id].remove(websocket)
                # Remove stream entry if no connections left
                if not self.active_connections[stream_id]:
                    del self.active_connections[stream_id]
                    logger.info(f"Removed empty stream: {stream_id}")
            except ValueError:
                logger.warning(f"Attempted to remove non-existent connection from stream: {stream_id}")
            except Exception as e:
                logger.error(f"Error removing connection from stream {stream_id}: {e}")

        # Remove connection metadata after logging stats
        if websocket in self.connection_info:
            stats = self.connection_info[websocket]
            duration = asyncio.get_event_loop().time() - stats.get('connected_at', 0)
            logger.info(
                f"Connection stats for {client_info}: Duration: {duration:.2f}s, Msgs: {stats.get('messages_sent', 0)}, Bytes: {stats.get('bytes_sent', 0)}")
            del self.connection_info[websocket]

        logger.info(f"Client {client_info} disconnected from stream: {stream_id}")

    def store_supported_primitives(self, websocket: WebSocket, primitives: List[str]):
        """
        Stores the list of supported primitives declared by a connected client.

        Args:
            websocket: The WebSocket connection of the client.
            primitives: A list of strings representing the supported primitive names.
        """
        if websocket in self.connection_info:
            self.connection_info[websocket]['supported_primitives'] = primitives
            client_info = self.connection_info[websocket].get('client_info', 'unknown')
            logger.info(f"Stored {len(primitives)} supported primitives for client {client_info}.")
            logger.debug(f"Primitives for {client_info}: {primitives}")
        else:
            logger.warning("Attempted to store primitives for an unknown or disconnected WebSocket.")

    def get_supported_primitives(self, websocket: WebSocket) -> List[str]:
        """
        Retrieves the list of supported primitives for a given client connection.

        Args:
            websocket: The WebSocket connection of the client.

        Returns:
            A list of strings representing the supported primitive names, or an empty list if not found.
        """
        if websocket in self.connection_info:
            return self.connection_info[websocket].get('supported_primitives', [])
        else:
            logger.warning("Attempted to get primitives for an unknown or disconnected WebSocket.")
            return []

    # --- send_text / send_binary methods remain largely the same ---
    # (Make sure they handle potential errors and check self.closed_connections)

    async def send_text(self, message: str, stream_id: str):
        """
        Send a text message to all connections in a stream.
        (Implementation assumes previous version is functional, adding checks)
        """
        if stream_id not in self.active_connections:
            logger.warning(f"Attempted to send message to non-existent stream: {stream_id}")
            return

        # Create a stable list of connections to iterate over
        connections_to_send = list(self.active_connections.get(stream_id, []))
        client_count = len(connections_to_send)
        if not connections_to_send:
            return # No clients in the stream

        logger.debug(f"Sending text message to {client_count} clients in stream {stream_id}")

        disconnected_clients = []
        for connection in connections_to_send:
            try:
                # Double check if connection is still considered open and not recently closed
                if connection in self.closed_connections or \
                   (connection in self.connection_info and not self.connection_info[connection].get('is_open', True)):
                    logger.debug(f"Skipping send to closed/closing connection in stream {stream_id}")
                    continue

                await connection.send_text(message)

                # Update stats if connection info still exists
                if connection in self.connection_info:
                    self.connection_info[connection]['messages_sent'] += 1
                    self.connection_info[connection]['bytes_sent'] += len(message.encode('utf-8'))

            except Exception as e:
                client_info = self.connection_info.get(connection, {}).get('client_info', 'unknown')
                logger.error(f"Error sending text message to client {client_info} in stream {stream_id}: {e}")
                # Avoid modifying list during iteration, mark for removal
                if connection not in disconnected_clients:
                    disconnected_clients.append(connection)

        # Clean up clients that failed during sending
        if disconnected_clients:
            logger.warning(f"Found {len(disconnected_clients)} disconnected clients during send to stream {stream_id}")
            for client in disconnected_clients:
                # Ensure disconnect is called only once
                if client in self.connection_info:
                    self.disconnect(client, stream_id) # disconnect handles removal

    async def send_binary(self, data: bytes, stream_id: str):
        """
        Send binary data to all connections in a stream.
        (Implementation assumes previous version is functional, adding checks)
        """
        # Similar logic to send_text, ensuring checks for closed connections
        if stream_id not in self.active_connections:
            logger.warning(f"Attempted to send binary data to non-existent stream: {stream_id}")
            return

        connections_to_send = list(self.active_connections.get(stream_id, []))
        client_count = len(connections_to_send)
        if not connections_to_send:
            return

        logger.debug(f"Sending {len(data)} bytes of binary data to {client_count} clients in stream {stream_id}")

        disconnected_clients = []
        for connection in connections_to_send:
            try:
                if connection in self.closed_connections or \
                   (connection in self.connection_info and not self.connection_info[connection].get('is_open', True)):
                    logger.debug(f"Skipping binary send to closed/closing connection in stream {stream_id}")
                    continue

                await connection.send_bytes(data)

                if connection in self.connection_info:
                    self.connection_info[connection]['messages_sent'] += 1
                    self.connection_info[connection]['bytes_sent'] += len(data)

            except Exception as e:
                client_info = self.connection_info.get(connection, {}).get('client_info', 'unknown')
                logger.error(f"Error sending binary data to client {client_info} in stream {stream_id}: {e}")
                if connection not in disconnected_clients:
                    disconnected_clients.append(connection)

        if disconnected_clients:
            logger.warning(f"Found {len(disconnected_clients)} disconnected clients during binary send to stream {stream_id}")
            for client in disconnected_clients:
                 if client in self.connection_info:
                    self.disconnect(client, stream_id)


    def get_connection_count(self, stream_id: Optional[str] = None) -> int:
        """Get the number of active connections."""
        if stream_id:
            # Ensure we count only connections still considered active
            return len([
                ws for ws in self.active_connections.get(stream_id, [])
                if ws not in self.closed_connections and self.connection_info.get(ws, {}).get('is_open', False)
            ])
        else:
            # Count all connections across streams that are considered active
            count = 0
            for stream_connections in self.active_connections.values():
                count += len([
                    ws for ws in stream_connections
                    if ws not in self.closed_connections and self.connection_info.get(ws, {}).get('is_open', False)
                ])
            return count

    def get_active_streams(self) -> List[str]:
        """Get a list of stream IDs that have active connections."""
        return [stream_id for stream_id, conns in self.active_connections.items() if self.get_connection_count(stream_id) > 0]

    def get_connection_info(self, websocket: WebSocket) -> Optional[Dict[str, Any]]:
        """Get information about a specific connection."""
        return self.connection_info.get(websocket)


# --- Singleton Pattern or Dependency Injection ---
# Keep the existing get_connection_manager() function if it relies on app.state

# Example assuming app.state is used in main.py lifespan:
def get_connection_manager() -> ConnectionManager:
    """Dependency function to get the ConnectionManager instance."""
    # This assumes the instance is attached to app.state during startup
    from app.main import app # Use appropriate import based on your structure
    if not hasattr(app.state, 'connection_manager'):
        # Initialize fallback or raise error if not found
        logger.error("ConnectionManager not found in app.state!")
        # You might initialize it here as a fallback, but it's better done in lifespan
        app.state.connection_manager = ConnectionManager()
    return app.state.connection_manager

