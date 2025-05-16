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
        await websocket.accept()
        if stream_id not in self.active_connections:
            self.active_connections[stream_id] = []
        self.active_connections[stream_id].append(websocket)
        client_info = WebSocketLogger.get_client_info(websocket)
        self.connection_info[websocket] = {
            'stream_id': stream_id,
            'client_info': client_info,
            'connected_at': asyncio.get_event_loop().time(),
            'messages_sent': 0,
            'bytes_sent': 0,
            'is_open': True,
            'supported_primitives': []
        }
        logger.info(
            f"Client {client_info} connected to stream: {stream_id}, total connections for stream: {len(self.active_connections[stream_id])}")

    def disconnect(self, websocket: WebSocket, stream_id: str):
        client_info = "unknown"
        if websocket in self.connection_info:
            client_info = self.connection_info[websocket].get('client_info', 'unknown')
            self.connection_info[websocket]['is_open'] = False
        if websocket not in self.closed_connections:
            self.closed_connections.append(websocket)
        if len(self.closed_connections) > 100:
            self.closed_connections = self.closed_connections[-50:]
        if stream_id in self.active_connections:
            try:
                if websocket in self.active_connections[stream_id]:
                    self.active_connections[stream_id].remove(websocket)
                if not self.active_connections[stream_id]:
                    del self.active_connections[stream_id]
                    logger.info(f"Removed empty stream: {stream_id}")
            except ValueError:
                logger.warning(f"Attempted to remove non-existent connection from stream: {stream_id}")
            except Exception as e:
                logger.error(f"Error removing connection from stream {stream_id}: {e}")
        if websocket in self.connection_info:
            stats = self.connection_info[websocket]
            duration = asyncio.get_event_loop().time() - stats.get('connected_at', 0)
            logger.info(
                f"Connection stats for {client_info}: Duration: {duration:.2f}s, Msgs: {stats.get('messages_sent', 0)}, Bytes: {stats.get('bytes_sent', 0)}")
            del self.connection_info[websocket]
        logger.info(f"Client {client_info} disconnected from stream: {stream_id}")

    def store_supported_primitives(self, websocket: WebSocket, primitives: List[str]):
        if websocket in self.connection_info:
            self.connection_info[websocket]['supported_primitives'] = primitives
            client_info = self.connection_info[websocket].get('client_info', 'unknown')
            logger.info(f"Stored {len(primitives)} supported primitives for client {client_info}.")
            logger.debug(f"Primitives for {client_info}: {primitives}")
        else:
            logger.warning("Attempted to store primitives for an unknown or disconnected WebSocket.")

    def get_supported_primitives(self, websocket: WebSocket) -> List[str]:
        if websocket in self.connection_info:
            return self.connection_info[websocket].get('supported_primitives', [])
        else:
            logger.warning("Attempted to get primitives for an unknown or disconnected WebSocket.")
            return []

    async def send_text(self, message: str, stream_id: str):
        if stream_id not in self.active_connections:
            logger.warning(f"Attempted to send message to non-existent stream: {stream_id}")
            return
        connections_to_send = list(self.active_connections.get(stream_id, []))
        client_count = len(connections_to_send)
        if not connections_to_send:
            return
        logger.debug(f"Sending text message to {client_count} clients in stream {stream_id}")
        disconnected_clients = []
        for connection in connections_to_send:
            try:
                if connection in self.closed_connections or \
                        (connection in self.connection_info and not self.connection_info[connection].get('is_open',
                                                                                                         True)):
                    logger.debug(f"Skipping send to closed/closing connection in stream {stream_id}")
                    continue
                await connection.send_text(message)
                if connection in self.connection_info:
                    self.connection_info[connection]['messages_sent'] += 1
                    self.connection_info[connection]['bytes_sent'] += len(message.encode('utf-8'))
            except Exception as e:
                client_info = self.connection_info.get(connection, {}).get('client_info', 'unknown')
                logger.error(f"Error sending text message to client {client_info} in stream {stream_id}: {e}")
                if connection not in disconnected_clients:
                    disconnected_clients.append(connection)
        if disconnected_clients:
            logger.warning(f"Found {len(disconnected_clients)} disconnected clients during send to stream {stream_id}")
            for client in disconnected_clients:
                if client in self.connection_info:
                    self.disconnect(client, stream_id)

    async def send_binary(self, data: bytes, stream_id: str):
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
                        (connection in self.connection_info and not self.connection_info[connection].get('is_open',
                                                                                                         True)):
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
            logger.warning(
                f"Found {len(disconnected_clients)} disconnected clients during binary send to stream {stream_id}")
            for client in disconnected_clients:
                if client in self.connection_info:
                    self.disconnect(client, stream_id)

    def get_connection_count(self, stream_id: Optional[str] = None) -> int:
        if stream_id:
            return len([
                ws for ws in self.active_connections.get(stream_id, [])
                if ws not in self.closed_connections and self.connection_info.get(ws, {}).get('is_open', False)
            ])
        else:
            count = 0
            for stream_connections in self.active_connections.values():
                count += len([
                    ws for ws in stream_connections
                    if ws not in self.closed_connections and self.connection_info.get(ws, {}).get('is_open', False)
                ])
            return count

    def get_active_streams(self) -> List[str]:
        return [stream_id for stream_id, conns in self.active_connections.items() if
                self.get_connection_count(stream_id) > 0]

    def get_connection_info(self, websocket: WebSocket) -> Optional[Dict[str, Any]]:
        return self.connection_info.get(websocket)


# --- Singleton Pattern or Dependency Injection ---
def get_connection_manager() -> ConnectionManager:
    """
	Dependency function to get the centrally managed UI ConnectionManager instance
	from app.state. This instance is typically initialized during application lifespan.
	"""
    # It's crucial that `app.main` (or wherever app is defined) is fully initialized,
    # especially `app.state`, before this function is effectively used by FastAPI's DI.
    from app.main import app

    # Corrected to look for 'ui_connection_manager' as set in lifespan.py
    if hasattr(app.state, 'ui_connection_manager') and app.state.ui_connection_manager is not None:
        return app.state.ui_connection_manager
    else:
        # This case indicates a critical failure in application setup.
        # The ConnectionManager should always be initialized by the lifespan event.
        logger.error(
            "FATAL: UI ConnectionManager ('ui_connection_manager') not found in app.state "
            "or is None. This instance should be created and assigned during application startup (lifespan). "
            "Check application lifecycle and initialization order."
        )
        # Raising an error is generally preferred over creating a new, unexpected instance here,
        # as that would likely lead to further inconsistent state.
        raise RuntimeError(
            "UI ConnectionManager not available in app.state. Application did not initialize correctly."
        )

