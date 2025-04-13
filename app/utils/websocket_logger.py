"""
Enhanced WebSocket logging utility for FastAPI backend.
Add this file as app/utils/websocket_logger.py
"""
import logging
import json
from typing import Any, Dict, Optional
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketLogger:
    """Utility class for comprehensive WebSocket logging"""

    @staticmethod
    def format_message(message: str, max_length: int = 1000) -> str:
        """Format a message for logging, truncating if necessary"""
        if not message:
            return "<empty>"

        if len(message) > max_length:
            return f"{message[:max_length]}... (truncated, total length: {len(message)})"
        return message

    @staticmethod
    def get_client_info(websocket: WebSocket) -> str:
        """Extract client information from a WebSocket connection"""
        try:
            client_host = websocket.client.host if hasattr(websocket, 'client') and hasattr(websocket.client,
                                                                                            'host') else "unknown"
            client_port = websocket.client.port if hasattr(websocket, 'client') and hasattr(websocket.client,
                                                                                            'port') else "unknown"
            return f"{client_host}:{client_port}"
        except Exception:
            return "unknown"

    @staticmethod
    def log_connection(websocket: WebSocket, stream_id: str, user_id: Optional[str] = None):
        """Log a new WebSocket connection"""
        client_info = WebSocketLogger.get_client_info(websocket)
        if user_id:
            logger.info(f"WS CONNECT: User {user_id} from {client_info} connected to stream {stream_id}")
        else:
            logger.info(f"WS CONNECT: Client from {client_info} connected to stream {stream_id}")

    @staticmethod
    def log_disconnection(websocket: WebSocket, stream_id: str, reason: Optional[str] = None):
        """Log a WebSocket disconnection"""
        client_info = WebSocketLogger.get_client_info(websocket)
        if reason:
            logger.info(
                f"WS DISCONNECT: Client from {client_info} disconnected from stream {stream_id} - Reason: {reason}")
        else:
            logger.info(f"WS DISCONNECT: Client from {client_info} disconnected from stream {stream_id}")

    @staticmethod
    def log_text_received(websocket: WebSocket, stream_id: str, message: str):
        """Log a text message received from a client"""
        client_info = WebSocketLogger.get_client_info(websocket)
        formatted_message = WebSocketLogger.format_message(message)

        # Try to parse JSON for better logging
        try:
            json_data = json.loads(message)
            msg_type = json_data.get('type', 'unknown')

            # Log the type and a summary
            logger.info(f"WS RECEIVED: [{stream_id}] Type: {msg_type} from {client_info}")
            # Log the full content at debug level
            logger.debug(f"WS RECEIVED DETAIL: [{stream_id}] {formatted_message}")
        except json.JSONDecodeError:
            # Not JSON, log as plain text
            logger.info(f"WS RECEIVED: [{stream_id}] Text message from {client_info}")
            logger.debug(f"WS RECEIVED DETAIL: [{stream_id}] {formatted_message}")

    @staticmethod
    def log_binary_received(websocket: WebSocket, stream_id: str, data: bytes):
        """Log binary data received from a client"""
        client_info = WebSocketLogger.get_client_info(websocket)
        logger.info(f"WS RECEIVED: [{stream_id}] Binary data ({len(data)} bytes) from {client_info}")

    @staticmethod
    def log_text_sent(stream_id: str, message: str, client_count: int = 1):
        """Log a text message sent to clients"""
        recipient_info = f"{client_count} client(s)" if client_count > 1 else "client"

        # Try to parse JSON for better logging
        try:
            json_data = json.loads(message)
            msg_type = json_data.get('type', 'unknown')

            # Log the type and a summary
            logger.info(f"WS SENT: [{stream_id}] Type: {msg_type} to {recipient_info}")
            # Log the full content at debug level
            formatted_message = WebSocketLogger.format_message(message)
            logger.debug(f"WS SENT DETAIL: [{stream_id}] {formatted_message}")
        except json.JSONDecodeError:
            # Not JSON, log as plain text
            formatted_message = WebSocketLogger.format_message(message)
            logger.info(f"WS SENT: [{stream_id}] Text message to {recipient_info}")
            logger.debug(f"WS SENT DETAIL: [{stream_id}] {formatted_message}")

    @staticmethod
    def log_binary_sent(stream_id: str, data_length: int, client_count: int = 1):
        """Log binary data sent to clients"""
        recipient_info = f"{client_count} client(s)" if client_count > 1 else "client"
        logger.info(f"WS SENT: [{stream_id}] Binary data ({data_length} bytes) to {recipient_info}")

    @staticmethod
    def log_error(stream_id: str, message: str, exception: Optional[Exception] = None):
        """Log a WebSocket error"""
        if exception:
            logger.error(f"WS ERROR: [{stream_id}] {message}", exc_info=exception)
        else:
            logger.error(f"WS ERROR: [{stream_id}] {message}")