# Add this file as app/utils/logging_config.py

import logging
import sys
import os
from logging.handlers import RotatingFileHandler


def configure_logging(log_level=logging.INFO, log_to_file=True, log_dir="logs"):
    """
    Configure application logging with enhanced formatting

    Args:
        log_level: Logging level (e.g., logging.DEBUG, logging.INFO)
        log_to_file: Whether to log to files in addition to console
        log_dir: Directory for log files
    """
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Create the logs directory if it doesn't exist and log_to_file is True
    if log_to_file and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    root_logger.addHandler(console_handler)

    # Add file handlers if enabled
    if log_to_file:
        # General log file (all levels)
        general_handler = RotatingFileHandler(
            os.path.join(log_dir, 'app.log'),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        general_handler.setFormatter(formatter)
        general_handler.setLevel(log_level)
        root_logger.addHandler(general_handler)

        # WebSocket specific log file
        websocket_handler = RotatingFileHandler(
            os.path.join(log_dir, 'websocket.log'),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        websocket_handler.setFormatter(formatter)
        websocket_handler.setLevel(log_level)

        # Only log WebSocket related messages to this file
        class WebSocketFilter(logging.Filter):
            def filter(self, record):
                return record.getMessage().startswith('WS ')

        websocket_handler.addFilter(WebSocketFilter())
        root_logger.addHandler(websocket_handler)

        # Error log file (ERROR and above)
        error_handler = RotatingFileHandler(
            os.path.join(log_dir, 'error.log'),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)
        root_logger.addHandler(error_handler)

    # Set specific logger levels
    logging.getLogger('uvicorn').setLevel(logging.WARNING)
    logging.getLogger('websockets').setLevel(logging.WARNING)

    # Create a WebSocket logger with its own configuration
    ws_logger = logging.getLogger('websocket')
    ws_logger.setLevel(log_level)
    ws_logger.propagate = False  # Don't propagate to root logger

    # Add handlers to WebSocket logger
    ws_console_handler = logging.StreamHandler(sys.stdout)
    ws_console_handler.setFormatter(formatter)
    ws_logger.addHandler(ws_console_handler)

    if log_to_file:
        ws_file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'websocket_detail.log'),
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        ws_file_handler.setFormatter(formatter)
        ws_logger.addHandler(ws_file_handler)

    logging.info(
        f"Logging configured: level={logging.getLevelName(log_level)}, log_to_file={log_to_file}, log_dir='{log_dir}'")