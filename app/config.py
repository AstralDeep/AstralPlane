# app/config.py
import os
import json
from typing import List, Dict, Any, Union, Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """Application settings"""

    # ... (Keep existing settings like APP_NAME, DEBUG, HOST, PORT, CORS, JWT, etc.) ...
    APP_NAME: str = "AI Agent Interface Backend"
    APP_ENV: str = os.getenv("APP_ENV", "development")
    DEBUG: bool = os.getenv("DEBUG", "True").lower() in ("true", "1", "t")
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ]
    SECRET_KEY: str = os.getenv("SECRET_KEY", "supersecretkey")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24
    WS_PING_INTERVAL: int = int(os.getenv("WS_PING_INTERVAL", "20"))

    '''
        {
            "id": "mcp_mock_chatviewbasic",  # Keep existing example if needed
            "name": "Mock MCP Chat Server",
            "description": "Connects to the standalone mcp_server.py via SSE.",
            "transport": "sse",
            "url": os.getenv("MCP_SSE_URL", "http://127.0.0.1:8123/sse")
        },
        {
            "id": "mcp_async_demo",
            "name": "Async Demo Server (FastMCP)",
            "description": "Demonstrates various asynchronous notifications.",
            "transport": "sse",
            "url": os.getenv("MCP_ASYNC_DEMO_URL", "http://127.0.0.1:8124/sse")  # Use port 8124
        },
        {
            "id": "mcp_mock_chatviewreasoning",  # Keep existing example if needed
            "name": "Mock MCP Reasoning Chat Server",
            "description": "Connects to the standalone mcp_server.py via SSE.",
            "transport": "sse",
            "url": os.getenv("MCP_SSE_URL", "http://127.0.0.1:8125/sse")
        },
        {
            "id": "mcp_chatviewreasoning",  # Keep existing example if needed
            "name": "MCP Reasoning Chat Server",
            "description": "Connects to the standalone mcp_server.py via SSE.",
            "transport": "sse",
            "url": os.getenv("MCP_SSE_URL", "http://127.0.0.1:8126/sse")
        }   
    '''

    # --- MCP Server Configuration ---
    MCP_SERVERS: List[Dict[str, Any]] = [

        {
            "id": "mcp_chatviewreasoning",  # Keep existing example if needed
            "name": "MCP Reasoning Chat Server",
            "description": "Connects to the standalone mcp_server.py via SSE.",
            "transport": "sse",
            "url": os.getenv("MCP_SSE_URL", "http://127.0.0.1:8126/sse")
        }

    ]

    # ... (Keep existing Database, Dataplane, Testing, Admin settings) ...
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./app.db")
    DATABASE_ECHO: bool = os.getenv("DATABASE_ECHO", "False").lower() in ("true", "1", "t")
    DATAPLANE_CONNECTION_TIMEOUT: int = int(os.getenv("DATAPLANE_CONNECTION_TIMEOUT", "30"))
    TESTING_MODE: bool = os.getenv("TESTING_MODE", "False").lower() in ("true", "1", "t")
    ADMIN_PASSWORD: Optional[str] = os.getenv("ADMIN_PASSWORD", None if not DEBUG else "admin")


    class Config:
        env_file = ".env"
        case_sensitive = True

# Create settings instance
settings = Settings()

# Log the MCP servers being used
logger.info(f"Configured External MCP Servers: {settings.MCP_SERVERS}")

# Validate essential settings
if not settings.SECRET_KEY or settings.SECRET_KEY == "supersecretkey":
    logger.warning("Security warning: SECRET_KEY is not set or is using the default value.")

