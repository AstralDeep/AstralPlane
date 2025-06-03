import logging
import os
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load environment variables from .env_pg file
load_dotenv()

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
	"""Application settings"""

	APP_NAME: str = "AI Agent Interface Backend"
	APP_DESCRIPTION: str = "AI Agents using MCP!"
	APP_VERSION: str = "0.0.1"
	APP_ENV: str = os.getenv("APP_ENV", "development")
	DEBUG: bool = os.getenv("DEBUG", "True").lower() in ("true", "1", "t")
	HOST: str = os.getenv("HOST", "0.0.0.0")
	PORT: int = int(os.getenv("PORT", "8000"))
	CORS_ORIGINS: List[str] = [
		"http://localhost:3000",
		"http://localhost:8001",
		"http://localhost:5173",
		"http://127.0.0.1:3000",
		"http://127.0.0.1:8001",
		"http://127.0.0.1:5173",
		"https://sandbox.ai.uky.edu",
	]
	SECRET_KEY: str = os.getenv("SECRET_KEY", "supersecretkey")
	ALGORITHM: str = "HS256"
	ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24
	WS_PING_INTERVAL: int = int(os.getenv("WS_PING_INTERVAL", "20"))
	MCP_CALL_TOOL_TIMEOUT: int = 300  # 5 minutes


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

# Validate essential settings
if not settings.SECRET_KEY or settings.SECRET_KEY == "supersecretkey":
	logger.warning("Security warning: SECRET_KEY is not set or is using the default value.")
