# app/models/mcp_server_config_model.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.sql import func
from app.database import Base # From your first provided file

class MCPServerConfig(Base):
    __tablename__ = "mcp_server_configs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(String, unique=True, index=True, nullable=False)
    # This will be the connection URL for the MCP server
    url = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    # Timestamps
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # You can add other fields relevant to an MCP connection,
    # e.g., auth_type, credentials_secret_key, specific_mcp_protocol_version, etc.

    def __repr__(self):
        return f"<MCPServerConfig(id={self.id}, name='{self.name}', url='{self.url}', is_active={self.is_active})>"