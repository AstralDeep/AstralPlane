# app/models/mcp_server_config_model.py
from sqlalchemy import Column, String, Boolean, DateTime, func # Removed Integer
from app.database import Base # Assuming this is your declarative base

class MCPServerConfig(Base):
    __tablename__ = "mcp_server_configs"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    url = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False) # Added nullable=False for clarity

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # You can add other fields relevant to an MCP connection,
    # e.g., auth_type, credentials_secret_key, specific_mcp_protocol_version, etc.

    def __repr__(self):
        return f"<MCPServerConfig(id='{self.id}', name='{self.name}', url='{self.url}', is_active={self.is_active})>"