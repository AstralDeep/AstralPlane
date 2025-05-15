# app/models/schemas.py
# Complete File - Includes Optional current_project fix, New Tool Schema Models, and Async Notification Schemas

from typing import Dict, List, Optional, Any, Union, Literal # Added Literal
from pydantic import BaseModel as PydanticBaseModel, Field, field_validator, model_validator
from datetime import datetime

# --- Base Model ---
class BaseModel(PydanticBaseModel):
    """Base model with common functionality for all models."""
    class Config:
        from_attributes = True
        populate_by_name = True
        arbitrary_types_allowed = True


# --- Base response and error models ---
class ErrorResponse(BaseModel):
    """Error response model"""
    detail: str

class SuccessResponse(BaseModel):
    """Success response model"""
    status: str = "success"
    message: Optional[str] = None


# --- Authentication models ---
class Token(BaseModel):
    """Base token response model"""
    access_token: str
    token_type: str = "bearer"
    expires_at: datetime

class TokenData(BaseModel):
    """Token data model for decoded JWT payload"""
    user_id: Optional[str] = None
    exp: Optional[datetime] = None

class UserCredentials(BaseModel):
    """User login credentials"""
    username: str
    password: str

class UserProfile(BaseModel):
    """User profile model"""
    id: str # User ID, can remain string
    username: str
    global_role: str
    preference_id: Optional[str] = None
    profile_tags: List[str] = Field(default_factory=list)

# --- Project, View, and Layout Models ---
class ViewConfig(BaseModel):
    """Individual view configuration within a project."""
    id: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    content: Optional[Any] = None
    position: Optional[Dict[str, int]] = None
    size: Optional[Dict[str, int]] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[str] = None
    updateBinding: Optional[str] = None
    actionId: Optional[str] = None


class ProjectLayout(BaseModel):
    """Layout information/hints stored for a project"""
    layout_type: str = "interpreted"
    layout_data: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    updated_by: Optional[str] = None


class Project(BaseModel):
    """
    Project model representing a configured MCP server connection.
    Its 'id' will now correspond to the string ID of MCPServerConfig.
    """
    id: str # This should align with the new string ID of MCPServerConfig
    name: str # This will be the human-readable name like "Mock MCP Chat Server"
    description: Optional[str] = None
    owner_id: str # Should be 'system' for configured servers
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    members: List[Dict[str, Any]] = Field(default_factory=list)
    views: Dict[str, ViewConfig] = Field(default_factory=dict)
    layout: ProjectLayout = Field(default_factory=ProjectLayout)
    project_type: str = "generic"


# --- ProjectsResponse (MODIFIED) ---
class ProjectsResponse(BaseModel):
    """Projects list response with current project"""
    projects: List[Project]
    current_project: Optional[Project] = None


class ProjectCreate(BaseModel):
    """Project creation model (Not used for configured servers)"""
    name: str
    description: Optional[str] = None


# --- LOGIN RESPONSE MODEL (MODIFIED) ---
class LoginResponse(Token):
    """Response model for successful login"""
    user: UserProfile
    current_project: Optional[Project] = None


# --- Tool models (Keep as is) ---
class Tool(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    type: str
    stream_key: str
    stream_id: str # This typically refers to the MCP stream, not the server config ID
    project_id: Optional[str] = None # If this links to Project.id, it will be the string ID

class ToolCreate(BaseModel):
    name: str
    description: Optional[str] = None
    type: str
    config: Optional[Dict[str, Any]] = None


# --- Layout models (Keep as is, potentially legacy) ---
class Layout(BaseModel):
    id: str
    user_id: str
    project_id: str # If this links to Project.id, it will be the string ID
    views: Dict[str, Any] = Field(default_factory=dict)
    layout: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class LayoutUpdate(BaseModel):
    views: Optional[Dict[str, Any]] = None
    layout: Optional[Dict[str, Any]] = None


# --- HIERARCHICAL UI & WEBSOCKET MODELS ---
class StackLayoutConfig(BaseModel):
    direction: Optional[Literal['vertical', 'horizontal']] = 'vertical'
    gap: Optional[str] = None
    padding: Optional[str] = None
    justify_content: Optional[str] = None
    align_items: Optional[str] = None
    border: Optional[bool] = False
    style: Optional[Dict[str, str]] = None
    gridTemplateAreas: Optional[str] = None
    gridTemplateColumns: Optional[str] = None
    gridTemplateRows: Optional[str] = None
    className: Optional[str] = None


class UIElement(BaseModel):
    id: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    children: Optional[List['UIElement']] = None
    updateBinding: Optional[str] = None
    actionId: Optional[str] = None
    content: Optional[Any] = None
    gridArea: Optional[str] = None

UIElement.model_rebuild()


class InitialUIStatePayload(BaseModel):
    rootElement: UIElement

class InitialUIStateMessage(BaseModel):
    type: Literal['initial_ui_state'] = 'initial_ui_state'
    payload: InitialUIStatePayload

class PrimitiveContentUpdatePayload(BaseModel):
    targetBinding: Optional[str] = None
    targetId: Optional[str] = None
    content: Any
    updateType: Literal['append', 'replace'] = 'replace'
    sectionId: Optional[str] = None

    @model_validator(mode='before')
    @classmethod
    def check_target_exclusive_v2(cls, data: Any) -> Any:
        if isinstance(data, dict):
            target_binding = data.get('targetBinding')
            target_id = data.get('targetId')
            if target_binding is not None and target_id is not None:
                raise ValueError('Only one of targetBinding or targetId can be specified')
            if target_binding is None and target_id is None:
                raise ValueError('One of targetBinding or targetId must be specified')
        return data

class PrimitiveContentUpdateMessage(BaseModel):
    type: Literal['primitive_content_update'] = 'primitive_content_update'
    payload: PrimitiveContentUpdatePayload

class UIActionPayload(BaseModel):
    actionId: str
    sourceElementId: str
    arguments: Dict[str, Any] = Field(default_factory=dict)

class UIActionMessage(BaseModel):
    type: Literal['ui_action'] = 'ui_action'
    payload: UIActionPayload

# --- NEW TOOL SCHEMA MODELS ---
class ToolSchemaInfo(BaseModel):
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = Field(None, alias='inputSchema')
    output_schema: Optional[Dict[str, Any]] = Field(None, alias='outputSchema')

class ToolSchemasPayload(BaseModel):
    server_id: str # This will now be the string ID, e.g., "mcp_mock_chatviewbasic"
    tools: Dict[str, ToolSchemaInfo]

class ToolSchemasMessage(BaseModel):
    type: Literal['tool_schemas'] = 'tool_schemas'
    payload: ToolSchemasPayload


# --- ASYNC NOTIFICATION SCHEMAS ---
class MCPProgressPayload(BaseModel):
    server_id: str # This will now be the string ID
    token: Optional[str] = None
    percentage: Optional[float] = None
    message: Optional[str] = None
    title: Optional[str] = None

class MCPProgressMessage(BaseModel):
    type: Literal['mcp_progress'] = 'mcp_progress'
    payload: MCPProgressPayload

class MCPNotificationPayload(BaseModel):
    server_id: str # This will now be the string ID
    notification_type: Literal[
        "ResourceUpdated", "ResourceListChanged", "ToolListChanged",
        "PromptListChanged", "CancelledByServer"
    ]
    details: Optional[Dict[str, Any]] = None

class MCPNotificationMessage(BaseModel):
    type: Literal['mcp_notification'] = 'mcp_notification'
    payload: MCPNotificationPayload

class RootsChangedPayload(BaseModel):
    server_id: str # This will now be the string ID
    roots: List[Dict[str, Any]]

class RootsChangedMessage(BaseModel):
    type: Literal['notify_roots_changed'] = 'notify_roots_changed'
    payload: RootsChangedPayload

class CancelRequestPayload(BaseModel):
    server_id: str # This will now be the string ID
    requestId: str

class CancelRequestMessage(BaseModel):
    type: Literal['notify_cancelled'] = 'notify_cancelled'
    payload: CancelRequestPayload

class MCPLogEntry(BaseModel):
    level: Literal["error", "warning", "info", "debug", "log"]
    message: str
    timestamp: Optional[datetime] = Field(default_factory=datetime.now)

# --- MCP Server Configuration Schemas (MODIFIED) ---
class MCPServerConfigBase(BaseModel):
    """Base schema for MCP Server Configuration."""
    # The 'id' is now a user-defined string, like a slug or unique key.
    id: str = Field(..., description="User-defined unique string identifier for the server (e.g., 'mcp_mock_chatviewbasic').")
    name: str = Field(..., description="Human-readable name for the server (e.g., 'Mock MCP Chat Server').")
    url: str
    description: Optional[str] = None
    is_active: bool = True

class MCPServerConfigCreate(MCPServerConfigBase):
    """Schema for creating a new MCP Server Configuration."""
    # Inherits id, name, url, description, is_active from MCPServerConfigBase.
    # All these fields (including 'id') must now be provided during creation.
    pass

class MCPServerConfigUpdate(BaseModel):
    """Schema for updating an existing MCP Server Configuration."""
    # ID is typically not updatable as it's the primary key.
    name: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None
    # You cannot update the 'id' after creation using this schema.

class MCPServerConfigResponse(MCPServerConfigBase):
    """Schema for returning MCP Server Configuration from the API."""
    # 'id' is inherited from MCPServerConfigBase and is now str.
    # 'name', 'url', 'description', 'is_active' are also inherited.
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True # Ensures compatibility with SQLAlchemy models