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
    id: str
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
    """Project model representing a configured MCP server connection"""
    id: str
    name: str
    description: Optional[str] = None
    owner_id: str # Should be 'system' for configured servers
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    members: List[Dict[str, Any]] = Field(default_factory=list)
    # views and layout are less relevant here as UI comes from server
    views: Dict[str, ViewConfig] = Field(default_factory=dict)
    layout: ProjectLayout = Field(default_factory=ProjectLayout)
    project_type: str = "generic"


# --- ProjectsResponse (MODIFIED) ---
class ProjectsResponse(BaseModel):
    """Projects list response with current project"""
    projects: List[Project]
    # --- Changed to Optional[Project] ---
    current_project: Optional[Project] = None
    # --- End Change ---


class ProjectCreate(BaseModel):
    """Project creation model (Not used for configured servers)"""
    name: str
    description: Optional[str] = None


# --- LOGIN RESPONSE MODEL (MODIFIED) ---
class LoginResponse(Token):
    """Response model for successful login"""
    user: UserProfile
    # --- Also make current_project optional here ---
    current_project: Optional[Project] = None


# --- Tool models (Keep as is) ---
class Tool(BaseModel):
    """Tool model (potentially less used if UI is server-driven)"""
    id: str
    name: str
    description: Optional[str] = None
    type: str
    stream_key: str
    stream_id: str
    project_id: Optional[str] = None
    config: Optional[Dict[str, Any]] = None

class ToolCreate(BaseModel):
    """Tool creation model"""
    name: str
    description: Optional[str] = None
    type: str
    config: Optional[Dict[str, Any]] = None


# --- Layout models (Keep as is, potentially legacy) ---
class Layout(BaseModel):
    """Layout model for UI configuration (potentially legacy)"""
    id: str
    user_id: str
    project_id: str
    views: Dict[str, Any] = Field(default_factory=dict)
    layout: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class LayoutUpdate(BaseModel):
    """Layout update model (potentially legacy)"""
    views: Optional[Dict[str, Any]] = None
    layout: Optional[Dict[str, Any]] = None


# --- HIERARCHICAL UI & WEBSOCKET MODELS ---
class StackLayoutConfig(BaseModel):
    """Configuration for a StackLayout UI element."""
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
    """Represents an element in the UI hierarchy sent to the frontend."""
    id: str
    type: str # Matches server-provided type, e.g., 'StackLayout', 'ChatViewBasic'
    config: Dict[str, Any] = Field(default_factory=dict)
    children: Optional[List['UIElement']] = None
    updateBinding: Optional[str] = None # For receiving updates
    actionId: Optional[str] = None # For triggering actions
    content: Optional[Any] = None # Initial content (less common if server-driven)
    gridArea: Optional[str] = None


# --- Pydantic v2: Recursive models need explicit update_forward_refs ---
# Call this at the end of the file or after all relevant models are defined
UIElement.model_rebuild()


class InitialUIStatePayload(BaseModel):
    """Payload for the initial UI state message."""
    rootElement: UIElement

class InitialUIStateMessage(BaseModel):
    """Message sent on WebSocket connect defining the UI."""
    type: Literal['initial_ui_state'] = 'initial_ui_state'
    payload: InitialUIStatePayload

class PrimitiveContentUpdatePayload(BaseModel):
    """Payload for updating content within a UI element."""
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
    """Message to update content in the UI."""
    type: Literal['primitive_content_update'] = 'primitive_content_update'
    payload: PrimitiveContentUpdatePayload

class UIActionPayload(BaseModel):
    """Payload for user actions triggered from the UI."""
    actionId: str
    sourceElementId: str
    # --- Modified to accept general arguments ---
    arguments: Dict[str, Any] = Field(default_factory=dict)
    # --- Remove potentially limiting 'payload' field if arguments is used instead ---
    # payload: Dict[str, Any] = Field(default_factory=dict)

class UIActionMessage(BaseModel):
    """Message sent from frontend when user interacts."""
    type: Literal['ui_action'] = 'ui_action'
    payload: UIActionPayload

# --- NEW TOOL SCHEMA MODELS ---
class ToolSchemaInfo(BaseModel):
    """Schema information for a single tool."""
    name: str
    description: Optional[str] = None
    input_schema: Optional[Dict[str, Any]] = Field(None, alias='inputSchema')
    output_schema: Optional[Dict[str, Any]] = Field(None, alias='outputSchema')

class ToolSchemasPayload(BaseModel):
    """Payload containing tool schemas for a specific server."""
    server_id: str # Match frontend expectation (e.g., 'sse_server_1')
    tools: Dict[str, ToolSchemaInfo] # Tool name -> Schema info

class ToolSchemasMessage(BaseModel):
    """WebSocket message to send discovered tool schemas to the frontend."""
    type: Literal['tool_schemas'] = 'tool_schemas'
    payload: ToolSchemasPayload


# --- ASYNC NOTIFICATION SCHEMAS ---

# --- Add models for Server -> Client Notification Relaying ---
class MCPProgressPayload(BaseModel):
    """Payload for progress updates relayed from MCP server."""
    server_id: str
    token: Optional[str] = Field(None, description="Correlating progress token")
    percentage: Optional[float] = Field(None, ge=0.0, le=1.0, description="Progress percentage (0.0 to 1.0)")
    message: Optional[str] = Field(None, description="Progress message")
    title: Optional[str] = Field(None, description="Optional title for the progress")


class MCPProgressMessage(BaseModel):
    """WebSocket message to send MCP progress updates to the frontend."""
    type: Literal['mcp_progress'] = 'mcp_progress'
    payload: MCPProgressPayload


class MCPNotificationPayload(BaseModel):
    """Generic payload for relaying simple MCP notifications."""
    server_id: str
    notification_type: Literal[
        "ResourceUpdated",
        "ResourceListChanged",
        "ToolListChanged",
        "PromptListChanged",
        "CancelledByServer"
        # Add other specific MCP notification types you want to relay
    ] = Field(..., description="The type of MCP notification received")
    details: Optional[Dict[str, Any]] = Field(None, description="Optional details specific to the notification type (e.g., resource ID)")


class MCPNotificationMessage(BaseModel):
    """WebSocket message to inform frontend about MCP state changes."""
    type: Literal['mcp_notification'] = 'mcp_notification'
    payload: MCPNotificationPayload


# --- Add models for Client -> Server Notification Triggering ---
class RootsChangedPayload(BaseModel):
    """Payload sent by frontend when its roots change."""
    server_id: str = Field(..., description="Target MCP server ID")
    # Define the structure based on MCP spec - example below
    roots: List[Dict[str, Any]] = Field(..., description="List of root objects (e.g., {'uri': 'file:///path/to/workspace'})")


class RootsChangedMessage(BaseModel):
    """WebSocket message from frontend to notify backend of root changes."""
    type: Literal['notify_roots_changed'] = 'notify_roots_changed'
    payload: RootsChangedPayload


class CancelRequestPayload(BaseModel):
    """Payload sent by frontend to request cancellation."""
    server_id: str = Field(..., description="Target MCP server ID")
    requestId: str = Field(..., description="The ID of the MCP request to cancel")


class CancelRequestMessage(BaseModel):
    """WebSocket message from frontend to request cancellation."""
    type: Literal['notify_cancelled'] = 'notify_cancelled'
    payload: CancelRequestPayload

# --- Optional: Define specific structures for log messages if needed ---
class MCPLogEntry(BaseModel):
    """Structure for a single log entry relayed to the frontend."""
    level: Literal["error", "warning", "info", "debug", "log"] # Match MCP log levels
    message: str
    timestamp: Optional[datetime] = Field(default_factory=datetime.now) # Add timestamp during relay


# Final rebuild if any forward references were added/affected by new models
# UIElement.model_rebuild() # Already called earlier, likely not needed again unless complex refs added