# app/api/projects.py
# MODIFIED: Filters server list based on proactive UI readiness check

from fastapi import APIRouter, Depends, HTTPException, Path, Query
import logging
from typing import List, Optional, Dict, Any, Set # Added Set
from datetime import datetime
import asyncio

from starlette import status

# Import necessary schemas and services
from ..models.schemas import Project, ProjectCreate, ErrorResponse, SuccessResponse, ProjectsResponse, ProjectLayout
from ..services.auth_service import get_current_user, get_user_id

# --- MCP Integration Imports ---
# Import manager to check pre-established connection state
from ..services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager
from ..config import settings # Import settings to get configured MCP_SERVERS

router = APIRouter()
logger = logging.getLogger(__name__)

# --- GET / Endpoint (Lists Configured MCP Servers - Proactive Filter) ---
@router.get("/", response_model=ProjectsResponse)
async def get_projects( # Needs to be async to call async manager methods
        user_id: str = Depends(get_user_id),
        skip: int = Query(0, ge=0, description="Number of projects/servers to skip"),
        limit: int = Query(10, ge=1, description="Maximum number of projects/servers to return"),
        # Inject MCPConnectionManager to check status
        mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    """
    Get available MCP servers that are connected and UI-ready based on proactive checks.
    """
    logger.info(f"Generating UI-ready project/server list for user '{user_id}' based on stored state.")
    now = datetime.now()
    valid_ui_projects: List[Project] = []

    # --- Iterate through configured servers and check their stored readiness ---
    configured_server_ids = list(mcp_conn_manager.server_configs.keys())

    # Check readiness concurrently (optional optimization)
    readiness_checks = await asyncio.gather(*(mcp_conn_manager.is_server_ui_ready(sid) for sid in configured_server_ids))

    for i, server_id in enumerate(configured_server_ids):
        is_ready = readiness_checks[i]
        server_config = mcp_conn_manager.server_configs.get(server_id) # Get config from manager

        if is_ready and server_config:
            logger.debug(f"Server '{server_id}' is UI-ready. Including in list.")
            # --- Access Control (Placeholder) ---
            # Add logic here if access depends on the user_id
            user_is_member = True # Assume access for now

            if user_is_member:
                project = Project(
                    id=server_id,
                    name=server_config.get("name", f"Server {server_id}"),
                    description=server_config.get("description", "Configured MCP Server"),
                    owner_id="system", created_at=now, updated_at=now,
                    members=[{"user_id": user_id, "role": "member"}],
                    views={}, layout=ProjectLayout(),
                    project_type=f"mcp_{server_config.get('transport', 'unknown')}"
                )
                valid_ui_projects.append(project)
        else:
             # Log why it's not included (using details might require another async call, keep simple for now)
             logger.info(f"Filtering out server '{server_id}': Not UI-ready based on stored state.")


    # --- Determine default and apply pagination ---
    current_project: Optional[Project] = valid_ui_projects[0] if valid_ui_projects else None

    start = skip
    end = skip + limit
    paginated_projects = valid_ui_projects[start:end]

    current_project_id_log = current_project.id if current_project else 'None'
    logger.info(f"Returning {len(paginated_projects)} UI-ready servers (out of {len(valid_ui_projects)} total accessible & UI-ready). Current default set to: {current_project_id_log}")

    return ProjectsResponse(
        projects=paginated_projects,
        current_project=current_project # Can be None
    )


# --- GET /{project_id} Endpoint ---
# (Keep as is - provides config details, not dependent on UI readiness)
@router.get("/{project_id}", response_model=Project)
async def get_project(
    project_id: str = Path(..., description="The ID of the configured MCP server"),
    user_id: str = Depends(get_user_id)
):
    """Get details for a specific configured MCP server, represented as a Project."""
    logger.info(f"Attempting to get details for project/server ID: {project_id} for user '{user_id}'.")
    now = datetime.now()
    server_config = next((s for s in settings.MCP_SERVERS if s.get("id") == project_id), None)
    if not server_config:
        logger.warning(f"Project/Server ID '{project_id}' not found in configuration.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project/Server not found")
    user_has_access = True # Placeholder
    if not user_has_access: raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")
    project = Project(
        id=server_config["id"], name=server_config.get("name", f"Server {project_id}"),
        description=server_config.get("description", "Configured MCP Server"), owner_id="system",
        created_at=now, updated_at=now, members=[{"user_id": user_id, "role": "member"}],
        views={}, layout=ProjectLayout(), project_type=f"mcp_{server_config.get('transport', 'unknown')}"
    )
    logger.info(f"Returning details for configured server '{project_id}'.")
    return project


# --- Other Endpoints (POST, PUT, DELETE, /members) ---
# (Keep as 501 Not Implemented)
@router.post("/", status_code=status.HTTP_501_NOT_IMPLEMENTED, include_in_schema=False)
async def create_project(project: ProjectCreate, user_id: str = Depends(get_user_id)):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Cannot create projects/servers via this API.")
@router.put("/{project_id}", status_code=status.HTTP_501_NOT_IMPLEMENTED, include_in_schema=False)
async def update_project(project_update: Dict[str, Any], project_id: str = Path(...), user_id: str = Depends(get_user_id)):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Cannot update project/server configurations via this API.")
@router.delete("/{project_id}", status_code=status.HTTP_501_NOT_IMPLEMENTED, include_in_schema=False)
async def delete_project(project_id: str = Path(...), user_id: str = Depends(get_user_id)):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Cannot delete project/server configurations via this API.")
@router.post("/{project_id}/members", status_code=status.HTTP_501_NOT_IMPLEMENTED, include_in_schema=False)
async def add_project_member(user_data: Dict[str, str], project_id: str = Path(...), user_id: str = Depends(get_user_id)):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Member management not applicable to configured servers.")
@router.delete("/{project_id}/members/{member_id}", status_code=status.HTTP_501_NOT_IMPLEMENTED, include_in_schema=False)
async def remove_project_member(project_id: str = Path(...), member_id: str = Path(...), user_id: str = Depends(get_user_id)):
    raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Member management not applicable to configured servers.")

