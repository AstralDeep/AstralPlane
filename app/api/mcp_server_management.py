# app/api/mcp_server_management.py
import logging
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Path, Query
from sqlalchemy.orm import Session

from app.database import get_db_session
from app.models.schemas import MCPServerConfigCreate, MCPServerConfigResponse, MCPServerConfigUpdate
from app.services import mcp_config_crud_service
from app.services.mcp_connection_manager import MCPConnectionManager
from app.services.mcp_connection_manager import get_mcp_connection_manager

router = APIRouter()

logger = logging.getLogger(__name__)

# Example curl command for the endpoint:
"""
curl -X POST http://localhost:8000/api/mcp-servers/ \
-H "Content-Type: application/json" \
-d '{
  "id": "mcp_mock_chatviewbasic",
  "name": "Mock MCP Chat Server",
  "url": "http://127.0.0.1:8123/sse",
  "description": "Mock MCP Chat Server - Connects via SSE.",
  "is_active": true
}'
"""


@router.post("/", response_model=MCPServerConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_new_mcp_server_config(
        config_payload: MCPServerConfigCreate,
        db: Session = Depends(get_db_session),
        mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    # Now you use config_payload to access the data
    db_config_by_name = mcp_config_crud_service.get_mcp_server_config_by_name(db, name=config_payload.name)
    if db_config_by_name:
        raise HTTPException(status_code=400, detail=f"MCP Server with name '{config_payload.name}' already exists.")

    db_config_by_url = mcp_config_crud_service.get_mcp_server_config_by_url(db, url=config_payload.url)
    if db_config_by_url:
        raise HTTPException(status_code=400, detail=f"MCP Server with URL '{config_payload.url}' already exists.")

    new_config_db_model = mcp_config_crud_service.create_mcp_server_config(db=db, config_in=config_payload)

    # Notify the manager to add and potentially connect to the new server
    await mcp_manager.add_server_from_config(new_config_db_model)

    return new_config_db_model  # FastAPI will convert this SQLAlchemy model to MCPServerConfigResponse

# Example curl command for the endpoint:
"""
curl -X GET "http://localhost:8000/api/mcp-servers/?skip=0&limit=5"
- OR -
curl -X GET "http://localhost:8000/api/mcp-servers/?only_active=true"
"""
@router.get("/", response_model=List[MCPServerConfigResponse], status_code=status.HTTP_200_OK)
async def get_all_mcp_server_configs(
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return"),
    only_active: Optional[bool] = Query(None, description="Filter by active status (true or false)"),
    db: Session = Depends(get_db_session)
):
    """
    Retrieves a list of all MCP server configurations, with optional pagination and active status filtering.
    """
    logger.info(f"Fetching all MCP server configurations with skip={skip}, limit={limit}, only_active={only_active}")
    configs = mcp_config_crud_service.get_mcp_server_configs(
        db=db, skip=skip, limit=limit, only_active=only_active
    )
    return configs


# Example curl command for the endpoint:
"""
curl -X PATCH http://localhost:8000/api/mcp-servers/mcp_mock_chatviewbasic \
-H "Content-Type: application/json" \
-d '{
  "name": "Mock MCP Chat Server (Updated Name)",
  "description": "This server has an updated description and is now inactive.",
  "url": "http://127.0.0.1:8124/sse",
  "is_active": false
}'
"""
@router.patch("/id/{server_id}", response_model=MCPServerConfigResponse, status_code=status.HTTP_200_OK)
async def update_mcp_server_config_by_id_endpoint(
    update_payload: MCPServerConfigUpdate,
    server_id: int = Path(..., description="The ID of the MCP server configuration to update", gt=0),
    db: Session = Depends(get_db_session),
    mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    """
    Partially updates an MCP server configuration identified by its integer ID.
    """
    logger.info(f"Attempting to update MCP server configuration with ID: {server_id}")
    logger.info(f"ENDPOINT (ID) - Update payload received: {update_payload.model_dump_json(indent=2)}")

    existing_config = mcp_config_crud_service.get_mcp_server_config(db=db, config_id=server_id)

    if not existing_config:
        logger.warning(f"MCP server configuration with ID '{server_id}' not found for update.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MCP Server configuration with ID '{server_id}' not found."
        )

    # Uniqueness checks for name and URL if they are being changed
    if update_payload.name is not None and update_payload.name != existing_config.name:
        colliding_config = mcp_config_crud_service.get_mcp_server_config_by_name(db, name=update_payload.name)
        if colliding_config and colliding_config.id != server_id: # Check it's not the current server itself
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Another MCP Server with the new name '{update_payload.name}' already exists."
            )

    if update_payload.url is not None and str(update_payload.url) != existing_config.url:
        url_to_check = str(update_payload.url)
        colliding_config = mcp_config_crud_service.get_mcp_server_config_by_url(db, url=url_to_check)
        if colliding_config and colliding_config.id != server_id: # Check it's not the current server itself
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Another MCP Server with the new URL '{url_to_check}' already exists."
            )

    updated_db_config = mcp_config_crud_service.update_mcp_server_config(
        db=db,
        config_id=server_id,
        config_in=update_payload
    )

    if not updated_db_config:
        logger.error(f"Failed to update MCP server configuration for ID '{server_id}' in the database.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not update MCP Server configuration with ID '{server_id}'."
        )

    logger.info(f"Successfully updated MCP server configuration '{updated_db_config.name}' (ID: {updated_db_config.id}).")

    try:
        await mcp_manager.update_server_from_config(updated_db_config)
        logger.info(f"Successfully notified MCPConnectionManager to re-evaluate server ID: {updated_db_config.id} after update.")
    except Exception as e:
        logger.error(f"Error notifying MCPConnectionManager after updating server ID '{updated_db_config.id}': {e}", exc_info=True)

    return updated_db_config


# Example curl command for the endpoint:
"""
curl -X DELETE http://localhost:8000/api/mcp-servers/mcp_mock_chatviewbasic
"""
@router.delete("/id/{server_id}", response_model=Dict[str, str], status_code=status.HTTP_200_OK)
async def delete_mcp_server_config_by_id_endpoint(
    server_id: int = Path(..., description="The ID of the MCP server configuration to delete", gt=0),
    db: Session = Depends(get_db_session),
    mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    """
    Deletes an MCP server configuration by its integer ID.
    """
    logger.info(f"Attempting to delete MCP server configuration with ID: {server_id}")
    config_to_delete = mcp_config_crud_service.get_mcp_server_config(db=db, config_id=server_id)

    if not config_to_delete:
        logger.warning(f"MCP server configuration with ID '{server_id}' not found for deletion.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"MCP Server configuration with ID '{server_id}' not found."
        )

    config_name = config_to_delete.name # For the response message

    delete_successful = mcp_config_crud_service.delete_mcp_server_config(db=db, config_id=server_id)

    if not delete_successful:
        logger.error(f"Failed to delete MCP server configuration '{config_name}' (ID: {server_id}) from database.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not delete MCP Server '{config_name}' (ID: {server_id})."
        )

    logger.info(f"Successfully deleted MCP server '{config_name}' (ID: {server_id}) from database.")

    server_id_str = str(server_id)
    try:
        await mcp_manager.remove_server_by_id(server_db_id_str=server_id_str)
        logger.info(f"Successfully notified MCPConnectionManager to remove server ID: {server_id_str}")
    except Exception as e:
        logger.error(f"Error notifying MCPConnectionManager to remove server ID '{server_id_str}': {e}", exc_info=True)
        return {"message": f"MCP Server '{config_name}' (ID: {server_id}) deleted from DB, but manager notification encountered an issue."}

    return {"message": f"MCP Server '{config_name}' (ID: {server_id}) successfully deleted and manager notified."}