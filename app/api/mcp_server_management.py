# app/api/mcp_server_management.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from app.database import get_db
from app.services import mcp_config_crud_service # Make sure services/__init__.py exists or adjust import
from app.models.schemas import MCPServerConfigCreate, MCPServerConfigUpdate, MCPServerConfigResponse
from app.services.mcp_connection_manager import get_mcp_connection_manager, MCPConnectionManager # You'll need this

router = APIRouter()
# You might want to add security dependencies here, e.g., require admin user

@router.post("/", response_model=MCPServerConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_new_mcp_server_config(
    config_in: MCPServerConfigCreate,
    db: Session = Depends(get_db),
    mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    if mcp_config_crud_service.get_mcp_server_config_by_name(db, name=config_in.name):
        raise HTTPException(status_code=400, detail=f"MCP Server with name '{config_in.name}' already exists.")
    if mcp_config_crud_service.get_mcp_server_config_by_url(db, url=config_in.url):
        raise HTTPException(status_code=400, detail=f"MCP Server with URL '{config_in.url}' already exists.")

    new_config = mcp_config_crud_service.create_mcp_server_config(db=db, config_in=config_in)
    if new_config.is_active:
        await mcp_manager.add_server_from_config(new_config) # Notify manager
    return new_config

@router.get("/", response_model=List[MCPServerConfigResponse])
def read_all_mcp_server_configs(
    skip: int = 0,
    limit: int = 100,
    active: Optional[bool] = None, # None means all, True for active, False for inactive
    db: Session = Depends(get_db)
):
    configs = mcp_config_crud_service.get_mcp_server_configs(db, skip=skip, limit=limit, only_active=active)
    return configs

@router.get("/{config_id}", response_model=MCPServerConfigResponse)
def read_mcp_server_config_by_id(config_id: int, db: Session = Depends(get_db)):
    db_config = mcp_config_crud_service.get_mcp_server_config(db, config_id=config_id)
    if db_config is None:
        raise HTTPException(status_code=404, detail="MCP Server configuration not found")
    return db_config

@router.put("/{config_id}", response_model=MCPServerConfigResponse)
async def update_existing_mcp_server_config(
    config_id: int,
    config_in: MCPServerConfigUpdate,
    db: Session = Depends(get_db),
    mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    existing_config = mcp_config_crud_service.get_mcp_server_config(db, config_id=config_id)
    if not existing_config:
        raise HTTPException(status_code=404, detail="MCP Server configuration not found")

    # Check for potential name/URL conflicts if they are being changed
    if config_in.name and config_in.name != existing_config.name:
        if mcp_config_crud_service.get_mcp_server_config_by_name(db, name=config_in.name):
            raise HTTPException(status_code=400, detail=f"MCP Server with name '{config_in.name}' already exists.")
    if config_in.url and config_in.url != existing_config.url:
        if mcp_config_crud_service.get_mcp_server_config_by_url(db, url=config_in.url):
            raise HTTPException(status_code=400, detail=f"MCP Server with URL '{config_in.url}' already exists.")

    updated_config = mcp_config_crud_service.update_mcp_server_config(db, config_id=config_id, config_in=config_in)
    if updated_config is None: # Should be caught by the check above, but for safety
        raise HTTPException(status_code=404, detail="MCP Server configuration not found during update")

    # Notify MCPConnectionManager about the update
    await mcp_manager.update_server_from_config(updated_config)
    return updated_config

@router.delete("/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_existing_mcp_server_config(
    config_id: int,
    db: Session = Depends(get_db),
    mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
    config_to_delete = mcp_config_crud_service.get_mcp_server_config(db, config_id=config_id)
    if not config_to_delete:
        raise HTTPException(status_code=404, detail="MCP Server configuration not found")

    deleted = mcp_config_crud_service.delete_mcp_server_config(db, config_id=config_id)
    if not deleted:
         # Should have been caught by the get_mcp_server_config check
        raise HTTPException(status_code=500, detail="Failed to delete MCP Server configuration")

    await mcp_manager.remove_server_by_id(str(config_to_delete.id)) # Use the DB ID
    return None # For 204 No Content response