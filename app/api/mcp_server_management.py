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

@router.post("/", response_model=MCPServerConfigResponse, status_code=status.HTTP_201_CREATED)
async def create_new_mcp_server_config(
		config_payload: MCPServerConfigCreate,
		db: Session = Depends(get_db_session),
		mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	# Assuming 'id' is part of config_payload and is the string identifier.
	# The mcp_config_crud_service.create_mcp_server_config should handle or validate this.
	# Example check if ID is provided and already exists (if 'id' is in MCPServerConfigCreate):
	if hasattr(config_payload, 'id') and config_payload.id:
		# Assuming a function like get_mcp_server_config_by_id that takes the string ID
		db_config_by_id = mcp_config_crud_service.get_mcp_server_config(db, config_id=config_payload.id)
		if db_config_by_id:
			raise HTTPException(status_code=400, detail=f"MCP Server with ID '{config_payload.id}' already exists.")

	db_config_by_name = mcp_config_crud_service.get_mcp_server_config_by_name(db, name=config_payload.name)
	if db_config_by_name and (not hasattr(config_payload, 'id') or db_config_by_name.id != config_payload.id):
		raise HTTPException(status_code=400, detail=f"MCP Server with name '{config_payload.name}' already exists.")

	db_config_by_url = mcp_config_crud_service.get_mcp_server_config_by_url(db, url=config_payload.url)
	if db_config_by_url and (not hasattr(config_payload, 'id') or db_config_by_url.id != config_payload.id):
		raise HTTPException(status_code=400, detail=f"MCP Server with URL '{config_payload.url}' already exists.")

	new_config_db_model = mcp_config_crud_service.create_mcp_server_config(db=db, config_in=config_payload)

	await mcp_manager.add_server_from_config(new_config_db_model)
	return new_config_db_model


# Example curl command for the endpoint:
"""
curl -X GET "http://localhost:8000/api/mcp-servers/?skip=0&limit=5"
# - OR -
curl -X GET "http://localhost:8000/api/mcp-servers/?only_active=true"
# - OR -
curl -X GET "http://localhost:8000/api/mcp-servers/?skip=0&limit=10&only_active=false"
"""
@router.get("/", response_model=List[MCPServerConfigResponse], status_code=status.HTTP_200_OK)
async def get_all_mcp_server_configs(
		skip: int = Query(0, ge=0, description="Number of records to skip for pagination"),
		limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return"),
		only_active: Optional[bool] = Query(None, description="Filter by active status (true or false)"),
		db: Session = Depends(get_db_session)
):
	logger.info(f"Fetching all MCP server configurations with skip={skip}, limit={limit}, only_active={only_active}")
	configs = mcp_config_crud_service.get_mcp_server_configs(
		db=db, skip=skip, limit=limit, only_active=only_active
	)
	return configs


# Example curl command for the endpoint:
# The server_id in the path is now a string (e.g., "mcp_mock_chatviewbasic").
"""
curl -X PATCH http://localhost:8000/api/mcp-servers/id/mcp_mock_chatviewbasic \
-H "Content-Type: application/json" \
-d '{
  "name": "Mock MCP Chat Server (Updated Name)",
  "description": "This server has an updated description and is now inactive.",
  "url": "http://127.0.0.1:8123/sse",
  "is_active": false
}'
"""
@router.patch("/id/{server_id}", response_model=MCPServerConfigResponse, status_code=status.HTTP_200_OK)
async def update_mcp_server_config_by_id_endpoint(
		update_payload: MCPServerConfigUpdate,
		server_id: str = Path(..., description="The string ID of the MCP server configuration to update"),
		db: Session = Depends(get_db_session),
		mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	logger.info(f"Attempting to update MCP server configuration with ID: {server_id}")
	# The 'server_id' here is a string from the path.
	# The CRUD service get_mcp_server_config should expect a string config_id.
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
		# Ensure the colliding config is not the current server itself
		if colliding_config and colliding_config.id != server_id:
			raise HTTPException(
				status_code=status.HTTP_409_CONFLICT,
				detail=f"Another MCP Server with the new name '{update_payload.name}' already exists (ID: {colliding_config.id})."
			)

	if update_payload.url is not None and str(update_payload.url) != existing_config.url:
		url_to_check = str(update_payload.url)
		colliding_config = mcp_config_crud_service.get_mcp_server_config_by_url(db, url=url_to_check)
		# Ensure the colliding config is not the current server itself
		if colliding_config and colliding_config.id != server_id:
			raise HTTPException(
				status_code=status.HTTP_409_CONFLICT,
				detail=f"Another MCP Server with the new URL '{url_to_check}' already exists (ID: {colliding_config.id})."
			)

	updated_db_config = mcp_config_crud_service.update_mcp_server_config(
		db=db,
		config_id=server_id,  # Passes the string ID
		config_in=update_payload
	)

	if not updated_db_config:  # Should not happen if existing_config was found, but good practice
		logger.error(f"Failed to update MCP server configuration for ID '{server_id}' in the database.")
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,  # Or 404 if it somehow disappeared
			detail=f"Could not update MCP Server configuration with ID '{server_id}'."
		)

	logger.info(
		f"Successfully updated MCP server configuration '{updated_db_config.name}' (ID: {updated_db_config.id}).")
	await mcp_manager.update_server_from_config(updated_db_config)
	return updated_db_config


# Example curl command for the endpoint:
# The server_id in the path is now a string (e.g., "mcp_mock_chatviewbasic").
"""
curl -X DELETE http://localhost:8000/api/mcp-servers/id/mcp_mock_chatviewbasic
"""
@router.delete("/id/{server_id}", response_model=Dict[str, str], status_code=status.HTTP_200_OK)
async def delete_mcp_server_config_by_id_endpoint(
		server_id: str = Path(..., description="The string ID of the MCP server configuration to delete"),
		db: Session = Depends(get_db_session),
		mcp_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	logger.info(f"Attempting to delete MCP server configuration with ID: {server_id}")
	# server_id is a string here. The CRUD service must be able to use it.
	config_to_delete = mcp_config_crud_service.get_mcp_server_config(db=db, config_id=server_id)

	if not config_to_delete:
		logger.warning(f"MCP server configuration with ID '{server_id}' not found for deletion.")
		raise HTTPException(
			status_code=status.HTTP_404_NOT_FOUND,
			detail=f"MCP Server configuration with ID '{server_id}' not found."
		)

	config_name = config_to_delete.name

	delete_successful = mcp_config_crud_service.delete_mcp_server_config(db=db, config_id=server_id)  # Uses string ID

	if not delete_successful:
		logger.error(f"Failed to delete MCP server configuration '{config_name}' (ID: {server_id}) from database.")
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
			detail=f"Could not delete MCP Server '{config_name}' (ID: {server_id})."
		)

	logger.info(f"Successfully deleted MCP server '{config_name}' (ID: {server_id}) from database.")

	try:
		# MCPConnectionManager should expect the string ID.
		await mcp_manager.remove_server_by_id(server_db_id_str=server_id)
		logger.info(f"Successfully notified MCPConnectionManager to remove server with ID: {server_id}")
	except Exception as e:
		logger.error(f"Error notifying MCPConnectionManager to remove server ID '{server_id}': {e}", exc_info=True)
		return {
			"message": f"MCP Server '{config_name}' (ID: {server_id}) deleted from DB, but manager notification encountered an issue."}

	return {"message": f"MCP Server '{config_name}' (ID: {server_id}) successfully deleted and manager notified."}
