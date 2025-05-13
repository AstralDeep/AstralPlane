# app/api/mcp_server_management.py
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db_session
from app.models.schemas import MCPServerConfigCreate, MCPServerConfigResponse
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
