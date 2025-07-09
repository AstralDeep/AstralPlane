# app/services/mcp_config_crud_service.py
from typing import List, Optional
from sqlalchemy import true, false
from sqlalchemy.orm import Session
from app.models.mcp_server_config_model import MCPServerConfig
from app.models.schemas import MCPServerConfigCreate, MCPServerConfigUpdate


def create_mcp_server_config(db: Session, config_in: MCPServerConfigCreate) -> MCPServerConfig:
	db_config = MCPServerConfig(**config_in.model_dump())
	db.add(db_config)
	db.commit()
	db.refresh(db_config)
	return db_config


def get_mcp_server_config(db: Session, config_id: str) -> Optional[MCPServerConfig]:
	return db.query(MCPServerConfig).filter(MCPServerConfig.id.is_(config_id)).first()


def get_mcp_server_config_by_name(db: Session, name: str) -> Optional[MCPServerConfig]:
	return db.query(MCPServerConfig).filter(MCPServerConfig.name.is_(name)).first()


def get_mcp_server_config_by_url(db: Session, url: str) -> Optional[MCPServerConfig]:
	return db.query(MCPServerConfig).filter(MCPServerConfig.url.is_(url)).first()


def get_mcp_server_configs(db: Session, skip: int = 0, limit: int = 100, only_active: Optional[bool] = None) -> List[
	MCPServerConfig]:
	query = db.query(MCPServerConfig)
	if only_active is not None:  # Allows filtering for active (True) or inactive (False)
		# Corrected line to build a proper SQL expression
		if only_active:
			query = query.filter(MCPServerConfig.is_active == true())
		else:
			query = query.filter(MCPServerConfig.is_active == false())

	return query.order_by(MCPServerConfig.id).offset(skip).limit(limit).all()


def update_mcp_server_config(db: Session, config_id: str, config_in: MCPServerConfigUpdate) -> Optional[
	MCPServerConfig]:
	print(f"CRUD - Payload received (config_in): {config_in.model_dump_json(indent=2)}")
	db_config = get_mcp_server_config(db, config_id)
	if db_config:
		update_data = config_in.model_dump(exclude_unset=True)
		# ADD THIS LOG:
		print(f"CRUD - Data to be applied (after exclude_unset=True): {update_data}")
		for key, value in update_data.items():
			setattr(db_config, key, value)
		db.commit()
		db.refresh(db_config)
	return db_config


def delete_mcp_server_config(db: Session, config_id: str) -> bool:
	db_config = get_mcp_server_config(db, config_id)
	if db_config:
		db.delete(db_config)
		db.commit()
		return True
	return False
