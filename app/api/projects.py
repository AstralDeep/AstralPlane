# app/api/projects.py

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path as FilePath
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from starlette import status

from ..config import settings  # Import settings to get configured MCP_SERVERS
# Import necessary schemas and services
from ..models.schemas import Project, ProjectCreate, SuccessResponse, ProjectsResponse, ProjectLayout
from ..services.auth_service import get_user_id
# --- MCP Integration Imports ---
from ..services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager

router = APIRouter()
logger = logging.getLogger(__name__)


# --- Configuration Storage Helper ---
class MCPConfigManager:
	"""Helper class to manage MCP server configurations persistently"""

	def __init__(self, config_file: str = "mcp_servers.json"):
		self.config_file = FilePath(config_file)
		self.ensure_config_file()

	def ensure_config_file(self):
		"""Ensure the configuration file exists"""
		if not self.config_file.exists():
			self.config_file.write_text(json.dumps([], indent=2))

	def load_configs(self) -> List[Dict[str, Any]]:
		"""Load server configurations from file"""
		try:
			with open(self.config_file, 'r') as f:
				return json.load(f)
		except (json.JSONDecodeError, FileNotFoundError):
			logger.warning(f"Could not load config file {self.config_file}, returning empty list")
			return []

	def save_configs(self, configs: List[Dict[str, Any]]):
		"""Save server configurations to file"""
		try:
			with open(self.config_file, 'w') as f:
				json.dump(configs, f, indent=2)
		except Exception as e:
			logger.error(f"Failed to save configurations: {e}")
			raise HTTPException(status_code=500, detail="Failed to save server configuration")

	def get_config_by_id(self, server_id: str) -> Optional[Dict[str, Any]]:
		"""Get a specific server configuration by ID"""
		configs = self.load_configs()
		return next((config for config in configs if config.get("id") == server_id), None)

	def add_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
		"""Add a new server configuration"""
		configs = self.load_configs()

		# Check if ID already exists
		if any(c.get("id") == config.get("id") for c in configs):
			raise HTTPException(status_code=400, detail="Server ID already exists")

		configs.append(config)
		self.save_configs(configs)
		return config

	def update_config(self, server_id: str, updates: Dict[str, Any]) -> Dict[str, Any]:
		"""Update an existing server configuration"""
		configs = self.load_configs()

		for i, config in enumerate(configs):
			if config.get("id") == server_id:
				# Update the configuration
				configs[i].update(updates)
				configs[i]["updated_at"] = datetime.now().isoformat()
				self.save_configs(configs)
				return configs[i]

		raise HTTPException(status_code=404, detail="Server configuration not found")

	def delete_config(self, server_id: str) -> bool:
		"""Delete a server configuration"""
		configs = self.load_configs()
		original_length = len(configs)

		configs = [config for config in configs if config.get("id") != server_id]

		if len(configs) == original_length:
			raise HTTPException(status_code=404, detail="Server configuration not found")

		self.save_configs(configs)
		return True

	def add_member_to_config(self, server_id: str, user_id: str, role: str = "member") -> Dict[str, Any]:
		"""Add a member to a server configuration"""
		config = self.get_config_by_id(server_id)
		if not config:
			raise HTTPException(status_code=404, detail="Server configuration not found")

		# Initialize members list if it doesn't exist
		if "members" not in config:
			config["members"] = []

		# Check if user is already a member
		if any(member.get("user_id") == user_id for member in config["members"]):
			raise HTTPException(status_code=400, detail="User is already a member")

		# Add the new member
		config["members"].append({"user_id": user_id, "role": role})
		return self.update_config(server_id, config)

	def remove_member_from_config(self, server_id: str, member_id: str) -> Dict[str, Any]:
		"""Remove a member from a server configuration"""
		config = self.get_config_by_id(server_id)
		if not config:
			raise HTTPException(status_code=404, detail="Server configuration not found")

		if "members" not in config:
			config["members"] = []

		original_length = len(config["members"])
		config["members"] = [member for member in config["members"] if member.get("user_id") != member_id]

		if len(config["members"]) == original_length:
			raise HTTPException(status_code=404, detail="Member not found")

		return self.update_config(server_id, config)


# Initialize the config manager
config_manager = MCPConfigManager()


# --- Helper Functions ---
def dict_to_project(server_config: Dict[str, Any], user_id: str) -> Project:
	"""Convert a server configuration dictionary to a Project object"""
	now = datetime.now()

	return Project(
		id=server_config["id"],
		name=server_config.get("name", f"Server {server_config['id']}"),
		description=server_config.get("description", "MCP Server"),
		owner_id=server_config.get("owner_id", "system"),
		created_at=datetime.fromisoformat(server_config.get("created_at", now.isoformat())),
		updated_at=datetime.fromisoformat(server_config.get("updated_at", now.isoformat())),
		members=server_config.get("members", [{"user_id": user_id, "role": "member"}]),
		views=server_config.get("views", {}),
		layout=ProjectLayout(),
		project_type=f"mcp_{server_config.get('transport', 'unknown')}"
	)


def project_create_to_dict(project: ProjectCreate, user_id: str) -> Dict[str, Any]:
	"""Convert a ProjectCreate object to a server configuration dictionary"""
	now = datetime.now()

	return {
		"id": project.name.lower().replace(" ", "_"),  # Generate ID from name
		"name": project.name,
		"description": project.description or "MCP Server",
		"owner_id": user_id,
		"created_at": now.isoformat(),
		"updated_at": now.isoformat(),
		"members": [{"user_id": user_id, "role": "owner"}],
		"views": {},
		"transport": "stdio",  # Default transport
		"command": ["python", "-m", "server"],  # Default command
		"args": []
	}


# --- GET / Endpoint (Lists Configured MCP Servers - Proactive Filter) ---
@router.get("/", response_model=ProjectsResponse)
async def get_projects(
		user_id: str = Depends(get_user_id),
		skip: int = Query(0, ge=0, description="Number of projects/servers to skip"),
		limit: int = Query(10, ge=1, description="Maximum number of projects/servers to return"),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""Get available MCP servers that are connected and UI-ready based on proactive checks."""
	logger.info(f"Generating UI-ready project/server list for user '{user_id}' based on stored state.")

	# Load configurations from both settings and persistent storage
	persistent_configs = config_manager.load_configs()
	settings_configs = getattr(settings, 'MCP_SERVERS', [])

	# Combine configurations (persistent takes precedence)
	all_configs = {}
	for config in settings_configs + persistent_configs:
		all_configs[config["id"]] = config

	valid_ui_projects: List[Project] = []
	configured_server_ids = list(all_configs.keys())

	# Check readiness concurrently
	readiness_checks = await asyncio.gather(
		*(mcp_conn_manager.is_server_ui_ready(sid) for sid in configured_server_ids))

	for i, server_id in enumerate(configured_server_ids):
		is_ready = readiness_checks[i]
		server_config = all_configs.get(server_id)

		if is_ready and server_config:
			logger.debug(f"Server '{server_id}' is UI-ready. Including in list.")

			# Check if user has access to this server
			members = server_config.get("members", [])
			user_has_access = any(member.get("user_id") == user_id for member in members) or not members

			if user_has_access:
				project = dict_to_project(server_config, user_id)
				valid_ui_projects.append(project)
		else:
			logger.info(f"Filtering out server '{server_id}': Not UI-ready based on stored state.")

	# Apply pagination
	current_project: Optional[Project] = valid_ui_projects[0] if valid_ui_projects else None
	start = skip
	end = skip + limit
	paginated_projects = valid_ui_projects[start:end]

	current_project_id_log = current_project.id if current_project else 'None'
	logger.info(f"Returning {len(paginated_projects)} UI-ready servers. Current default: {current_project_id_log}")

	return ProjectsResponse(
		projects=paginated_projects,
		current_project=current_project
	)


# --- GET /{project_id} Endpoint ---
@router.get("/{project_id}", response_model=Project)
async def get_project(
		project_id: str = Path(..., description="The ID of the configured MCP server"),
		user_id: str = Depends(get_user_id)
):
	"""Get details for a specific configured MCP server, represented as a Project."""
	logger.info(f"Getting details for project/server ID: {project_id} for user '{user_id}'.")

	# Try to get from persistent storage first, then settings
	server_config = config_manager.get_config_by_id(project_id)
	if not server_config:
		settings_configs = getattr(settings, 'MCP_SERVERS', [])
		server_config = next((s for s in settings_configs if s.get("id") == project_id), None)

	if not server_config:
		logger.warning(f"Project/Server ID '{project_id}' not found.")
		raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project/Server not found")

	# Check access
	members = server_config.get("members", [])
	user_has_access = any(member.get("user_id") == user_id for member in members) or not members

	if not user_has_access:
		raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

	project = dict_to_project(server_config, user_id)
	logger.info(f"Returning details for server '{project_id}'.")
	return project


# --- POST / Endpoint (Create Project) ---
@router.post("/", response_model=Project, status_code=status.HTTP_201_CREATED)
async def create_project(
		project: ProjectCreate,
		user_id: str = Depends(get_user_id),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""Create a new MCP server configuration."""
	logger.info(f"User '{user_id}' creating new project: {project.name}")

	# Convert ProjectCreate to server configuration
	server_config = project_create_to_dict(project, user_id)

	try:
		# Add to persistent storage
		saved_config = config_manager.add_config(server_config)

		# Optionally, try to initialize the connection (don't fail if this doesn't work)
		try:
			await mcp_conn_manager.add_server_config(saved_config)
		except Exception as e:
			logger.warning(f"Failed to initialize connection for new server {saved_config['id']}: {e}")

		# Convert back to Project for response
		project_response = dict_to_project(saved_config, user_id)
		logger.info(f"Successfully created project '{project_response.id}'")
		return project_response

	except HTTPException:
		raise
	except Exception as e:
		logger.error(f"Failed to create project: {e}")
		raise HTTPException(status_code=500, detail="Failed to create project")


# --- PUT /{project_id} Endpoint (Update Project) ---
@router.put("/{project_id}", response_model=Project)
async def update_project(
		project_update: Dict[str, Any],
		project_id: str = Path(..., description="The ID of the project/server to update"),
		user_id: str = Depends(get_user_id),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""Update a project/server configuration."""
	logger.info(f"User '{user_id}' updating project '{project_id}'")

	# Get current configuration
	current_config = config_manager.get_config_by_id(project_id)
	if not current_config:
		raise HTTPException(status_code=404, detail="Project/Server not found")

	# Check if user has permission to update (owner or admin)
	members = current_config.get("members", [])
	user_member = next((m for m in members if m.get("user_id") == user_id), None)

	if not user_member or user_member.get("role") not in ["owner", "admin"]:
		raise HTTPException(status_code=403, detail="Insufficient permissions to update project")

	try:
		# Update the configuration
		updated_config = config_manager.update_config(project_id, project_update)

		# Optionally, update the connection manager
		try:
			await mcp_conn_manager.update_server_config(project_id, updated_config)
		except Exception as e:
			logger.warning(f"Failed to update connection for server {project_id}: {e}")

		# Convert back to Project for response
		project_response = dict_to_project(updated_config, user_id)
		logger.info(f"Successfully updated project '{project_id}'")
		return project_response

	except HTTPException:
		raise
	except Exception as e:
		logger.error(f"Failed to update project: {e}")
		raise HTTPException(status_code=500, detail="Failed to update project")


# --- DELETE /{project_id} Endpoint (Delete Project) ---
@router.delete("/{project_id}", response_model=SuccessResponse)
async def delete_project(
		project_id: str = Path(..., description="The ID of the project/server to delete"),
		user_id: str = Depends(get_user_id),
		mcp_conn_manager: MCPConnectionManager = Depends(get_mcp_connection_manager)
):
	"""Delete a project/server configuration."""
	logger.info(f"User '{user_id}' deleting project '{project_id}'")

	# Get current configuration
	current_config = config_manager.get_config_by_id(project_id)
	if not current_config:
		raise HTTPException(status_code=404, detail="Project/Server not found")

	# Check if user has permission to delete (owner only)
	members = current_config.get("members", [])
	user_member = next((m for m in members if m.get("user_id") == user_id), None)

	if not user_member or user_member.get("role") != "owner":
		raise HTTPException(status_code=403, detail="Only the owner can delete this project")

	try:
		# Delete from persistent storage
		config_manager.delete_config(project_id)

		# Optionally, disconnect from the connection manager
		try:
			await mcp_conn_manager.remove_server_config(project_id)
		except Exception as e:
			logger.warning(f"Failed to disconnect server {project_id}: {e}")

		logger.info(f"Successfully deleted project '{project_id}'")
		return SuccessResponse(message=f"Project '{project_id}' deleted successfully")

	except HTTPException:
		raise
	except Exception as e:
		logger.error(f"Failed to delete project: {e}")
		raise HTTPException(status_code=500, detail="Failed to delete project")


# --- POST /{project_id}/members Endpoint (Add Member) ---
@router.post("/{project_id}/members", response_model=SuccessResponse)
async def add_project_member(
		user_data: Dict[str, str],
		project_id: str = Path(..., description="The ID of the project/server"),
		user_id: str = Depends(get_user_id)
):
	"""Add a member to a project."""
	logger.info(f"User '{user_id}' adding member to project '{project_id}'")

	# Validate input
	if "user_id" not in user_data:
		raise HTTPException(status_code=400, detail="user_id is required")

	new_user_id = user_data["user_id"]
	role = user_data.get("role", "member")

	# Validate role
	if role not in ["member", "admin", "owner"]:
		raise HTTPException(status_code=400, detail="Invalid role. Must be 'member', 'admin', or 'owner'")

	# Get current configuration
	current_config = config_manager.get_config_by_id(project_id)
	if not current_config:
		raise HTTPException(status_code=404, detail="Project/Server not found")

	# Check if user has permission to add members (owner or admin)
	members = current_config.get("members", [])
	user_member = next((m for m in members if m.get("user_id") == user_id), None)

	if not user_member or user_member.get("role") not in ["owner", "admin"]:
		raise HTTPException(status_code=403, detail="Insufficient permissions to add members")

	try:
		config_manager.add_member_to_config(project_id, new_user_id, role)
		logger.info(f"Successfully added member '{new_user_id}' to project '{project_id}'")
		return SuccessResponse(message=f"Member '{new_user_id}' added successfully")

	except HTTPException:
		raise
	except Exception as e:
		logger.error(f"Failed to add member: {e}")
		raise HTTPException(status_code=500, detail="Failed to add member")


# --- DELETE /{project_id}/members/{member_id} Endpoint (Remove Member) ---
@router.delete("/{project_id}/members/{member_id}", response_model=SuccessResponse)
async def remove_project_member(
		project_id: str = Path(..., description="The ID of the project/server"),
		member_id: str = Path(..., description="The ID of the member to remove"),
		user_id: str = Depends(get_user_id)
):
	"""Remove a member from a project."""
	logger.info(f"User '{user_id}' removing member '{member_id}' from project '{project_id}'")

	# Get current configuration
	current_config = config_manager.get_config_by_id(project_id)
	if not current_config:
		raise HTTPException(status_code=404, detail="Project/Server not found")

	# Check if user has permission to remove members (owner or admin)
	members = current_config.get("members", [])
	user_member = next((m for m in members if m.get("user_id") == user_id), None)

	if not user_member or user_member.get("role") not in ["owner", "admin"]:
		raise HTTPException(status_code=403, detail="Insufficient permissions to remove members")

	# Don't allow removing the owner
	member_to_remove = next((m for m in members if m.get("user_id") == member_id), None)
	if member_to_remove and member_to_remove.get("role") == "owner":
		raise HTTPException(status_code=400, detail="Cannot remove the project owner")

	try:
		config_manager.remove_member_from_config(project_id, member_id)
		logger.info(f"Successfully removed member '{member_id}' from project '{project_id}'")
		return SuccessResponse(message=f"Member '{member_id}' removed successfully")

	except HTTPException:
		raise
	except Exception as e:
		logger.error(f"Failed to remove member: {e}")
		raise HTTPException(status_code=500, detail="Failed to remove member")
