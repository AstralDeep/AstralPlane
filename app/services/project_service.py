# app/services/project_service.py

import logging
import time
from typing import List, Dict, Any, Optional, Set

# --- Core FastAPI/Python Imports ---
from fastapi import WebSocket # WebSocket is used in a method signature
# Depends and Path are not used directly in this file if ProjectViewsService is not a route item.
# from fastapi import Depends, Path
from pydantic import ValidationError

# --- Service Dependencies (Import only the classes needed for type hints) ---
from app.services.connection_manager import ConnectionManager # Import the CLASS for type hint
from app.services.mcp_connection_manager import MCPConnectionManager # Import the CLASS for type hint
# REMOVE the import of get_connection_manager and get_mcp_connection_manager functions here
# if they are not used elsewhere in THIS FILE.

from mcp.client.session import ClientSession # For type hint in get_project_ui_hierarchy

# --- UI Model Imports ---
from app.models.schemas import (
    UIElement, StackLayoutConfig, ViewConfig, Project, ProjectLayout
)

from app.utils.websocket_logger import WebSocketLogger

logger = logging.getLogger(__name__)


# --- ProjectService (Keep as is or remove if unused) ---
class ProjectService:
	_instance = None

	def __new__(cls, repository=None):
		if cls._instance is None: cls._instance = super(ProjectService, cls).__new__(
			cls); cls._instance._initialized = False
		return cls._instance

	def __init__(self, repository=None):
		if getattr(self, "_initialized", False): return
		logger.info("Project service initialized (potentially inactive).");
		self._initialized = True

	def get_project(self, project_id: str) -> Optional[Project]:
		return None

	def user_has_access(self, user_id: Optional[str], project_id: str) -> bool:
		return True


# --- ProjectViewsService (MODIFIED FOR PROACTIVE LAYOUT & STRICT CHECK) ---
class ProjectViewsService:
	"""
    Service for retrieving server-defined UI layout (fetched proactively)
    and performing strict validation against client capabilities.
    """
	# The _instance and __new__ pattern for singleton might be redundant
	# if lifespan is the sole creator and app.state is the sole provider.
	# However, it doesn't hurt to keep it for now.
	_instance = None

	def __new__(cls, *args, **kwargs):  # Modified to accept args for __init__
		if cls._instance is None:
			cls._instance = super(ProjectViewsService, cls).__new__(cls)
			cls._instance._initialized = False  # Ensure init runs only once via the flag
		return cls._instance

	def __init__(
			self,
			mcp_conn_manager: MCPConnectionManager,
			connection_manager: ConnectionManager  # This is the UI ConnectionManager
	):
		"""Initializes the service with pre-resolved dependencies."""
		# Check if already initialized (part of the singleton pattern from your code)
		if getattr(self, "_initialized", False) and self._initialized:
			return

		logger.info("--- ENTERING ProjectViewsService.__init__ (Proactive Server UI Layout Mode) ---")
		if not mcp_conn_manager: raise ValueError("MCPConnectionManager dependency is required for ProjectViewsService")
		if not connection_manager: raise ValueError(
			"ConnectionManager (UI) dependency is required for ProjectViewsService")

		self.mcp_conn_manager = mcp_conn_manager
		self.connection_manager = connection_manager  # For client capabilities
		logger.info("Project Views Service initialized (Proactive Server UI Layout Mode)")
		self._initialized = True

	def _validate_layout_capabilities(self, element_dict: Dict[str, Any], supported_primitives: Set[str]) -> bool:
		# ... (your existing validation logic - keep as is) ...
		primitive_type = element_dict.get('type')
		element_id = element_dict.get('id', 'unknown')
		if not primitive_type:
			logger.error(f"Layout Validation Error: Element '{element_id}' missing 'type' field.")
			return False
		if primitive_type not in supported_primitives:
			logger.error(
				f"Layout Validation FAILED: Client does not support required primitive '{primitive_type}' for element '{element_id}'.")
			return False
		if 'children' in element_dict and element_dict['children']:
			if not isinstance(element_dict['children'], list):
				logger.error(f"Layout Validation Error: Element '{element_id}' has invalid 'children' format.")
				return False
			for child_dict in element_dict['children']:
				if not isinstance(child_dict, dict):
					logger.error(f"Layout Validation Error: Child of element '{element_id}' is not a dict.")
					return False
				if not self._validate_layout_capabilities(child_dict, supported_primitives):
					return False
		return True

	async def get_project_ui_hierarchy(
			self,
			websocket: WebSocket,
			stream_id: str,  # e.g., "mcp:1" where "1" is the DB ID
			mcp_session: Optional[ClientSession] = None  # <-- ADDED: Live MCP Session passed from websocket_endpoint
	) -> Optional[UIElement]:
		"""
        Retrieves the pre-fetched UI layout, performs strict validation against
        client capabilities, and returns the UI structure.
        Can now optionally use the live mcp_session for further operations if needed.
        """
		ws_client_info = f"{WebSocketLogger.get_client_info(websocket)}"
		logger.info(f"Getting UI hierarchy for stream '{stream_id}' for client {ws_client_info}...")
		logger.debug(f"Live MCP session provided to get_project_ui_hierarchy: {'Yes' if mcp_session else 'No'}")

		# ... (your existing pre-checks for self.connection_manager, self.mcp_conn_manager - keep as is) ...
		if not self.connection_manager or not self.mcp_conn_manager:
			logger.error(f"[{stream_id}] Cannot generate UI: Service dependencies missing.")
			return self._create_error_element("Internal Error: Service dependencies missing.")

		# --- 1. Get Client Capabilities (Keep as is) ---
		supported_primitives_list = self.connection_manager.get_supported_primitives(websocket)
		if not supported_primitives_list:
			logger.error(
				f"[{stream_id}] Client {ws_client_info} did not declare supported primitives. Cannot validate layout.")
			return self._create_error_element("Client capabilities not received. Cannot display UI.")
		supported_primitives: Set[str] = set(supported_primitives_list)
		logger.debug(f"[{stream_id}] Client {ws_client_info} supports primitives: {supported_primitives}")

		# --- 2. Identify Target MCP Server (Keep as is) ---
		if not stream_id.startswith("mcp:"):
			logger.error(f"[{stream_id}] Invalid stream format.");
			return self._create_error_element("Invalid connection target format.")
		mcp_server_id = stream_id.split(":", 1)[1]  # This is the DB ID string
		if not mcp_server_id:
			logger.error(f"[{stream_id}] Missing MCP server ID.");
			return self._create_error_element("Missing target server ID.")

		# --- 3. Retrieve Pre-Fetched Server Layout (Keep as is) ---
		logger.debug(f"[{stream_id}] Fetching pre-defined UI layout for server '{mcp_server_id}'...")
		server_layout_dict = await self.mcp_conn_manager.get_retrieved_ui_layout(mcp_server_id)

		if server_layout_dict is None:
			logger.error(f"[{stream_id}] UI layout not available for server '{mcp_server_id}' (check startup logs).")
			details = await self.mcp_conn_manager.get_connection_details(mcp_server_id)
			error_msg = details.get("error_message") if details else "Layout unavailable"
			status_msg = details.get("status") if details else "unknown"
			return self._create_error_element(
				f"UI not available for '{mcp_server_id}'. Status: {status_msg}. Reason: {error_msg}")
		logger.info(f"[{stream_id}] Retrieved pre-fetched UI layout from server '{mcp_server_id}'.")

		# --- OPTIONAL: Use mcp_session here if needed ---
		# if mcp_session:
		#     logger.debug(f"[{stream_id}] Live MCP session available. Can perform additional server calls if needed for UI generation.")
		#     # Example: ui_enhancements = await mcp_session.call_tool("get_dynamic_ui_parts", {"base_layout_id": server_layout_dict.get("id")})
		#     # server_layout_dict = {**server_layout_dict, **ui_enhancements} # Merge or modify

		# --- 4. Strict Capability Validation (Keep as is) ---
		logger.debug(f"[{stream_id}] Performing STRICT validation of layout against client capabilities...")
		try:
			aligned_layout_dict = server_layout_dict
			is_layout_supported = self._validate_layout_capabilities(aligned_layout_dict, supported_primitives)
			if not is_layout_supported:
				logger.error(
					f"[{stream_id}] UI layout from server '{mcp_server_id}' requires primitives not supported by client {ws_client_info}.")
				return self._create_error_element(
					f"UI for '{mcp_server_id}' requires capabilities not supported by your client.")
			logger.info(f"[{stream_id}] Client capabilities sufficient for the received UI layout.")
		except Exception as val_err:
			logger.error(f"[{stream_id}] Error during layout capability validation: {val_err}", exc_info=True)
			return self._create_error_element("Internal error validating UI layout.")

		# --- 5. Pydantic Validation and Conversion (Keep as is) ---
		try:
			root_element = UIElement(**aligned_layout_dict)
			logger.info(f"[{stream_id}] Successfully validated and converted server layout. Root ID: {root_element.id}")
			return root_element
		except ValidationError as e:
			logger.error(f"[{stream_id}] Pydantic validation failed for server layout: {e}", exc_info=True)
			return self._create_error_element("Received invalid UI layout structure from the server.")
		except Exception as conv_err:
			logger.error(f"[{stream_id}] Error converting layout dict to UIElement: {conv_err}", exc_info=True)
			return self._create_error_element("Internal error processing UI layout structure.")

	def _create_error_element(self, message: str) -> UIElement:
		error_id = f"error-display-{abs(hash(message + str(time.time())))}"
		logger.debug(f"Creating error UI element id='{error_id}' msg='{message}'")
		return UIElement(
			id=error_id, type='TextView',
			config={"initialText": f"Error: {message}", "variant": "error"}
		)
