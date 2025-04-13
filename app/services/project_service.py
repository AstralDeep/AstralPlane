# app/services/project_service.py
# --- COMPLETE REPLACEMENT - USES PRE-FETCHED LAYOUT & STRICT CAPABILITY CHECK ---
# MODIFIED: Calls WebSocketLogger.get_client_info instead of non-existent method

import logging
import time
from typing import List, Dict, Any, Optional, Set

# --- Core FastAPI/Python Imports ---
from fastapi import Depends, WebSocket, Path
from pydantic import ValidationError

# --- Service Dependencies ---
from app.services.connection_manager import ConnectionManager, get_connection_manager
from app.services.mcp_connection_manager import MCPConnectionManager, get_mcp_connection_manager

# --- UI Model Imports ---
from app.models.schemas import (
    UIElement, StackLayoutConfig, ViewConfig, Project, ProjectLayout # Keep UIElement
)

# --- ADD THIS IMPORT ---
from app.utils.websocket_logger import WebSocketLogger
# --- END IMPORT ---

logger = logging.getLogger(__name__)

# --- ProjectService (Keep as is or remove if unused) ---
class ProjectService:
    # ... (Keep existing minimal implementation or remove) ...
    _instance = None
    def __new__(cls, repository=None):
        if cls._instance is None: cls._instance = super(ProjectService, cls).__new__(cls); cls._instance._initialized = False
        return cls._instance
    def __init__(self, repository = None):
        if getattr(self, "_initialized", False): return
        logger.info("Project service initialized (potentially inactive)."); self._initialized = True
    def get_project(self, project_id: str) -> Optional[Project]: return None # Placeholder
    def user_has_access(self, user_id: Optional[str], project_id: str) -> bool: return True # Placeholder


# --- ProjectViewsService (MODIFIED FOR PROACTIVE LAYOUT & STRICT CHECK) ---
class ProjectViewsService:
    """
    Service for retrieving server-defined UI layout (fetched proactively)
    and performing strict validation against client capabilities.
    """
    _instance = None

    def __init__(
        self,
        mcp_conn_manager: MCPConnectionManager,
        connection_manager: ConnectionManager
    ):
        """Initializes the service with pre-resolved dependencies."""
        logger.info("--- ENTERING ProjectViewsService.__init__ (Proactive Server UI Layout Mode) ---")
        if not mcp_conn_manager: raise ValueError("MCPConnectionManager dependency is required")
        if not connection_manager: raise ValueError("ConnectionManager dependency is required")
        self.mcp_conn_manager = mcp_conn_manager
        self.connection_manager = connection_manager
        logger.info("Project Views Service initialized (Proactive Server UI Layout Mode)")
        self._initialized = True

    # --- Helper for Strict Capability Validation ---
    def _validate_layout_capabilities(self, element_dict: Dict[str, Any], supported_primitives: Set[str]) -> bool:
        """
        Recursively validates if all primitives ('type') in the layout are supported by the client.
        Returns True if all supported, False otherwise. STRICT: Does not filter/fallback.
        """
        # --- Schema Alignment: Assumes server layout uses 'type' key ---
        primitive_type = element_dict.get('type')
        element_id = element_dict.get('id', 'unknown')

        if not primitive_type:
            logger.error(f"Layout Validation Error: Element '{element_id}' missing 'type' field.")
            return False

        # *** STRICT CHECK ***
        if primitive_type not in supported_primitives:
            logger.error(f"Layout Validation FAILED: Client does not support required primitive '{primitive_type}' for element '{element_id}'.")
            return False # Strict failure - client cannot render this layout

        # Recursively check children
        if 'children' in element_dict and element_dict['children']:
            if not isinstance(element_dict['children'], list):
                 logger.error(f"Layout Validation Error: Element '{element_id}' has invalid 'children' format.")
                 return False
            for child_dict in element_dict['children']:
                if not isinstance(child_dict, dict):
                     logger.error(f"Layout Validation Error: Child of element '{element_id}' is not a dict.")
                     return False
                # If any child fails validation, the whole layout fails
                if not self._validate_layout_capabilities(child_dict, supported_primitives):
                    return False
        return True # All elements (this and children) are supported


    # --- Modified get_project_ui_hierarchy ---
    async def get_project_ui_hierarchy(
        self,
        websocket: WebSocket,
        stream_id: str
    ) -> Optional[UIElement]:
        """
        Retrieves the pre-fetched UI layout, performs strict validation against
        client capabilities, and returns the UI structure.
        """
        # --- APPLY FIX HERE ---
        ws_client_info = f"{WebSocketLogger.get_client_info(websocket)}" # Use static method from logger util
        # --- END FIX ---
        logger.info(f"Getting UI hierarchy for stream '{stream_id}' for client {ws_client_info}...") #

        # --- Pre-checks ---
        if not self.connection_manager or not self.mcp_conn_manager: #
             logger.error(f"[{stream_id}] Cannot generate UI: Service dependencies missing.") #
             return self._create_error_element("Internal Error: Service dependencies missing.") #

        # --- 1. Get Client Capabilities ---
        supported_primitives_list = self.connection_manager.get_supported_primitives(websocket) #
        if not supported_primitives_list: #
            logger.error(f"[{stream_id}] Client {ws_client_info} did not declare supported primitives. Cannot validate layout.") #
            return self._create_error_element("Client capabilities not received. Cannot display UI.") #
        supported_primitives: Set[str] = set(supported_primitives_list) #
        logger.debug(f"[{stream_id}] Client {ws_client_info} supports primitives: {supported_primitives}") #

        # --- 2. Identify Target MCP Server ---
        if not stream_id.startswith("mcp:"): #
             logger.error(f"[{stream_id}] Invalid stream format."); return self._create_error_element("Invalid connection target format.") #
        mcp_server_id = stream_id.split(":", 1)[1] #
        if not mcp_server_id: #
             logger.error(f"[{stream_id}] Missing MCP server ID."); return self._create_error_element("Missing target server ID.") #

        # --- 3. Retrieve Pre-Fetched Server Layout ---
        logger.debug(f"[{stream_id}] Fetching pre-defined UI layout for server '{mcp_server_id}'...") #
        # Use the corrected async accessor method
        server_layout_dict = await self.mcp_conn_manager.get_retrieved_ui_layout(mcp_server_id) #

        if server_layout_dict is None: #
             # This means the server failed connection/layout fetch during startup
             logger.error(f"[{stream_id}] UI layout not available for server '{mcp_server_id}' (check startup logs).") #
             # Check details for a more specific error if available
             details = await self.mcp_conn_manager.get_connection_details(mcp_server_id) #
             error_msg = details.get("error_message") if details else "Layout unavailable" #
             status_msg = details.get("status") if details else "unknown" #
             return self._create_error_element(f"UI not available for '{mcp_server_id}'. Status: {status_msg}. Reason: {error_msg}") #

        logger.info(f"[{stream_id}] Retrieved pre-fetched UI layout from server '{mcp_server_id}'.") #

        # --- 4. Strict Capability Validation ---
        logger.debug(f"[{stream_id}] Performing STRICT validation of layout against client capabilities...") #
        try:
            # --- TODO: Ensure Schema Alignment ---
            # If server sends 'primitive', convert keys to 'type' etc. before this step.
            # aligned_layout_dict = align_server_schema_to_uielement(server_layout_dict)
            aligned_layout_dict = server_layout_dict # Assuming alignment for now #

            is_layout_supported = self._validate_layout_capabilities(aligned_layout_dict, supported_primitives) #

            if not is_layout_supported: #
                # Error logged within _validate_layout_capabilities
                logger.error(f"[{stream_id}] UI layout from server '{mcp_server_id}' requires primitives not supported by client {ws_client_info}.") #
                # *** STRICT FAILURE ***
                return self._create_error_element(f"UI for '{mcp_server_id}' requires capabilities not supported by your client.") #

            logger.info(f"[{stream_id}] Client capabilities sufficient for the received UI layout.") #

        except Exception as val_err: #
             logger.error(f"[{stream_id}] Error during layout capability validation: {val_err}", exc_info=True) #
             return self._create_error_element("Internal error validating UI layout.") #


        # --- 5. Pydantic Validation and Conversion ---
        try:
            # Validate the final dictionary structure and convert to UIElement objects
            root_element = UIElement(**aligned_layout_dict) #
            logger.info(f"[{stream_id}] Successfully validated and converted server layout. Root ID: {root_element.id}") #
            return root_element #
        except ValidationError as e: #
            logger.error(f"[{stream_id}] Pydantic validation failed for server layout: {e}", exc_info=True) #
            return self._create_error_element("Received invalid UI layout structure from the server.") #
        except Exception as conv_err: #
            logger.error(f"[{stream_id}] Error converting layout dict to UIElement: {conv_err}", exc_info=True) #
            return self._create_error_element("Internal error processing UI layout structure.") #


    def _create_error_element(self, message: str) -> UIElement:
        """Creates a simple TextView UI element to display an error message.""" #
        error_id = f"error-display-{abs(hash(message + str(time.time())))}" #
        logger.debug(f"Creating error UI element id='{error_id}' msg='{message}'") #
        return UIElement( #
            id=error_id, type='TextView', # Assume TextView is always supported for errors #
            config={"initialText": f"Error: {message}", "variant": "error"} #
        )