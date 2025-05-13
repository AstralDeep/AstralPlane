import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.orm import Session  # Make sure Session is imported

from app.config import settings
from app.database import get_db, create_tables, Base  # get_db is your context manager
from app.services import mcp_config_crud_service
from app.services.connection_manager import ConnectionManager
from app.services.mcp_connection_manager import MCPConnectionManager
from app.services.project_service import ProjectViewsService
from app.models.mcp_server_config_model import MCPServerConfig  # Ensure models are known to Base

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
	logger.info(f"Application startup: {settings.APP_NAME} v{app.version}")

	# 1. Create database tables
	try:
		logger.info(f"Ensuring database tables exist. Known tables: {Base.metadata.tables.keys()}")
		create_tables()
		logger.info("Database tables checked/created successfully.")
	except Exception as e:
		logger.error(f"FATAL: Error creating database tables: {e}", exc_info=True)
		raise RuntimeError("Could not initialize database tables.") from e

	# 2. Instantiate ConnectionManager for UI clients
	ui_manager_instance = ConnectionManager()
	app.state.ui_connection_manager = ui_manager_instance
	logger.info("UI ConnectionManager instance created and stored in app.state.")

	# 3. Initialize MCPConnectionManager instance and store on app.state
	mcp_manager_instance = MCPConnectionManager(
		settings=settings,
		connection_manager=app.state.ui_connection_manager
	)
	app.state.mcp_connection_manager = mcp_manager_instance
	logger.info("MCPConnectionManager instance created and stored in app.state.")

	# 4. Load MCP server configurations from DB into the manager
	# --- CORRECTED DATABASE SESSION HANDLING ---
	try:
		# Use a 'with' statement to correctly manage the DB session lifecycle
		with get_db() as db_for_startup:  # db_for_startup will be the yielded Session object
			logger.info("Loading MCP server configurations from database using session from get_db()...")
			all_db_configs = mcp_config_crud_service.get_mcp_server_configs(
				db_for_startup,
				limit=1000,  # Consider making this configurable if needed
				only_active=None  # Load all (active and inactive)
			)

			if hasattr(app.state, 'mcp_connection_manager') and app.state.mcp_connection_manager:
				await app.state.mcp_connection_manager.initialize_servers_from_db(all_db_configs)
				logger.info(f"MCPConnectionManager initialized with data for {len(all_db_configs)} servers.")
			else:
				# This should ideally not happen if step 3 was successful
				logger.error("CRITICAL: MCPConnectionManager not found on app.state during DB config loading.")

	except Exception as e:
		logger.error(f"Failed to load MCP server configurations during startup: {e}", exc_info=True)
	# The 'finally' block to close db_for_startup is automatically handled by the 'with' statement
	# when used with your @contextmanager get_db().
	# --- END OF CORRECTED DATABASE SESSION HANDLING ---

	# 5. Initialize ProjectViewsService
	if not hasattr(app.state, 'project_views_service') or not app.state.project_views_service:
		try:
			logger.info("Initializing ProjectViewsService with dependencies from app.state...")
			if not hasattr(app.state, 'mcp_connection_manager') or not hasattr(app.state, 'ui_connection_manager'):
				logger.error("Cannot initialize ProjectViewsService: core manager dependencies not found on app.state.")
			else:
				pvs_instance = ProjectViewsService(
					mcp_conn_manager=app.state.mcp_connection_manager,
					connection_manager=app.state.ui_connection_manager
				)
				app.state.project_views_service = pvs_instance
				logger.info("ProjectViewsService initialized and stored in app.state.")
		except Exception as e:
			logger.error(f"Failed to initialize ProjectViewsService: {e}", exc_info=True)

	logger.info("Application startup complete.")
	yield  # Application runs here

	# --- Shutdown ---
	logger.info("Application shutdown sequence started...")
	if hasattr(app.state, 'mcp_connection_manager') and app.state.mcp_connection_manager:
		logger.info("Shutting down MCP connections...")
		await app.state.mcp_connection_manager.cleanup_all_connections()
		logger.info("MCP connections gracefully closed.")
	logger.info("Application shutdown complete.")
