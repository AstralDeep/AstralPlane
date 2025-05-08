import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.services.connection_manager import ConnectionManager
from app.services.mcp_connection_manager import MCPConnectionManager
from app.services.project_service import ProjectViewsService

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info(f"Starting up {settings.APP_NAME}...")

    # Initialize ConnectionManager
    connection_manager_instance = ConnectionManager()
    app_instance.state.connection_manager = connection_manager_instance

    # Initialize MCPConnectionManager
    try:
        mcp_conn_manager_instance = MCPConnectionManager(
            settings=settings,
            connection_manager=connection_manager_instance
        )
        app_instance.state.mcp_connection_manager = mcp_conn_manager_instance

        # Proactively connect to MCP servers
        connect_tasks = []
        for server_id, config in mcp_conn_manager_instance.server_configs.items():
            connect_tasks.append(
                asyncio.create_task(
                    mcp_conn_manager_instance.connect_and_prepare_server(server_id, config),
                    name=f"mcp_connect_{server_id}"
                )
            )
        if connect_tasks:
            done, pending = await asyncio.wait(connect_tasks, timeout=30.0)
            for task in pending:
                task.cancel()

    except Exception as e:
        logger.critical(f"Failed to initialize MCPConnectionManager: {e}", exc_info=True)
        app_instance.state.mcp_connection_manager = None

    # Initialize ProjectViewsService
    try:
        cm = app_instance.state.connection_manager
        mcp_cm = app_instance.state.mcp_connection_manager
        if cm and mcp_cm:
            pvs = ProjectViewsService(mcp_conn_manager=mcp_cm, connection_manager=cm)
            app_instance.state.project_views_service = pvs
    except Exception as e:
        logger.error(f"Failed to initialize ProjectViewsService: {e}", exc_info=True)
        app_instance.state.project_views_service = None

    yield  # App is running here

    # Cleanup
    try:
        if hasattr(app_instance.state, "mcp_connection_manager") and app_instance.state.mcp_connection_manager:
            await app_instance.state.mcp_connection_manager.cleanup_all_connections()
    except Exception as e:
        logger.error(f"Error cleaning up MCP connections: {e}", exc_info=True)

    logger.info(f"{settings.APP_NAME} shutdown complete.")
