import asyncio
import json
import logging
import os
import time
from typing import List, Dict, Any, Optional

# MCP Imports
import mcp.types as types
import uvicorn
from mcp.server.fastmcp import FastMCP
from pydantic.networks import AnyUrl  # Needed for type hinting potentially

from app.config import settings
from app.utils.logging_config import configure_logging

# --- Configuration ---
HOST = "127.0.0.1"
PORT = 8124
SERVER_NAME = "mcp_async_demo"

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
configure_logging(log_level=log_level, log_to_file=settings.DEBUG, log_dir="logs")
logger = logging.getLogger("app.mcp_servers.mcp_async_demo")
logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")

# --- FastMCP Server Instance ---
mcp_async = FastMCP(
	name=SERVER_NAME,
	host=HOST,
	port=PORT,
	log_level="DEBUG",  # Keep DEBUG for tracing
	dependencies=["uvicorn"]  # Example: Add dependencies if needed
)

# --- Track Active Tasks for Cancellation ---
active_tasks: Dict[str, asyncio.Task] = {}
active_tasks_lock = asyncio.Lock()


# --- UI Structure Function (Removed Status View) ---
def construct_async_ui_layout() -> dict:
	"""Constructs the UI layout dictionary with explanations and result views."""
	logger.info(f"[{SERVER_NAME}] Constructing Final Async Demo UI Layout (Rev 10)...")
	# Define base binding strings
	base_binding = f"mcp_stream:{SERVER_NAME}"
	log_binding = f"{base_binding}:log_messages"
	progress_binding = f"{base_binding}:progress_updates"

	# Define specific result bindings based on action IDs
	send_logs_result_binding = f"{base_binding}:send_log_messages_result"
	long_task_result_binding = f"{base_binding}:long_task_result"
	tool_list_change_result_binding = f"{base_binding}:trigger_tool_list_change_result"
	res_list_change_result_binding = f"{base_binding}:trigger_resource_list_change_result"
	prompt_list_change_result_binding = f"{base_binding}:trigger_prompt_list_change_result"
	trigger_resource_update_result_binding = f"{base_binding}:trigger_resource_update_result"

	return {
		"id": "async-root-layout", "type": "StackLayout",
		"config": {"direction": "vertical", "padding": "16px", "gap": "16px"},
		"children": [
			{"id": "title", "type": "TextView",
			 "config": {"initialText": "MCP Async Demo Server", "variant": "headline"}},
			{"id": "explanation_main", "type": "TextView", "config": {
				"initialText": "This server demonstrates asynchronous MCP features. Use buttons to trigger actions. "
							   "Observe feedback in 'Outputs & Logs' section (Logs, Progress, Results). Sending "
							   "standard notifications using SDK helper methods.",
				"variant": "body"}},  # Updated text
			{"id": "actions-layout", "type": "StackLayout",
			 "config": {"direction": "horizontal", "gap": "10px", "wrap": "wrap",
						"style": {"marginTop": "10px", "marginBottom": "20px"}},
			 "children": [
				 {"id": "send-logs-btn", "type": "Button", "config": {"label": "Send Logs"},
				  "actionId": "send_log_messages"},
				 {"id": "long-task-btn", "type": "Button", "config": {"label": "Start Long Task"},
				  "actionId": "long_task"},
				 {"id": "trigger-tool-list-btn", "type": "Button", "config": {"label": "Trigger Tool List Change"},
				  "actionId": "trigger_tool_list_change"},
				 {"id": "trigger-res-list-btn", "type": "Button", "config": {"label": "Trigger Resource List Change"},
				  "actionId": "trigger_resource_list_change"},
				 {"id": "trigger-prompt-list-btn", "type": "Button", "config": {"label": "Trigger Prompt List Change"},
				  "actionId": "trigger_prompt_list_change"},
			 ]},
			{"id": "res-update-section", "type": "StackLayout",
			 "config": {"direction": "vertical", "gap": "8px", "padding": "10px", "border": True,
						"style": {"borderColor": "#e0e0e0"}},
			 "children": [
				 {"id": "res-update_explanation", "type": "TextView", "config": {
					 "initialText": "Simulate Resource Notification: Type resource ID, click button. Server logs "
									"action locally and attempts outward 'ResourceUpdated' notification using session "
									"helper. Direct result appears below.",
					 "variant": "caption"}},  # Updated text
				 {"id": "res-update-controls", "type": "StackLayout",
				  "config": {"direction": "horizontal", "gap": "10px", "align_items": "flex-end"},
				  "children": [
					  {"id": "resource-uri-input", "type": "InputField",
					   "config": {"placeholder": "resource:uri/to/update", "label": "Resource URI"}},
					  {"id": "trigger-res-update-btn", "type": "Button",
					   "config": {"label": "Trigger Resource Update", "valueSourceElementIds": ["resource-uri-input"]},
					   "actionId": "trigger_resource_update"},
				  ]},
			 ]
			 },
			# --- Outputs Section (Status TextView Removed) ---
			{"id": "outputs-layout", "type": "StackLayout", "config": {"direction": "vertical", "gap": "10px",
																	   "style": {"marginTop": "20px",
																				 "border": "1px solid #ccc",
																				 "padding": "10px"}},
			 "children": [
				 {"id": "outputs_title", "type": "TextView",
				  "config": {"initialText": "Outputs & Logs", "variant": "titleSmall"}},
				 {"id": "progress-text", "type": "TextView",
				  "config": {"initialText": "Progress updates appear here.", "variant": "body"},
				  "updateBinding": progress_binding},
				 # -- TextViews for specific tool results --
				 {"id": "trigger-resource-update-result-view", "type": "TextView",
				  "config": {"initialText": "Resource Update Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": trigger_resource_update_result_binding},
				 {"id": "send-logs-result-view", "type": "TextView",
				  "config": {"initialText": "Send Logs Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": send_logs_result_binding},
				 {"id": "long-task-result-view", "type": "TextView",
				  "config": {"initialText": "Long Task Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": long_task_result_binding},
				 {"id": "tool-list-change-result-view", "type": "TextView",
				  "config": {"initialText": "Tool List Change Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": tool_list_change_result_binding},
				 {"id": "res-list-change-result-view", "type": "TextView",
				  "config": {"initialText": "Resource List Change Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": res_list_change_result_binding},
				 {"id": "prompt-list-change-result-view", "type": "TextView",
				  "config": {"initialText": "Prompt List Change Result...", "variant": "body",
							 "style": {"fontStyle": "italic", "color": "grey"}},
				  "updateBinding": prompt_list_change_result_binding},
				 # --- End Result TextViews ---
				 {"id": "log-view", "type": "LogView",
				  "config": {"title": "Async Server Logs", "lineFormat": "json", "autoScroll": True,
							 "style": {"height": "300px", "marginTop": "10px"}}, "updateBinding": log_binding},
			 ]},
		]
	}


# --- MCP Tools ---

@mcp_async.tool(
	name="get_ui_layout",
	description="Retrieves the UI layout configuration for the async demo server."
)
async def get_ui_layout() -> List[types.TextContent]:
	"""MCP Tool: Returns the UI layout as JSON."""
	ctx = mcp_async.get_context()  # FastMCP Context
	await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Called.")
	ui_layout_dict = construct_async_ui_layout()
	try:
		ui_layout_json = json.dumps(ui_layout_dict)
		await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Serialized layout.")
		return [types.TextContent(type="text", text=ui_layout_json)]
	except TypeError as json_err:
		error_log_message = f"Failed to serialize UI layout: {type(json_err).__name__}"
		await ctx.error(error_log_message)
		error_message = f"Error: Could not serialize UI layout - {json_err}"
		return [types.TextContent(type="text", text=error_message)]


@mcp_async.tool(
	name="send_log_messages",
	description="Sends example log messages to the LogView."
)
async def send_log_messages() -> List[types.TextContent]:
	"""MCP Tool: Sends INFO, WARNING, ERROR logs via context logger."""
	ctx = mcp_async.get_context()  # FastMCP Context
	timestamp = time.strftime('%H:%M:%S')
	completion_log_message = f"[{timestamp}] Sent INFO, WARNING, ERROR logs to LogView."

	await ctx.info("This is an INFO level log message.")
	await asyncio.sleep(0.1)
	await ctx.warning("This is a WARNING level log message.")
	await asyncio.sleep(0.1)
	await ctx.error("This is an ERROR level log message.")

	await ctx.info(completion_log_message)  # Log completion status

	return [types.TextContent(type="text", text="Log messages action completed.")]


@mcp_async.tool(
	name="long_task",
	description="Simulates a long task with progress updates and cancellation."
)
async def long_task() -> List[types.TextContent]:
	"""MCP Tool: Simulates a long task. Updates Progress via report_progress, Logs status."""
	ctx = mcp_async.get_context()  # FastMCP Context
	stored_request_id = ctx.request_id
	current_task = asyncio.current_task()
	timestamp = time.strftime('%H:%M:%S')
	start_status_message = f"[{timestamp}] Starting long task (ID: {str(stored_request_id)[-6:]}...). Check Progress & Logs."

	async with active_tasks_lock:
		if stored_request_id in active_tasks:
			await ctx.error(f"Task {stored_request_id} is already running.")
			return [types.TextContent(type="text", text="Error: Task already running.")]
		active_tasks[stored_request_id] = current_task
		await ctx.info(f"Tracking task {stored_request_id}")

	await ctx.info(start_status_message)  # Log start status

	steps = 10
	total_steps = float(steps)  # Define total for report_progress
	try:
		for i in range(steps):
			# --- Check for cancellation BEFORE doing work/reporting progress ---
			if current_task.cancelled():
				await ctx.info(f"Task {stored_request_id} cancellation detected before step {i + 1}.")
				raise asyncio.CancelledError()  # Raise standard CancelledError
			await asyncio.sleep(0)  # Yield control briefly

			step_info = f"Long task step {i + 1}/{steps}"
			await ctx.info(step_info)
			current_progress = float(i + 1)  # Current progress value

			# --- Use correct signature for report_progress ---
			try:
				await ctx.report_progress(progress=current_progress, total=total_steps)
			except AttributeError:
				await ctx.error("Error calling report_progress: Method not available on context? Check SDK version.")
			except Exception as report_err:
				await ctx.error(f"Error calling report_progress: {type(report_err).__name__}")

			# --- Simulate self-cancellation attempt ---
			if i == 4:
				await ctx.warning("Server simulating self-cancellation for this task...")
				await asyncio.sleep(0.5)
				# Use generic send_notification for Cancelled, as it requires specific params
				# and doesn't have a dedicated helper method in ServerSession.
				if hasattr(ctx, 'session') and hasattr(ctx.session, 'send_notification'):
					try:
						# Keep params for Cancelled notification as it requires requestId
						await ctx.session.send_notification(
							types.CancelledNotification(
								method="notifications/cancelled",
								params=types.CancelledNotificationParams(requestId=stored_request_id)
							)
						)
						await ctx.info(f"Attempted server-initiated cancellation notification for {stored_request_id}")
					except AttributeError:
						await ctx.error("NotificationError: ctx.session.send_notification method not found.")
					except Exception as e:
						await ctx.error(f"NotificationError: Failed sending outward cancellation ({type(e).__name__}).")
				else:
					await ctx.warning(
						"No active session or send_notification method available, cannot send server cancellation.")

			# --- Simulate work AFTER reporting progress for the step ---
			await asyncio.sleep(1.5)

		completion_timestamp = time.strftime('%H:%M:%S')
		completion_message = f"[{completion_timestamp}] Long task (ID: {str(stored_request_id)[-6:]}...) completed successfully."
		await ctx.info(completion_message)  # Log completion status
		return [types.TextContent(type="text", text="Long task finished.")]

	except asyncio.CancelledError:
		cancel_timestamp = time.strftime('%H:%M:%S')
		cancel_message = f"[{cancel_timestamp}] Long task (ID: {str(stored_request_id)[-6:]}...) was cancelled."
		await ctx.warning(cancel_message)  # Log cancel status
		raise  # Re-raise the standard asyncio.CancelledError

	except Exception as e:
		error_timestamp = time.strftime('%H:%M:%S')
		error_message = f"[{error_timestamp}] Task (ID: {str(stored_request_id)[-6:]}...) failed: {type(e).__name__}"
		await ctx.error(error_message)  # Log error status
		raise

	finally:
		async with active_tasks_lock:
			if stored_request_id in active_tasks:
				del active_tasks[stored_request_id]
				await ctx.info(f"Stopped tracking task {stored_request_id}")


@mcp_async.tool(name="trigger_tool_list_change", description="Logs action locally & attempts outward notification.")
async def trigger_tool_list_change() -> List[types.TextContent]:
	"""MCP Tool: Logs status locally & attempts outward notification using session helper."""
	ctx = mcp_async.get_context()  # FastMCP Context
	timestamp = time.strftime('%H:%M:%S')
	status_message = f"[{timestamp}] 'Tool List Change' action executed locally."
	await ctx.info(status_message + " (Attempting outward notification via session helper...)")  # Updated log

	if hasattr(ctx, 'session') and hasattr(ctx.session, 'send_tool_list_changed'):
		try:
			# --- Use the specific helper method ---
			await ctx.session.send_tool_list_changed()
			await ctx.info("Attempted notifications/tools/list_changed notification outward via helper.")  # Changed log
		except AttributeError:
			await ctx.error("NotificationError: ctx.session.send_tool_list_changed method not found.")
		except Exception as e:
			await ctx.error(f"NotificationError: Failed sending outward ToolListChanged ({type(e).__name__}).")
	else:
		await ctx.warning("No session or send_tool_list_changed method available for outward ToolListChanged.")

	return [types.TextContent(type="text", text="ToolListChanged action completed.")]


@mcp_async.tool(name="trigger_resource_list_change", description="Logs action locally & attempts outward notification.")
async def trigger_resource_list_change() -> List[types.TextContent]:
	"""MCP Tool: Logs status locally & attempts outward notification using session helper."""
	ctx = mcp_async.get_context()  # FastMCP Context
	timestamp = time.strftime('%H:%M:%S')
	status_message = f"[{timestamp}] 'Resource List Change' action executed locally."
	await ctx.info(status_message + " (Attempting outward notification via session helper...)")  # Updated log

	if hasattr(ctx, 'session') and hasattr(ctx.session, 'send_resource_list_changed'):
		try:
			# --- Use the specific helper method ---
			await ctx.session.send_resource_list_changed()
			await ctx.info(
				"Attempted notifications/resources/list_changed notification outward via helper.")  # Changed log
		except AttributeError:
			await ctx.error("NotificationError: ctx.session.send_resource_list_changed method not found.")
		except Exception as e:
			await ctx.error(f"NotificationError: Failed sending outward ResourceListChanged ({type(e).__name__}).")
	else:
		await ctx.warning("No session or send_resource_list_changed method available for outward ResourceListChanged.")

	return [types.TextContent(type="text", text="ResourceListChanged action completed.")]


@mcp_async.tool(name="trigger_prompt_list_change", description="Logs action locally & attempts outward notification.")
async def trigger_prompt_list_change() -> List[types.TextContent]:
	"""MCP Tool: Logs status locally & attempts outward notification using session helper."""
	ctx = mcp_async.get_context()  # FastMCP Context
	timestamp = time.strftime('%H:%M:%S')
	status_message = f"[{timestamp}] 'Prompt List Change' action executed locally."
	await ctx.info(status_message + " (Attempting outward notification via session helper...)")  # Updated log

	if hasattr(ctx, 'session') and hasattr(ctx.session, 'send_prompt_list_changed'):
		try:
			# --- Use the specific helper method ---
			await ctx.session.send_prompt_list_changed()
			await ctx.info(
				"Attempted notifications/prompts/list_changed notification outward via helper.")  # Changed log
		except AttributeError:
			await ctx.error("NotificationError: ctx.session.send_prompt_list_changed method not found.")
		except Exception as e:
			await ctx.error(f"NotificationError: Failed sending outward PromptListChanged ({type(e).__name__}).")
	else:
		await ctx.warning("No session or send_prompt_list_changed available for outward PromptListChanged.")

	return [types.TextContent(type="text", text="PromptListChanged action completed.")]


@mcp_async.tool(name="trigger_resource_update",
				description="Logs action with URI locally & attempts outward notification.")
async def trigger_resource_update(uri: str) -> List[types.TextContent]:
	"""MCP Tool: Logs action with URI locally & attempts outward notification using session helper."""
	ctx = mcp_async.get_context()  # FastMCP Context
	timestamp = time.strftime('%H:%M:%S')

	if not uri:
		error_message = f"[{timestamp}] Error: Missing URI for Resource Update."
		await ctx.error("URI parameter is required for trigger_resource_update.")
		# No need to log twice await ctx.error(error_message) # Log error status
		return [types.TextContent(type="text", text="Error: Missing URI parameter.")]

	status_message = f"[{timestamp}] 'Resource Update' action executed locally for URI: '{uri}'"
	await ctx.info(status_message + " (Attempting outward notification via session helper...)")  # Updated log

	if hasattr(ctx, 'session') and hasattr(ctx.session, 'send_resource_updated'):
		try:
			# --- Use the specific helper method, passing the URI ---
			# Convert string URI to AnyUrl if required by the helper method signature
			# (Assuming AnyUrl can parse common URI strings)
			try:
				pydantic_uri = AnyUrl(uri)
			except Exception:
				await ctx.error(f"Invalid URI format: {uri}")
				return [types.TextContent(type="text", text=f"Error: Invalid URI format '{uri}'.")]

			await ctx.session.send_resource_updated(uri=pydantic_uri)
			await ctx.info(
				f"Attempted notifications/resources/updated notification outward for URI: {uri} via helper.")  # Changed log
		except AttributeError:
			await ctx.error("NotificationError: ctx.session.send_resource_updated method not found.")
		except Exception as e:
			simple_error_msg = f"NotificationError: Failed sending outward ResourceUpdated ({type(e).__name__})."
			await ctx.error(simple_error_msg)
	else:
		await ctx.warning(
			f"No session or send_resource_updated method available for outward ResourceUpdated for URI: {uri}")

	return [types.TextContent(type="text", text=f"Resource update action for '{uri}' completed.")]


# --- Client->Server Notification Handlers ---

async def handle_roots_changed(params: Optional[Dict[str, Any]]):
	"""Handles 'notifications/roots/list_changed' from client."""
	ctx = mcp_async.get_context()  # FastMCP Context
	roots = params.get('roots', []) if params else []
	timestamp = time.strftime('%H:%M:%S')
	log_message = f"[{timestamp}] Server received 'roots/list_changed' notification from client. Roots: {len(roots)}"
	await ctx.info(log_message)
	logger.info(f"[{SERVER_NAME} Handler] Client reported roots: {roots}")


async def handle_client_cancellation(params: Optional[Dict[str, Any]]):
	"""Handles 'notifications/cancelled' from client."""
	ctx = mcp_async.get_context()  # FastMCP Context
	request_id = params.get('requestId') if params else None
	timestamp = time.strftime('%H:%M:%S')
	# Ensure request_id is treated as string for slicing if it exists
	request_id_str = str(request_id) if request_id is not None else None
	status_prefix = f"[{timestamp}] Server received 'cancelled' notification from client for Request ID: {request_id_str[-6:] if request_id_str else 'N/A'}..."
	await ctx.info(f"Received client cancellation request for ID: {request_id}")

	if not request_id:
		error_message = f"[{timestamp}] Error: Client cancellation notification missing requestId."
		await ctx.error(error_message)
		return

	await ctx.info(status_prefix + " Processing...")  # Log initial status
	final_status = status_prefix  # Initialize final_status

	async with active_tasks_lock:
		task_to_cancel = active_tasks.get(request_id)
		if task_to_cancel:
			await ctx.info(f"Found active task for {request_id}. Attempting cancellation...")
			cancelled = task_to_cancel.cancel()
			if cancelled:
				final_status += " Cancellation signal sent."
			# Note: Task might take time to actually process the cancellation
			else:
				final_status += " Task already done or cancelling?"
				await ctx.warning(
					f"task.cancel() returned False for {request_id}. Task may be done or already cancelled.")
				# Clean up immediately if cancel returns False, as it implies the task isn't running/cancellable
				if request_id in active_tasks: del active_tasks[request_id]
		else:
			final_status += " Ignored (Task not found/active)."
			await ctx.warning(f"No active task found to cancel for request ID: {request_id}")

	# Log final status determined inside lock
	await ctx.info(final_status)


# --- Register Notification Handlers ---
# Registering handlers directly via internal dict access.
# This is necessary if the FastMCP layer or the underlying lowlevel server
# does not provide specific decorators (@server.cancelled_notification(), etc.)
# for all desired notification types in this SDK version.
try:
	# Ensure the internal server object exists
	if not hasattr(mcp_async, '_mcp_server'):
		raise AttributeError("Internal '_mcp_server' not found on FastMCP instance.")

	# Register RootsListChanged handler
	if hasattr(types, "RootsListChangedNotification"):
		mcp_async._mcp_server.notification_handlers[types.RootsListChangedNotification] = handle_roots_changed
		logger.info(f"[{SERVER_NAME}] Registered handler for notifications/roots/list_changed")
	else:
		logger.error(f"[{SERVER_NAME}] mcp.types.RootsListChangedNotification not found. Cannot register handler.")

	# Register CancelledNotification handler
	if hasattr(types, "CancelledNotification"):
		mcp_async._mcp_server.notification_handlers[types.CancelledNotification] = handle_client_cancellation
		logger.info(f"[{SERVER_NAME}] Registered handler for notifications/cancelled (Client Initiated)")
	else:
		logger.error(f"[{SERVER_NAME}] mcp.types.CancelledNotification not found. Cannot register handler.")

except AttributeError as e:
	logger.error(
		f"[{SERVER_NAME}] Failed to access internal _mcp_server or notification_handlers dictionary: {e}. SDK structure might differ.")
except KeyError as e:
	logger.error(
		f"[{SERVER_NAME}] KeyError during notification handler registration: {e}. SDK might be incomplete or changed.")
except NameError:
	logger.error(f"[{SERVER_NAME}] Failed to find 'types' module while registering handlers.")
except Exception as e:
	logger.error(f"[{SERVER_NAME}] Unexpected error during notification handler registration: {e}", exc_info=True)

# --- Main execution block ---
if __name__ == "__main__":
	logger.info(f"--- Starting FastMCP Async Demo server ({SERVER_NAME}) ---")
	starlette_app = mcp_async.sse_app()
	if starlette_app is None:
		logger.critical(f"CRITICAL ERROR: mcp_async.sse_app() returned None. Cannot start server.")
		exit(1)

	run_host = mcp_async.settings.host
	run_port = mcp_async.settings.port
	log_level = mcp_async.settings.log_level.lower()

	logger.info(f"Attempting to listen on: http://{run_host}:{run_port}")
	logger.info("This server demonstrates MCP async features. Check Logs, Progress, and Results views for feedback.")
	logger.info("Using specific SDK session helper methods for standard notifications...")
	logger.info("----------------------------------------------------")

	try:
		uvicorn.run(
			starlette_app,
			host=run_host,
			port=run_port,
			log_level=log_level
		)
	except Exception as e:
		logger.critical(f"Failed to run Uvicorn server: {e}", exc_info=True)
		exit(1)
