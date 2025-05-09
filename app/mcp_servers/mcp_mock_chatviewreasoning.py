import asyncio
import json
import logging
import os
import time
from typing import List

# --- Import MCP libs ---
import mcp.types as types
import uvicorn
from mcp.server.fastmcp import FastMCP

from app.config import settings
from app.utils.logging_config import configure_logging

# --- Configuration ---
HOST = "127.0.0.1"
PORT = 8125
SERVER_NAME = "mcp_mock_chatviewreasoning"

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
configure_logging(log_level=log_level, log_to_file=settings.DEBUG, log_dir="logs")
logger = logging.getLogger("app.mcp_servers.mcp_mock_chatviewreasoning")
logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")

# --- FastMCP Server Instance ---
mcp_server = FastMCP(
	name=SERVER_NAME,
	host=HOST,
	port=PORT,
	log_level="DEBUG",
	dependencies=["uvicorn"]
)


# --- UI Structure Definition (Layout Updated) ---
def construct_reasoning_ui_layout() -> dict:
	"""Constructs the UI layout with a 2/3 chat + input, 1/3 reasoning log split."""
	logger.info(f"[{SERVER_NAME}] Constructing Updated 2/3 + 1/3 Layout...")
	base_binding = f"mcp_stream:{SERVER_NAME}"
	chat_result_binding = f"{base_binding}:reasoning_chat_query_result"
	reasoning_log_binding = f"{base_binding}:log_messages"
	input_field_id = "chat-input-field"
	send_button_id = "send-button"
	reasoning_action_id = "reasoning_chat_query"

	# Define the Input Area structure separately for clarity
	input_area = {
		"id": "input-area", "type": "StackLayout",
		"config": {
			"direction": "horizontal",
			"gap": "8px",
			"align_items": "flex-end",
			# Use padding instead of margin-top for spacing within the column
			"style": {"padding": "10px 0 0 0"}  # Add padding top
		},
		"children": [
			{"id": input_field_id, "type": "InputField", "config": {
				"placeholder": "Type your query...",
				"label": "Query",
				"style": {"flexGrow": 1},
				"enterKeyAction": {"isEnabled": True, "actionId": reasoning_action_id,
								   "targetElementId": send_button_id}
			}},
			{"id": send_button_id, "type": "Button", "actionId": reasoning_action_id, "config": {
				"label": "Send",
				"variant": "primary",
				"valueSourceElementIds": [input_field_id],
				"frontendActions": [
					{"type": "echoToView", "sourceElementId": input_field_id, "targetBinding": chat_result_binding,
					 "role": "user"},
					{"type": "clearElement", "targetElementId": input_field_id}
				]
			}},
		]
	}

	# Define the Left Column (Chat + Input)
	left_column = {
		"id": "left-column", "type": "StackLayout",
		"config": {
			"direction": "vertical",
			"gap": "0px",  # No gap between chat and input area
			"style": {
				"flex": "2",  # <<< Takes 2/3 width
				"height": "100%",  # Fill height of parent
				"display": "flex",
				"flexDirection": "column"
			}
		},
		"children": [
			# Chat View takes up available space
			{"id": "chat-display", "type": "ChatViewBasic",
			 "config": {"title": "Chat Conversation", "autoScroll": True,
						"style": {"flexGrow": 1, "minHeight": "100px"}},  # Let it grow
			 "updateBinding": chat_result_binding,
			 "children": None},
			# Input Area at the bottom of this column
			input_area
		]
	}

	# Define the Right Column (Reasoning Log)
	right_column_content = {
		"id": "reasoning-log-view", "type": "McpStructuredLogView",  # Use the custom primitive
		"config": {"title": "Reasoning Steps", "autoScroll": True, "style": {
			"flex": "1",  # <<< Takes 1/3 width
			"height": "100%"  # Fill height
		}},
		"updateBinding": reasoning_log_binding,
	}

	# Define the Root Layout
	return {
		"id": "reasoning-root-layout", "type": "StackLayout",
		"config": {
			"direction": "vertical",  # Overall layout is vertical (Title + Main Area)
			"padding": "16px",
			"gap": "16px",
			"style": {"height": "100vh", "display": "flex", "flexDirection": "column"}
		},
		"children": [
			{"id": "title", "type": "TextView",
			 "config": {"initialText": "MCP Reasoning Chat Demo", "variant": "headline"}},
			{"id": "explanation", "type": "TextView",
			 "config": {"initialText": "Enter a query. Assistant shows reasoning (right) and final answer (left).",
						"variant": "body"}},  # Simplified explanation

			# Main Area: Horizontal split for Left and Right columns
			{"id": "main-columns", "type": "StackLayout",
			 "config": {
				 "direction": "horizontal",
				 "gap": "16px",
				 "style": {
					 "flexGrow": 1,  # Allow this row to fill vertical space
					 "overflow": "hidden",  # Prevent content overflow issues
					 "height": "0"  # Needed for flexGrow to work correctly with overflow
				 }
			 },
			 "children": [
				 left_column,
				 right_column_content
			 ]
			 }
			# Input area is now *inside* the left_column
		]
	}


# --- MCP Tools (reasoning_chat_query and get_ui_layout remain the same as previous corrected version) ---

@mcp_server.tool(
	name="get_ui_layout",
	description="Retrieves the UI layout configuration for the reasoning chat demo."
)
async def get_ui_layout() -> List[types.TextContent]:
	"""MCP Tool: Returns the UI layout as JSON."""
	ctx = mcp_server.get_context()
	await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Called.")
	ui_layout_dict = construct_reasoning_ui_layout()
	try:
		ui_layout_json = json.dumps(ui_layout_dict)
		await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Serialized layout.")
		return [types.TextContent(type="text", text=ui_layout_json)]
	except TypeError as json_err:
		await ctx.error(f"Failed to serialize UI layout: {type(json_err).__name__}")
		return [types.TextContent(type="text", text=f"Error: Could not serialize UI layout - {json_err}")]


@mcp_server.tool(
	name="reasoning_chat_query",
	description="Receives user query, simulates reasoning with streamed logs, and returns a final answer."
)
async def reasoning_chat_query(query: str) -> List[types.TextContent]:
	"""
	MCP Tool: Simulates reasoning process (Corrected Bindings - No Cancellation).
	- Streams 'thinking' steps via context logger (implicitly targeting reasoning_log_binding).
	- Returns the final answer as TextContent (implicitly targeting chat_result_binding).
	"""
	ctx = mcp_server.get_context()
	request_id = ctx.request_id
	timestamp = time.strftime('%H:%M:%S')

	await ctx.info(f"[{timestamp} Req: {str(request_id)[-6:]}] Received query: '{query}'")
	await ctx.info(f"System: Started reasoning task.")

	try:
		await ctx.info("Thinking...")
		await asyncio.sleep(0.5)

		reasoning_steps = [
			"Analyzing query...", f"Checking '{query[:20]}...'", "Consulting KB A.",
			"Consulting KB B.", "Synthesizing...", "Selecting best option.", "Formatting."
		]  # Shortened for brevity
		num_steps = len(reasoning_steps)

		for i, step in enumerate(reasoning_steps):
			await asyncio.sleep(0)
			log_content = f"Step {i + 1}/{num_steps}: {step}"
			await ctx.info(log_content)
			await asyncio.sleep(0.8)  # Slightly faster simulation

		final_response = f"The answer regarding '{query}' is most likely 42."
		await ctx.info("Reasoning complete. Preparing final answer.")
		await asyncio.sleep(0.5)

		completion_timestamp = time.strftime('%H:%M:%S')
		await ctx.info(f"[{completion_timestamp} Req: {str(request_id)[-6:]}] Reasoning completed.")

		return [types.TextContent(type="text", text=final_response)]

	except Exception as e:
		error_timestamp = time.strftime('%H:%M:%S')
		error_message = f"[{error_timestamp} Req: {str(request_id)[-6:]}] Reasoning failed: {type(e).__name__}"
		await ctx.error(error_message, exc_info=False)
		return [types.TextContent(type="text", text=f"Error processing: {type(e).__name__}")]


# --- Main execution block (remains the same) ---
if __name__ == "__main__":
	logger.info(f"--- Starting FastMCP Reasoning Chat server ({SERVER_NAME}) ---")

	starlette_app = mcp_server.sse_app()

	if starlette_app is None:
		logger.critical(f"Failed to get Starlette app from MCP server. Cannot start.")
		exit(1)

	run_host = mcp_server.settings.host
	run_port = mcp_server.settings.port
	log_level = mcp_server.settings.log_level.lower()

	logger.info(f"Attempting to listen on: http://{run_host}:{run_port}")
	logger.info("This server demonstrates streaming reasoning logs (Updated Layout).")
	logger.info("----------------------------------------------------")

	try:
		uvicorn.run(starlette_app, host=run_host, port=run_port, log_level=log_level)
	except Exception as e:
		logger.critical(f"Failed to run Uvicorn server: {e}", exc_info=False)
		exit(1)
