# app/mock_chatviewbasic.py
# Modified version - Adjusted layout to move LogView up

import json
import logging
import os
import uuid
from typing import List, Dict, Any

# Required runner import
import uvicorn  # Explicitly import uvicorn

from app.config import settings
from app.utils.logging_config import configure_logging

# MCP Imports
import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.shared.context import RequestContext

# --- Configuration ---
HOST = "127.0.0.1"
PORT = 8123
SERVER_NAME = "mcp_mock_chatviewbasic"

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
configure_logging(log_level=log_level, log_to_file=settings.DEBUG, log_dir="logs")
logger = logging.getLogger("app.mcp_servers.mcp_mock_chatviewbasic")
logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")

# --- FastMCP Server Instance ---
mcp = FastMCP(name=SERVER_NAME, host=HOST, port=PORT, log_level="DEBUG")

# --- History Storage ---
session_histories: Dict[str, List[Any]] = {}


# --- UI Structure Function ---
def construct_ui_layout() -> dict:
	""" Constructs the UI layout dictionary with LogView positioned higher. """
	logger.info(f"[{SERVER_NAME}] Constructing UI Layout (LogView Higher)...")
	chat_display_binding = f"mcp_stream:{SERVER_NAME}:chatbot_query_result"
	log_binding = f"mcp_stream:{SERVER_NAME}:log_messages"
	input_field_id = "chat-input-field"
	chat_action_id = "chatbot_query"

	# Main content area (Chat + Input)
	main_content_area = {
		"id": "main-content", "type": "StackLayout",
		"config": {
			"direction": "vertical",
			"gap": "0px",
			# --- REMOVED flexGrow: 1 ---
			# It will now only take the height needed by its children
			"style": {"minHeight": "0"}  # Keeps flex context, prevents potential collapse issues
		},
		"children": [
			{"id": "chat-display", "type": "ChatViewBasic",
			 # Keep flexGrow here so chat expands WITHIN main_content_area
			 "config": {"title": f"{SERVER_NAME} Chat", "autoScroll": True,
						"style": {"flexGrow": 1, "minHeight": "200px"}},  # Added minHeight
			 "updateBinding": chat_display_binding},
			{"id": "input-area", "type": "StackLayout",
			 # Input area takes its natural height
			 "config": {"direction": "horizontal", "gap": "8px", "align_items": "flex-end",
						"style": {"paddingTop": "10px"}},
			 "children": [
				 {"id": input_field_id, "type": "InputField",
				  "config": {"placeholder": "Type...", "label": "Message", "style": {"flexGrow": 1},
							 "enterKeyAction": {"isEnabled": True, "actionId": chat_action_id,
												"targetElementId": "send-button"}}},
				 {"id": "send-button", "type": "Button", "actionId": chat_action_id,
				  "config": {"label": "Send", "variant": "primary", "valueSourceElementIds": [input_field_id],
							 "frontendActions": [{"type": "echoToView", "sourceElementId": input_field_id,
												  "targetBinding": chat_display_binding, "role": "user"},
												 {"type": "clearElement", "targetElementId": input_field_id}]}}]}
		]
	}

	# Log view area (Same definition)
	log_view_area = {
		"id": "server-logs", "type": "LogView",
		"config": {"title": "Server Logs", "lineFormat": "json", "autoScroll": True,
				   "style": {"height": "200px", "marginTop": "16px"}},  # Fixed height log view
		"updateBinding": log_binding
	}

	# Root layout (Structure unchanged, but behavior changes due to child flexGrow)
	return {
		"id": "root-layout", "type": "StackLayout",
		"config": {"direction": "vertical", "padding": "16px", "gap": "16px",
				   # Keep 100vh so the overall container tries to fill height
				   "style": {"height": "100vh", "display": "flex", "flexDirection": "column"}
				   },
		"children": [
			main_content_area,  # No longer grows indefinitely
			log_view_area  # Should appear more directly below main_content_area
		]
	}


# --- Chatbot Tool (Returning TextContent) --- (Unchanged from previous working version)
@mcp.tool(
	name="chatbot_query",
	description="Receives user query, calls LLM mock, returns assistant response as TextContent."
)
async def chatbot_query(query: str) -> List[types.TextContent]:  # Return TextContent
	ctx = mcp.get_context()
	user_query = query
	request_key = str(ctx.request_id) if ctx.request_id else f"fallback_session_{uuid.uuid4()}"
	short_req_id = request_key[-6:]
	await ctx.info(f"[{SERVER_NAME} Req:{short_req_id}] Received query: '{user_query}'")

	history: List[Any] = session_histories.get(request_key, [])
	if not history: session_histories[request_key] = history; await ctx.info(
		f"[{SERVER_NAME} Req:{short_req_id}] Initialized new history.")
	user_msg_content = types.TextContent(type="text", text=user_query)
	user_msg = types.SamplingMessage(role="user", content=user_msg_content)
	history.append(user_msg)
	await ctx.info(
		f"[{SERVER_NAME} Req:{short_req_id}] Asking client via create_message with {len(history)} messages...")

	try:
		if not ctx.session: raise RuntimeError("MCP Session context is missing")
		llm_response = await ctx.session.create_message(messages=history, max_tokens=100)
		await ctx.debug(f"[{SERVER_NAME} Req:{short_req_id}] DEBUG: Raw llm_response type: {type(llm_response)}")

		assistant_response_text = "Error: Assistant did not return text."
		# Process llm_response using hasattr (robust check)
		if (hasattr(llm_response, 'content') and hasattr(llm_response, 'role') and
				llm_response.content is not None and hasattr(llm_response.content, 'text') and
				hasattr(llm_response.content, 'type') and getattr(llm_response.content, 'type') == 'text'):
			assistant_response_text = llm_response.content.text
			await ctx.debug(
				f"[{SERVER_NAME} Req:{short_req_id}] Extracted text via CreateMessageResult-like structure.")
		elif (hasattr(llm_response, 'text') and hasattr(llm_response, 'type') and
			  getattr(llm_response, 'type') == 'text'):
			assistant_response_text = llm_response.text
			await ctx.warning(
				f"[{SERVER_NAME} Req:{short_req_id}] Extracted text directly from TextContent-like response.")
		else:
			await ctx.error(
				f"[{SERVER_NAME} Req:{short_req_id}] Unexpected llm_response format: {type(llm_response)}. Stringifying.")
			assistant_response_text = str(llm_response)

		await ctx.debug(
			f"[{SERVER_NAME} Req:{short_req_id}] DEBUG: Final assistant_response_text: {assistant_response_text!r}")
		await ctx.info(
			f"[{SERVER_NAME} Req:{short_req_id}] Processed assistant response text: '{assistant_response_text[:100]}...'")

		await ctx.debug(f"[{SERVER_NAME} Req:{short_req_id}] Returning plain TextContent.")
		return [types.TextContent(type="text", text=assistant_response_text)]  # Return TextContent

	except Exception as e:
		await ctx.error(f"[{SERVER_NAME} Req:{short_req_id}] Error: {e}", exc_info=True)
		return [types.TextContent(type="text", text=f"Error: {e}")]  # Return error as TextContent


# --- UI Layout Tool Handler --- (Unchanged)
@mcp.tool(name="get_ui_layout", description="Retrieves the UI layout configuration...")
async def get_ui_layout() -> List[Any]:
	ctx = mcp.get_context();
	await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Called.")
	ui_layout_dict = construct_ui_layout()
	try:
		ui_layout_json = json.dumps(ui_layout_dict);
		return [types.TextContent(type="text", text=ui_layout_json)]
	except TypeError as json_err:
		await ctx.error(f"Failed layout: {json_err}");
		return [
			types.TextContent(type="text", text=f"Error: {json_err}")]


# --- Main execution block --- (Unchanged)
if __name__ == "__main__":
	# (Startup logic unchanged)
	logger.info(f"--- Starting FastMCP server ({SERVER_NAME}) ---")
	starlette_app = mcp.sse_app()
	if starlette_app is None:
		logger.critical(f"mcp.sse_app() returned None.")
		exit(1)
	run_host = mcp.settings.host if hasattr(mcp, 'settings') else HOST
	run_port = mcp.settings.port if hasattr(mcp, 'settings') else PORT
	log_level = mcp.settings.log_level.lower() if hasattr(mcp, 'settings') and hasattr(mcp.settings,
																					   'log_level') else "debug"
	logger.info(f"Listen on: http://{run_host}:{run_port} with log level {log_level}")
	try:
		uvicorn.run(starlette_app, host=run_host, port=run_port, log_level=log_level)
	except Exception as e:
		logger.critical(f"Failed Uvicorn: {e}", exc_info=True);
		exit(1)
