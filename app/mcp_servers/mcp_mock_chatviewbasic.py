# app/mock_chatviewbasic.py

import json
import logging
import os
import uuid
from typing import List, Dict, Any

import uvicorn

try:
	from app.config import settings
	from app.utils.logging_config import configure_logging
except ImportError:
	print("Warning: app.config or app.utils.logging_config not found. Using basic settings/logging.")


	class MockSettings:
		DEBUG = True


	settings = MockSettings()


	def configure_logging(log_level, log_to_file, log_dir):
		logging.basicConfig(level=log_level)
		print(f"Basic logging configured to level: {log_level}")

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.shared.context import RequestContext

HOST = "0.0.0.0"
PORT = 8123

# SERVER_ID_FOR_BINDING should be this server's unique string ID in your database
SERVER_ID_FOR_BINDING = "mcp_mock_chatviewbasic"
# SERVER_NAME is the human-readable name from your database (used for display)
SERVER_NAME = "Mock MCP Chat Server"

log_level_name_env = os.getenv("LOG_LEVEL_MCP_MOCK_CHATVIEWBASIC", "DEBUG").upper()
log_level_val = getattr(logging, log_level_name_env, logging.DEBUG)

# Use the unique ID for logger name consistency
logger_internal_id = SERVER_ID_FOR_BINDING.replace(" ", "_").lower()
configure_logging(log_level=log_level_val, log_to_file=settings.DEBUG, log_dir="logs")
logger = logging.getLogger(f"app.mcp_servers.{logger_internal_id}")
logger.info(
	f"MCP Server Display Name: '{SERVER_NAME}', ID for Bindings/Protocol: '{SERVER_ID_FOR_BINDING}'. Logging Level: {log_level_name_env}.")

# The 'name' for FastMCP() is its internal protocol name. Using the unique ID is good practice.
mcp_protocol_name = SERVER_ID_FOR_BINDING
mcp = FastMCP(name=mcp_protocol_name, host=HOST, port=PORT, log_level=log_level_name_env)
logger.info(f"FastMCP instance created with protocol name '{mcp_protocol_name}'.")

session_histories: Dict[str, List[types.SamplingMessage]] = {}


def construct_ui_layout() -> Dict[str, Any]:
	logger.info(
		f"MCP Server '{SERVER_NAME}' (ID: '{SERVER_ID_FOR_BINDING}') Constructing UI Layout. Bindings will use SERVER_ID_FOR_BINDING: '{SERVER_ID_FOR_BINDING}'")

	# --- MODIFICATION: ALL mcp_stream bindings MUST use the server's unique string ID ---
	chat_display_binding = f"mcp_stream:{SERVER_ID_FOR_BINDING}:chatbot_query_result"  # Uses string ID
	log_binding = f"mcp_stream:{SERVER_ID_FOR_BINDING}:log_messages"  # Uses string ID

	input_field_id = "chat-input-field"
	chat_action_id = "chatbot_query"

	main_content_area = {
		"id": "main-content", "type": "StackLayout",
		"config": {"direction": "vertical", "gap": "0px", "style": {"minHeight": "0"}},
		"children": [
			{"id": "chat-display", "type": "ChatViewBasic",
			 "config": {"title": f"{SERVER_NAME} Chat", "autoScroll": True,
						"style": {"flexGrow": 1, "minHeight": "200px"}},
			 "updateBinding": chat_display_binding},  # Correctly uses ID-based binding
			{"id": "input-area", "type": "StackLayout",
			 "config": {"direction": "horizontal", "gap": "8px", "align_items": "flex-end",
						"style": {"paddingTop": "10px"}},
			 "children": [
				 {"id": input_field_id, "type": "InputField",
				  "config": {"placeholder": "Type...", "label": "Message", "style": {"flexGrow": 1},
							 "enterKeyAction": {"isEnabled": True, "actionId": chat_action_id,
												"targetElementId": "send-button"}}},
				 {"id": "send-button", "type": "Button", "actionId": chat_action_id,
				  "config": {"label": "Send", "variant": "primary", "valueSourceElementIds": [input_field_id],
							 "frontendActions": [
								 {"type": "echoToView", "sourceElementId": input_field_id,
								  "targetBinding": chat_display_binding, "role": "user"},
								 {"type": "clearElement", "targetElementId": input_field_id}
							 ]}}
			 ]}
		]
	}

	log_view_area = {
		"id": "server-logs", "type": "LogView",
		"config": {"title": f"{SERVER_NAME} Logs", "lineFormat": "json", "autoScroll": True,
				   "style": {"height": "200px", "marginTop": "16px"}},
		"updateBinding": log_binding  # Correctly uses ID-based binding
	}

	root_layout = {
		"id": "root-layout", "type": "StackLayout",
		"config": {"direction": "vertical", "padding": "16px", "gap": "16px",
				   "style": {"height": "100vh", "display": "flex", "flexDirection": "column"}},
		"children": [main_content_area, log_view_area]
	}
	logger.debug(
		f"MCP Server '{SERVER_NAME}' (ID: '{SERVER_ID_FOR_BINDING}') Generated UI Layout with log_binding: '{log_binding}' and chat_display_binding: '{chat_display_binding}'")
	return root_layout


@mcp.tool(name="chatbot_query",
		  description="Receives user query, calls LLM mock, returns assistant response as TextContent.")
async def chatbot_query(query: str) -> List[types.TextContent]:
	ctx = mcp.get_context()
	request_key = str(ctx.request_id) if ctx.request_id else f"fallback_session_{uuid.uuid4()}"
	short_req_id = request_key[-6:]
	await ctx.info(
		f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Received query: '{query}'")  # Using display name and ID in log for clarity
	history: List[types.SamplingMessage] = session_histories.get(request_key, [])
	if not history:
		session_histories[request_key] = history
		await ctx.info(f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Initialized new history.")
	user_msg_content = types.TextContent(type="text", text=query)
	user_msg = types.SamplingMessage(role="user", content=user_msg_content)  # type: ignore
	history.append(user_msg)
	await ctx.info(
		f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Asking client (backend) via create_message with {len(history)} messages...")
	try:
		if not ctx.session:
			await ctx.error(
				f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] MCP Session context is missing for create_message.")
			raise RuntimeError("MCP Session context is missing")
		llm_response = await ctx.session.create_message(messages=history, max_tokens=100)
		await ctx.debug(
			f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] DEBUG: Raw llm_response from create_message: {type(llm_response)} - {llm_response!r}")
		assistant_response_text = "Error: Assistant did not return expected text."
		if (hasattr(llm_response, 'content') and
				llm_response.content is not None and
				isinstance(llm_response.content, types.TextContent) and
				getattr(llm_response.content, 'type') == 'text'):
			assistant_response_text = llm_response.content.text
			await ctx.debug(
				f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Extracted text via CreateMessageResult-like structure.")
		else:
			await ctx.warning(
				f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Unexpected llm_response format: {type(llm_response)}. Stringifying.")
			assistant_response_text = str(llm_response)
		await ctx.info(
			f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Processed assistant response text: '{assistant_response_text[:100]}...'")
		return [types.TextContent(type="text", text=assistant_response_text)]
	except Exception as e:
		await ctx.error(
			f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Req:{short_req_id}] Error during chatbot_query: {e}",
			exc_info=True)
		return [types.TextContent(type="text", text=f"Error: {e}")]


@mcp.tool(name="get_ui_layout", description="Retrieves the UI layout configuration.")
async def get_ui_layout() -> List[types.TextContent]:
	ctx = mcp.get_context()
	await ctx.info(f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Tool:get_ui_layout] Called to provide UI definition.")
	ui_layout_dict = construct_ui_layout()
	try:
		ui_layout_json = json.dumps(ui_layout_dict)
		return [types.TextContent(type="text", text=ui_layout_json)]
	except TypeError as json_err:
		error_msg = f"Failed to serialize UI layout to JSON: {json_err}"
		await ctx.error(f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING}) Tool:get_ui_layout] {error_msg}", exc_info=True)
		error_response_json = json.dumps({"error": "Failed to generate UI layout", "detail": str(json_err)})
		return [types.TextContent(type="text", text=error_response_json)]


if __name__ == "__main__":
	logger.info(
		f"--- Starting FastMCP server '{SERVER_NAME}' (ID: '{SERVER_ID_FOR_BINDING}', Protocol Name: '{mcp.name}') ---")
	starlette_app = mcp.sse_app()
	if starlette_app is None:
		logger.critical(
			f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING})] mcp.sse_app() returned None. Cannot start server.")
		exit(1)
	run_host = mcp.settings.host if hasattr(mcp, 'settings') and mcp.settings.host else HOST
	run_port = mcp.settings.port if hasattr(mcp, 'settings') and mcp.settings.port else PORT
	uvicorn_log_level = (mcp.settings.log_level.lower()
						 if hasattr(mcp, 'settings') and hasattr(mcp.settings, 'log_level') and mcp.settings.log_level
						 else log_level_name_env.lower())
	logger.info(f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING})] Attempting to run Uvicorn on http://{run_host}:{run_port} with log level '{uvicorn_log_level}'")
	try:
		uvicorn.run(starlette_app, host=run_host, port=run_port, log_level=uvicorn_log_level)
	except Exception as e:
		logger.critical(f"[{SERVER_NAME} (ID:{SERVER_ID_FOR_BINDING})] Failed to start Uvicorn: {e}", exc_info=True)
		exit(1)
