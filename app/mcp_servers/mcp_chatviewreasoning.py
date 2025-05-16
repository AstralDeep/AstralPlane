# app/mcp_chatviewreasoning.py
# FINAL VERSION (REVISED V25 - Frontend Styling Feedback Applied):
# - Incorporates specific styling feedback from the frontend model.
# - Removes redundant styles (display: flex, flexDirection, fontFamily).
# - Keeps necessary layout styles (height, padding, flex, flexGrow, minHeight, overflow) in config or config.style.
# - Implements in-memory chat history using a module-level dictionary.
# - Includes history in LLM prompts.
# - Filters the final response in `real_reasoning_chat_query` for <answer> tags.
# - Uses standard logging channel workaround for streaming.
# Assumes MCP SDK and OpenAI SDK are installed.

import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import List, Dict, Optional

# --- MCP Imports ---
import mcp.types as types
# --- Imports ---
import openai # type: ignore
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.shared.context import RequestContext
from openai import AsyncOpenAI

# Assuming these exist in your project structure.
# If not, you might need to adjust or provide placeholder/direct values.
from app.config import settings
from app.utils.logging_config import configure_logging

# --- Configuration ---
HOST = os.getenv("MCP_REASONING_HOST", "127.0.0.1")
PORT = int(os.getenv("MCP_REASONING_PORT", "8126"))
SERVER_NAME = "mcp_chatviewreasoning"
LOCAL_LLM_BASE_URL = os.getenv("LOCAL_LLM_BASE_URL", "http://10.33.31.31:30000/v1")
LOCAL_LLM_API_KEY = os.getenv("LOCAL_LLM_API_KEY", "not-needed")  # API key for the local LLM
LOCAL_LLM_MODEL_NAME = os.getenv("LOCAL_LLM_MODEL_NAME", "DeepSeek-R1")  # Model name for the local LLM
MAX_HISTORY_TURNS = int(os.getenv("MAX_HISTORY_TURNS", "10"))  # Max history turns (user + assistant pairs)

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
# Ensure 'settings.DEBUG' exists or replace with a boolean.
configure_logging(log_level=log_level, log_to_file=getattr(settings, 'DEBUG', False), log_dir="logs")
logger = logging.getLogger("app.mcp_servers.mcp_chatviewreasoning")
logger.info(f"Enhanced logging configured via logging_config. Level: {log_level_name}")

NotificationParamsBase = types.NotificationParams
logger.info("Using types.NotificationParams as base for custom params.")

# --- FastMCP Server Instance ---
# Initialize the FastMCP server
mcp_server = FastMCP(
    name=SERVER_NAME,
    host=HOST,
    port=PORT,
    log_level="DEBUG",  # Set log level for the MCP server
    dependencies=["uvicorn"]  # Specify dependencies like the web server
)

# --- OpenAI Client Initialization ---
# Initialize the asynchronous OpenAI client to interact with the local LLM
async_openai_client: Optional[AsyncOpenAI] = None

try:
    async_openai_client = AsyncOpenAI(
       base_url=LOCAL_LLM_BASE_URL,  # URL of the local LLM API endpoint
       api_key=LOCAL_LLM_API_KEY,  # API key (might be optional for local models)
    )
    logger.info(f"Initialized OpenAI ASYNC client targeting: {LOCAL_LLM_BASE_URL}")
except Exception as e:
    # Log any other error during client initialization
    logger.error(f"Failed to initialize OpenAI ASYNC client: {e}")
    async_openai_client = None

# --- History Storage (In-Memory) ---
session_histories: Dict[str, List[Dict[str, str]]] = {}


# --- UI Structure Definition (Applying Frontend Styling Feedback) ---
def construct_real_reasoning_ui_layout() -> dict:
    """Constructs the UI layout dictionary applying styling feedback."""
    logger.info(f"[{SERVER_NAME}] Constructing UI Layout (Applying Frontend Styling Feedback)...")
    # Define unique binding strings for different UI elements
    base_binding = f"mcp_stream:{SERVER_NAME}"
    log_binding = f"{base_binding}:log_messages"
    raw_stream_binding = f"{base_binding}:raw_llm_stream"
    chat_result_binding = f"{base_binding}:real_reasoning_chat_query_result"
    # Define element IDs
    input_field_id = "chat-input-field"
    send_button_id = "send-button"
    reasoning_action_id = "real_reasoning_chat_query"

    # Input Area: Keep specific padding via config
    input_area = {
       "id": "input-area", "type": "StackLayout",
       "config": {
          "direction": "horizontal",
          "gap": "8px",
          "align_items": "flex-end", # This is good for a horizontal input group
          "padding": "10px 0 0 0"
       },
       "children": [
          {"id": input_field_id, "type": "InputField", "config": {
             "placeholder": "Type your query...",
             "label": "Query",
             "enterKeyAction": {"isEnabled": True, "actionId": reasoning_action_id,
                            "targetElementId": send_button_id}}
           },
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

    # Left Column: Use config.style for flex, config.height for height
    # Note: "height": "100%" and "style.flex" are not directly used by current primitives.dart StackLayout for its children sizing.
    # These are hints for a more advanced renderer or would require DynamicRenderer to interpret.
    left_column = {
       "id": "left-column", "type": "StackLayout",
       "config": {
          "direction": "vertical",
          "gap": "0px",
          "height": "100%",
          "style": {"flex": "2 1 0%"}
       },
       "children": [
          {"id": "chat-display", "type": "ChatViewBasic", "config": {
             "title": "Chat Conversation",
             "autoScroll": True,
             "renderAsMarkdown": True,
             "style": {"minHeight": "100px"} # minHeight is also a hint for advanced rendering
          },
           "updateBinding": chat_result_binding, "children": None}, # children: None is fine
          input_area
       ]
    }

    # Right Column: Use config.style for flex, config.height for height
    # Similar note as left_column for height and flex.
    right_column_container = {
       "id": "right-column", "type": "StackLayout",
       "config": {
          "direction": "vertical",
          "gap": "8px",
          "height": "100%",
          "style": {"flex": "1 1 0%"}
       },
       "children": [
          {"id": "reasoning-log-view", "type": "McpStructuredLogView",
           "config": {
              "title": "Reasoning Process Logs",
              "autoScroll": True,
              "style": {"flex": "1 1 0%", "minHeight": "100px"}
           },
           "updateBinding": log_binding
           },
          {"id": "raw-stream-view", "type": "StreamingTextView",
           "config": {
              "title": "LLM Raw Stream Log",
              "height": "200px", # This height can be used by StreamingTextView if it parses it
              "autoScroll": True,
              "padding": "5px",
              "backgroundColor": "#f0f0f0",
              "borderRadius": "4px",
              "whiteSpace": "pre-wrap",
              "style": {"flex": "1 1 0%", "minHeight": "100px"}
           },
           "content": "",
           "updateBinding": raw_stream_binding
           }
       ]
    }

    # Root Layout: Use config.height
    # Note: "height": "100vh" is a CSS unit, not directly translatable to Flutter constraints without logic.
    # The root widget in Flutter usually gets constraints from the screen.
    return {
       "id": "real-reasoning-root-layout", "type": "StackLayout",
       "config": {
          "direction": "vertical",
          "padding": "16px",
          "gap": "16px",
          "height": "100vh"
       },
       "children": [
          {"id": "title", "type": "TextView",
           "config": {"initialText": f"MCP Local LLM Reasoning Chat ({SERVER_NAME})", "variant": "headline"}},
          {"id": "explanation", "type": "TextView", "config": {
             "initialText": f"Enter query. Reasoning logs appear below (right-top). Raw LLM stream logs appear "
                         f"below (right-bottom) and require backend routing. Filtered answer appears in chat"
                         f" (left). LLM: '{LOCAL_LLM_BASE_URL}'.",
             "variant": "body"}},
          # Main Columns:
          # "height": "0" and "style.flexGrow: 1" are hints for expansion.
          # For the immediate fix, "align_items" is added.
          # For true flexGrow behavior, Flutter's DynamicRenderer would need to wrap this
          # in an Expanded widget because its parent is a vertical StackLayout.
          {"id": "main-columns", "type": "StackLayout",
           "config": {
              "direction": "horizontal",
              "align_items": "start",  # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ADDED THIS LINE
              "gap": "16px",
              "height": "0",
              "style": {"flexGrow": 1, "overflow": "hidden"} # overflow:hidden is CSS, not Flutter directly
           },
           "children": [left_column, right_column_container]
           }
       ]
    }


# --- MCP Tools --- (No changes needed in tool logic itself)
@mcp_server.tool(name="get_ui_layout", description="Retrieves the UI layout configuration.")
async def get_ui_layout() -> List[types.TextContent]:
    """MCP Tool: Returns the UI layout configuration as a JSON string."""
    ctx = mcp_server.get_context()
    await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Called.")
    ui_layout_dict = construct_real_reasoning_ui_layout()
    try:
       ui_layout_json = json.dumps(ui_layout_dict)
       return [types.TextContent(type="text", text=ui_layout_json)]
    except TypeError as json_err:
       await ctx.error(f"[{SERVER_NAME} Tool:get_ui_layout] Failed UI layout serialization: {json_err}")
       return [types.TextContent(type="text", text=f"Error: Could not serialize UI layout")]


# --- Background Task for Sending Notifications ---
async def _notification_sender_task(
       ctx: RequestContext,
       queue: asyncio.Queue,
       log_prefix: str
):
    """
    Asynchronously gets LLM chunks from a queue and sends them to the frontend
    via the standard MCP logging channel (ctx.session.send_log_message).
    """
    logger.debug(f"{log_prefix} SENDER_TASK (Log Workaround): Started.")
    send_error_count = 0
    max_send_errors = 5
    processed_count = 0
    raw_stream_target_binding = f"mcp_stream:{SERVER_NAME}:raw_llm_stream"

    while True:
       chunk_text = None
       try:
          logger.debug(f"{log_prefix} SENDER_TASK: Waiting to get item from queue (Processed: {processed_count})...")
          chunk_text = await queue.get()
          logger.debug(f"{log_prefix} SENDER_TASK: Got item from queue. Item type: {type(chunk_text)}")

          if chunk_text is None:
             logger.info(f"{log_prefix} SENDER_TASK: Received None sentinel. Exiting loop.")
             break

          processed_count += 1
          chunk_text_str = str(chunk_text) if chunk_text is not None else ""

          if not chunk_text_str:
             logger.warning(
                f"{log_prefix} SENDER_TASK: Retrieved empty or non-string chunk from queue (Item {processed_count}). Original type: {type(chunk_text)}. Skipping send.")
             queue.task_done()
             continue

          logger.info(
             f"{log_prefix} SENDER_TASK: Processing chunk {processed_count} for log channel: '{chunk_text_str[:100]}...'")

          if not ctx.session or not hasattr(ctx.session, 'send_log_message'):
             logger.error(f"{log_prefix} SENDER_TASK: Session or send_log_message unavailable. Stopping sender.")
             queue.task_done()
             break

          try:
             logger.debug(
                f"{log_prefix} SENDER_TASK: Preparing to send Log Message (Workaround). Logger='{raw_stream_target_binding}', Data='{chunk_text_str[:50]}...'")
             await ctx.session.send_log_message(
                level='info',
                data=chunk_text_str,
                logger=raw_stream_target_binding
             )
             logger.info(f"{log_prefix} SENDER_TASK: Successfully called send_log_message (Workaround).")
             send_error_count = 0
             await asyncio.sleep(0.01)

          except Exception as send_err:
             send_error_count += 1
             logger.error(
                f"{log_prefix} SENDER_TASK: Error during send_log_message call (Workaround): {type(send_err).__name__} - {send_err}. Count: {send_error_count}",
                exc_info=True)
             if send_error_count >= max_send_errors:
                logger.error(f"{log_prefix} SENDER_TASK: Too many consecutive send errors. Stopping.")
                queue.task_done()
                break
          finally:
             if chunk_text is not None:
                queue.task_done()

       except asyncio.CancelledError:
          logger.warning(f"{log_prefix} SENDER_TASK: Cancelled.")
          if chunk_text is not None: queue.task_done()
          break
       except Exception as e:
          logger.error(f"{log_prefix} SENDER_TASK: Unexpected error in loop: {type(e).__name__} - {e}", exc_info=True)
          if chunk_text is not None: queue.task_done()
          break

    logger.info(f"{log_prefix} SENDER_TASK (Log Workaround): Finished loop. Processed {processed_count} chunks.")


# --- Reasoning Chat Query Tool ---
@mcp_server.tool(
    name="real_reasoning_chat_query",
    description="Calls local LLM with history, logs system steps via ctx.info, streams ALL raw chunks via background queue using standard LOGGING channel (BATCHED), returns final answer *extracted from <answer> tags*."
)
async def real_reasoning_chat_query(query: str) -> List[types.TextContent]:
    """
    MCP Tool: Handles user queries with history.
    1. Retrieves history.
    2. Calls LLM with history + current query.
    3. Streams raw chunks via background task (logging channel workaround).
    4. Updates history with query and extracted answer.
    5. Returns only the extracted answer.
    """
    try:
       ctx = mcp_server.get_context()
       if not ctx: raise RuntimeError("Failed to get MCP context.")
       logger.debug("Successfully obtained MCP context.")
    except Exception as ctx_err:
       logger.error(f"CRITICAL: Failed to get MCP context: {ctx_err}", exc_info=True)
       return [types.TextContent(type="text", text="Error: Internal Server Error - Context unavailable")]

    request_id = ctx.request_id
    timestamp = time.strftime('%H:%M:%S')
    short_req_id = str(request_id)[-6:] if request_id else "NO-ID"
    log_prefix = f"[{timestamp} Req: {short_req_id}]"
    logger.debug(
       f"{log_prefix} Entered real_reasoning_chat_query tool (Log Workaround + Batching + Answer Filter + History).")

    if not ctx.session or not hasattr(ctx.session, 'send_log_message'):
       error_msg = "MCP Session or send_log_message unavailable (needed for workaround)."
       logger.error(f"{log_prefix} {error_msg}")
       try: await ctx.error(f"{log_prefix} {error_msg}")
       except Exception: pass
       return [types.TextContent(type="text", text=f"Error: Internal Server Error - {error_msg}")]
    if not async_openai_client:
       error_msg = "OpenAI Async client not configured or available."
       logger.error(f"{log_prefix} {error_msg}")
       try: await ctx.error(f"{log_prefix} {error_msg}")
       except Exception: pass
       return [types.TextContent(type="text", text=f"Error: {error_msg}")]

    request_key = str(request_id) if request_id else f"fallback_session_{uuid.uuid4()}"
    history = session_histories.get(request_key, [])
    if not history:
       session_histories[request_key] = history
       await ctx.info(f"{log_prefix} Initialized new history for request: {request_key}")
    else:
       await ctx.info(f"{log_prefix} Retrieved history for request: {request_key}. Length: {len(history)} messages.")

    queue: Optional[asyncio.Queue] = None
    sender_task: Optional[asyncio.Task] = None
    try:
       logger.debug(f"{log_prefix} Creating asyncio.Queue...")
       queue = asyncio.Queue(maxsize=100)
       logger.debug(f"{log_prefix} Queue created.")
       logger.debug(f"{log_prefix} Creating sender task (Log Workaround)...")
       sender_task = asyncio.create_task(
          _notification_sender_task(ctx, queue, log_prefix),
          name=f"sender_task_{short_req_id}"
       )
       logger.info(f"{log_prefix} Sender task created (Log Workaround).")
    except Exception as setup_err:
       logger.error(f"{log_prefix} FAILED during Queue/Task setup: {setup_err}", exc_info=True)
       return [types.TextContent(type="text", text="Error: Internal Server Error - Task setup failed")]

    await ctx.info(f"{log_prefix} Received query: '{query}'")

    accumulated_full_response = ""
    llm_error_occurred = False
    MIN_BATCH_SIZE_CHARS = 30
    MAX_BATCH_DELAY_SECS = 0.2
    chunk_buffer = ""
    last_send_time = time.monotonic()

    template_query_content = f"""For the problem, produce a clear, step-by-step explanation of how the result was derived. If any discrepancies are identified, re-calculate and provide the correct steps without referencing prior mistakes or external explanations.

Restate the problem clearly using <think> tags.
Break down the solution into intuitive, logically ordered steps, ensuring all conclusions follow proper logical principles. Use plain language and avoid unnecessary technical jargon.
Conclude the reasoning with the final answer inside <answer> tags.

All reasoning must be within the <think></think> tags, no reasoning should occur outside those tags

<think>
Problem: {query}
Reasoning:
1. Start with the problem.
2. Determine possible solutions.
2. Explain and solve step by step
3. Conclude with the final result based on the above steps.

Recheck the steps, and answer if there is a discrepancy
</think>
<answer>final answer</answer>
"""
    try:
       prompt_messages = []
       prompt_messages.extend(history)
       prompt_messages.append({"role": "user", "content": template_query_content})
       await ctx.info(f"{log_prefix} Prepared prompt with {len(history)} history messages + current query.")

       await ctx.info(f"{log_prefix} System: Calling Local LLM ({LOCAL_LLM_MODEL_NAME})...")
       stream_start_time = time.monotonic()
       # Ensure async_openai_client is not None before using it
       if not async_openai_client:
            raise RuntimeError("OpenAI client not initialized")


       stream = await async_openai_client.chat.completions.create(
          model=LOCAL_LLM_MODEL_NAME, messages=prompt_messages, temperature=0.6, stream=True
       )
       logger.debug(f"{log_prefix} LLM stream object created.")

       async for chunk in stream:
          content = chunk.choices[0].delta.content if chunk.choices and chunk.choices[0].delta else None
          if content:
             accumulated_full_response += content
             chunk_buffer += content
             current_time = time.monotonic()
             if chunk_buffer and (len(chunk_buffer) >= MIN_BATCH_SIZE_CHARS or (current_time - last_send_time) > MAX_BATCH_DELAY_SECS):
                logger.debug(f"{log_prefix} Batch ready (Size: {len(chunk_buffer)}). Putting batch onto queue for raw stream.")
                if queue: await queue.put(chunk_buffer)
                chunk_buffer = ""
                last_send_time = current_time
                await asyncio.sleep(0)

       stream_duration = (time.monotonic() - stream_start_time) * 1000
       await ctx.info(f"{log_prefix} System: Local LLM stream finished ({stream_duration:.0f} ms).")

       if chunk_buffer and queue:
          logger.debug(f"{log_prefix} Flushing final buffer content to queue for raw stream (Size: {len(chunk_buffer)}).")
          await queue.put(chunk_buffer)
          chunk_buffer = ""
          await asyncio.sleep(0)

    except openai.APIConnectionError as e: # type: ignore
       error_msg = f"Connection Error contacting LLM at {LOCAL_LLM_BASE_URL}"
       await ctx.error(f"{log_prefix} {error_msg} Details: {e}")
       accumulated_full_response = f"Error: {error_msg}"
       llm_error_occurred = True
    except openai.APIError as e: # type: ignore
       error_msg = f"LLM API Error: {type(e).__name__} - {e}"
       await ctx.error(f"{log_prefix} {error_msg}")
       accumulated_full_response = f"Error: LLM API Error ({type(e).__name__})"
       llm_error_occurred = True
    except asyncio.CancelledError:
       await ctx.warning(f"{log_prefix} Reasoning task cancelled during LLM stream.")
       if queue: await queue.put(None)
       raise
    except Exception as e:
       error_msg = f"Unexpected error during LLM stream processing: {type(e).__name__}"
       await ctx.error(f"{log_prefix} {error_msg}", exc_info=True)
       accumulated_full_response = f"Error: {error_msg}"
       llm_error_occurred = True
    finally:
       try:
          if queue:
             logger.debug(f"{log_prefix} FINALLY: Putting None sentinel onto queue.")
             await queue.put(None)
             logger.debug(f"{log_prefix} FINALLY: Skipped queue.join() wait.")
          else:
             logger.warning(f"{log_prefix} FINALLY: Queue object does not exist.")

          if sender_task:
             logger.debug(f"{log_prefix} FINALLY: Waiting for sender task to finish...")
             try:
                await asyncio.wait_for(sender_task, timeout=5.0)
                logger.debug(f"{log_prefix} FINALLY: Sender task finished gracefully.")
             except asyncio.TimeoutError:
                logger.warning(f"{log_prefix} FINALLY: Sender task timed out during cleanup, cancelling.")
                sender_task.cancel()
                try: await sender_task
                except asyncio.CancelledError: logger.debug(f"{log_prefix} FINALLY: Sender task cancellation confirmed.")
                except Exception as final_task_err: logger.error(f"{log_prefix} FINALLY: Error awaiting cancelled sender task: {final_task_err}")
             except Exception as e:
                logger.error(f"{log_prefix} FINALLY: Error during sender task final wait/cancel: {type(e).__name__}")
          else:
             logger.warning(f"{log_prefix} FINALLY: Sender task was not created, skipping cleanup.")
       except Exception as finally_err:
          logger.error(f"{log_prefix} FINALLY: Unexpected error during cleanup: {finally_err}", exc_info=True)

    final_chat_answer = ""
    if llm_error_occurred:
       final_chat_answer = accumulated_full_response
       await ctx.warning(f"{log_prefix} LLM call failed. Returning error message.")
    else:
       logger.debug(f"{log_prefix} Attempting to extract content between <answer> tags...")
       match = re.search(r"<answer>(.*?)</answer>", accumulated_full_response, re.DOTALL | re.IGNORECASE)
       if match:
          extracted_answer = match.group(1).strip()
          final_chat_answer = extracted_answer
          logger.info(f"{log_prefix} Successfully extracted answer content. Length: {len(final_chat_answer)}")
          logger.debug(f"{log_prefix} Extracted Answer Preview: {final_chat_answer[:200]}...")
       else:
          logger.warning(f"{log_prefix} <answer> tags not found in the response. Returning full response as fallback.")
          final_chat_answer = accumulated_full_response.strip()

    if not final_chat_answer and not llm_error_occurred:
       await ctx.warning(f"{log_prefix} System: Final extracted answer appears empty after processing.")
       final_chat_answer = "(LLM stream returned no answer content within tags)"

    history.append({"role": "user", "content": query})
    if not llm_error_occurred:
       history.append({"role": "assistant", "content": final_chat_answer})

    if len(history) > MAX_HISTORY_TURNS * 2:
       history = history[-(MAX_HISTORY_TURNS * 2):]
       await ctx.info(f"{log_prefix} Truncated history to {MAX_HISTORY_TURNS} turns.")

    session_histories[request_key] = history
    await ctx.info(f"{log_prefix} Updated history for request: {request_key}. New length: {len(history)} messages.")

    completion_timestamp = time.strftime('%H:%M:%S')
    await ctx.info(
       f"[{completion_timestamp} Req: {short_req_id}] Reasoning task completed. Returning final result for chat view.")

    return [types.TextContent(type="text", text=final_chat_answer)]


# --- Main execution block ---
if __name__ == "__main__":
    logger.info(f"--- Starting FastMCP Local LLM Reasoning Chat server ({SERVER_NAME}) ---")
    starlette_app = None
    try:
       starlette_app = mcp_server.sse_app()
       if starlette_app is None:
          raise RuntimeError("mcp_server.sse_app() returned None")
    except Exception as app_err:
       logger.critical(f"CRITICAL ERROR: Failed to initialize Starlette app from FastMCP: {app_err}")
       exit(1)

    run_host = HOST
    run_port = PORT
    uvicorn_log_level_str = "info" # Default uvicorn log level
    if hasattr(mcp_server, 'settings') and hasattr(mcp_server.settings, 'log_level'):
       uvicorn_log_level_str = mcp_server.settings.log_level.lower()
       # Validate uvicorn log level
       if uvicorn_log_level_str not in ["critical", "error", "warning", "info", "debug", "trace"]:
           logger.warning(f"Invalid MCP log_level '{uvicorn_log_level_str}' for Uvicorn, defaulting to 'info'.")
           uvicorn_log_level_str = "info"
    else: # Fallback if mcp_server.settings.log_level is not available
        if hasattr(logging.getLogger().getEffectiveLevel(), 'name'): # Check if standard logger has a name attribute for level
            mcp_effective_log_level_name = logging.getLevelName(logging.getLogger().getEffectiveLevel()).lower()
            if mcp_effective_log_level_name in ["critical", "error", "warning", "info", "debug", "trace"]:
                uvicorn_log_level_str = mcp_effective_log_level_name
            else: # Final fallback
                 uvicorn_log_level_str = "info"


    logger.info(f"Attempting to listen on: http://{run_host}:{run_port}")
    logger.info(f"Server Name: {SERVER_NAME}")
    if async_openai_client:
       logger.info(f"Configured for Local LLM: {LOCAL_LLM_BASE_URL} ({LOCAL_LLM_MODEL_NAME})")
    else:
       logger.warning("WARNING: Local LLM communication disabled.")
    logger.debug(" - Using Standard Logging Channel (ctx.session.send_log_message) for raw streaming.")
    logger.debug(" - Binding passed via 'logger', chunk via 'data'.")
    logger.debug(" - Backend log handler MUST parse logger/data for raw stream updates.")
    logger.debug(" - Final answer for chat view is filtered for <answer> tags.")
    logger.debug(f" - In-memory history enabled, keeping last {MAX_HISTORY_TURNS} turns.")
    logger.debug(" - Styling configuration aligned with frontend feedback (using config/config.style).")
    logger.debug("----------------------------------------------------")

    try:
       uvicorn.run(starlette_app, host=run_host, port=run_port, log_level=uvicorn_log_level_str)
    except Exception as e:
       logger.critical(f"Failed to run Uvicorn server ({SERVER_NAME}): {e}")
       exit(1)