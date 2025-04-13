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

import logging
import asyncio
import sys
import time
import json
import uuid
import os
import re
from typing import List, Dict, Any, Optional, Set, Union
import uvicorn

# --- Imports ---
import openai
from openai import AsyncOpenAI
from pydantic import BaseModel # Import Pydantic BaseModel

# --- MCP Imports ---
try:
    import mcp.types as types
    from mcp.server.fastmcp import FastMCP
    from mcp.shared.context import RequestContext
    from mcp.shared.exceptions import McpError
    from pydantic.networks import AnyUrl # Keep for potential future use
    MCP_AVAILABLE = True
    print("Successfully imported MCP libraries.")
    # Attempt to import base NotificationParams for inheritance, fallback to BaseModel
    try:
        # Attempt to get the base class for notification parameters if it exists
        NotificationParamsBase = types.NotificationParams
        print("Using types.NotificationParams as base for custom params.")
    except AttributeError:
        # Fallback to Pydantic's BaseModel if types.NotificationParams is not found
        print("Warning: types.NotificationParams not found, using pydantic.BaseModel for custom params.")
        NotificationParamsBase = BaseModel # type: ignore
except ImportError:
     # Handle cases where MCP SDK is not installed
     print("MCP libraries not found. Install mcp-sdk. Using Dummies.")
     MCP_AVAILABLE = False
     # --- Dummies (Provide basic stand-ins if MCP is not available) ---
     class FastMCP:
         _mcp_server = type('MockMcpServer', (), {'notification_handlers': {}})()
         def __init__(self, *args, **kwargs): self.settings = type('Settings', (), {'host':'localhost', 'port':8000, 'log_level':'info'})()
         def tool(self, *args, **kwargs): return lambda f: f # Decorator returns the function itself
         def get_context(self) -> 'Context': return Context() # Returns a dummy context
         def sse_app(self): return None # No SSE app available
     class types:
          class Notification:
               # Dummy Notification class initializer
               def __init__(self, *, method: str, params: Any): self.method = method; self.params = params
               def __repr__(self): return f"Notification(method='{self.method}', params={self.params!r})" # Basic representation
          class TextContent:
              # Dummy TextContent class initializer
              def __init__(self, *, type: str, text: str): self.type = type; self.text = text
     class Context:
         # Dummy session object with a send_notification method that prints
         # Update dummy session to include send_log_message
         session = type('Session', (), {
             'send_notification': lambda s, n: print(f"DUMMY_SEND_NOTIF: {n.method} - {n.params}"),
             'send_log_message': lambda s, level, data, logger=None: print(f"DUMMY_SEND_LOG: L={level} Logger='{logger}' Data='{str(data)[:100]}...'")
         })()
         _request_context = None; request_id = f"dummy-req-{uuid.uuid4()}" # Dummy request ID
         # Dummy async logging methods
         async def log(self, level, data, logger=None): print(f"DUMMY_CTX_LOG: L={level} Logger='{logger}' Data='{str(data)[:100]}...'")
         async def debug(self, data, logger=None): await self.log('debug', data, logger)
         async def info(self, data, logger=None): await self.log('info', data, logger)
         async def warning(self, data, logger=None): await self.log('warning', data, logger)
         async def error(self, data, logger=None, exc_info=False): await self.log('error', data, logger) # exc_info dummy

     class RequestContext: pass # Dummy RequestContext
     class McpError(Exception): pass # Dummy McpError exception
     class AnyUrl(str): pass # Dummy AnyUrl type
     NotificationParamsBase = BaseModel # type: ignore # Use Pydantic base if MCP types missing
     # --- End Dummies ---


# --- Configuration ---
# Load configuration from environment variables with defaults
HOST = os.getenv("MCP_REASONING_HOST", "127.0.0.1")
PORT = int(os.getenv("MCP_REASONING_PORT", "8126"))
SERVER_NAME = "mcp_chatviewreasoning"
LOCAL_LLM_BASE_URL = os.getenv("LOCAL_LLM_BASE_URL", "http://10.33.31.31:30000/v1")
LOCAL_LLM_API_KEY = os.getenv("LOCAL_LLM_API_KEY", "not-needed") # API key for the local LLM
LOCAL_LLM_MODEL_NAME = os.getenv("LOCAL_LLM_MODEL_NAME", "DeepSeek-R1") # Model name for the local LLM
MAX_HISTORY_TURNS = int(os.getenv("MAX_HISTORY_TURNS", "10")) # Max history turns (user + assistant pairs)

# --- Logging Setup ---
# Configure basic logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
server_logger = logging.getLogger(f"App.{SERVER_NAME}") # Get a logger specific to this server


# --- FastMCP Server Instance ---
# Initialize the FastMCP server
mcp_server = FastMCP(
    name=SERVER_NAME,
    host=HOST,
    port=PORT,
    log_level="DEBUG", # Set log level for the MCP server
    dependencies=["uvicorn"] # Specify dependencies like the web server
)

# --- OpenAI Client Initialization ---
# Initialize the asynchronous OpenAI client to interact with the local LLM
async_openai_client: Optional[AsyncOpenAI] = None
try:
    async_openai_client = AsyncOpenAI(
        base_url=LOCAL_LLM_BASE_URL, # URL of the local LLM API endpoint
        api_key=LOCAL_LLM_API_KEY,   # API key (might be optional for local models)
    )
    server_logger.info(f"Initialized OpenAI ASYNC client targeting: {LOCAL_LLM_BASE_URL}")
except NameError:
     # Log error if AsyncOpenAI is not available (openai package not installed/updated)
     server_logger.error("OpenAI SDK's AsyncOpenAI not found. Please install 'openai'. Reasoning tool will fail.")
     async_openai_client = None
except Exception as e:
    # Log any other error during client initialization
    server_logger.error(f"Failed to initialize OpenAI ASYNC client: {e}")
    async_openai_client = None

# --- History Storage (In-Memory) ---
session_histories: Dict[str, List[Dict[str, str]]] = {}

# --- UI Structure Definition (Applying Frontend Styling Feedback) ---
def construct_real_reasoning_ui_layout() -> dict:
    """Constructs the UI layout dictionary applying styling feedback."""
    server_logger.info(f"[{SERVER_NAME}] Constructing UI Layout (Applying Frontend Styling Feedback)...")
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
            "align_items": "flex-end",
            "padding": "10px 0 0 0" # Keep specific padding
        },
        # No style or className needed based on feedback
        "children": [
            # Input field: Rely on parent StackLayout for sizing, no flexGrow needed here
            {"id": input_field_id, "type": "InputField", "config": {
                "placeholder": "Type your query...",
                "label": "Query",
                "enterKeyAction": {"isEnabled": True, "actionId": reasoning_action_id, "targetElementId": send_button_id}}
            },
            {"id": send_button_id, "type": "Button", "actionId": reasoning_action_id, "config": {
                "label": "Send",
                "variant": "primary",
                "valueSourceElementIds": [input_field_id],
                "frontendActions": [
                    {"type": "echoToView", "sourceElementId": input_field_id, "targetBinding": chat_result_binding, "role": "user"},
                    {"type": "clearElement", "targetElementId": input_field_id}
                ]
            }},
        ]
    }

    # Left Column: Use config.style for flex, config.height for height
    left_column = {
        "id": "left-column", "type": "StackLayout",
        "config": {
            "direction": "vertical", # Default, but explicit
            "gap": "0px",
            "height": "100%", # Keep height in config
            "style": { "flex": "2 1 0%" } # Keep flex in config.style (use 'flex' shorthand or 'flexGrow: 2', 'flexShrink: 1', 'flexBasis: 0%')
        },
        # No redundant display:flex, flexDirection needed
        "children": [
            # Chat Display: Remove flexGrow, keep minHeight
            {"id": "chat-display", "type": "ChatViewBasic", "config": {
                 "title": "Chat Conversation",
                 "autoScroll": True,
                 "renderAsMarkdown": True,
                 "style": { "minHeight": "100px" } # Keep minHeight in config.style
                },
                "updateBinding": chat_result_binding, "children": None},
            input_area
        ]
    }

    # Right Column: Use config.style for flex, config.height for height
    right_column_container = {
        "id": "right-column", "type": "StackLayout",
        "config": {
            "direction": "vertical", # Default, but explicit
            "gap": "8px",
            "height": "100%", # Keep height in config
            "style": { "flex": "1 1 0%" } # Keep flex in config.style
        },
        # No redundant display:flex, flexDirection needed
        "children": [
            # Reasoning Log View: Use config.style for flex, minHeight
            {"id": "reasoning-log-view", "type": "McpStructuredLogView",
             "config": {
                 "title": "Reasoning Process Logs",
                 "autoScroll": True,
                 "style": { "flex": "1 1 0%", "minHeight": "100px" } # Keep flex and minHeight in config.style
                },
             "updateBinding": log_binding
            },
            # Raw Stream View: Remove fontFamily, keep flex, minHeight, pass others via config
            {"id": "raw-stream-view", "type": "StreamingTextView",
             "config": {
                 "title": "LLM Raw Stream Log",
                 "height": "200px", # Pass height via config
                 "autoScroll": True,
                 "padding": "5px", # Pass padding via config
                 "backgroundColor": "#f0f0f0", # Pass background via config
                 "borderRadius": "4px", # Pass border-radius via config
                 "whiteSpace": "pre-wrap", # Pass whitespace via config
                 "style": { "flex": "1 1 0%", "minHeight": "100px" } # Keep flex and minHeight in config.style
                 # fontFamily: monospace REMOVED
                 },
             "content": "",
             "updateBinding": raw_stream_binding
            }
        ]
    }

    # Root Layout: Use config.height
    return {
        "id": "real-reasoning-root-layout", "type": "StackLayout",
        "config": {
            "direction": "vertical", # Default, but explicit
            "padding": "16px",
            "gap": "16px",
            "height": "100vh" # Keep height in config
        },
         # No redundant display:flex, flexDirection needed
        "children": [
            {"id": "title", "type": "TextView", "config": {"initialText": f"MCP Local LLM Reasoning Chat ({SERVER_NAME})", "variant":"headline"}},
            {"id": "explanation", "type": "TextView", "config": {"initialText": f"Enter query. Reasoning logs appear below (right-top). Raw LLM stream logs appear below (right-bottom) and require backend routing. Filtered answer appears in chat (left). LLM: '{LOCAL_LLM_BASE_URL}'.", "variant":"body"}},
            # Main Columns: Use config.style for flexGrow, overflow. Use config.height
            {"id": "main-columns", "type": "StackLayout",
             "config": {
                 "direction": "horizontal",
                 "gap": "16px",
                 "height": "0", # Keep height in config
                 "style": { "flexGrow": 1, "overflow": "hidden" } # Keep flexGrow and overflow in config.style
                },
             "children": [ left_column, right_column_container ]
            }
        ]
    }


# --- MCP Tools --- (No changes needed in tool logic itself)

@mcp_server.tool( name="get_ui_layout", description="Retrieves the UI layout configuration.")
async def get_ui_layout() -> List[types.TextContent]:
    """MCP Tool: Returns the UI layout configuration as a JSON string."""
    ctx = mcp_server.get_context() # Get the request context
    await ctx.info(f"[{SERVER_NAME} Tool:get_ui_layout] Called.") # Log tool call
    ui_layout_dict = construct_real_reasoning_ui_layout() # Build the layout dictionary
    try:
        # Serialize the layout dictionary to JSON
        ui_layout_json = json.dumps(ui_layout_dict)
        # Return the JSON string within a TextContent object
        return [types.TextContent(type="text", text=ui_layout_json)]
    except TypeError as json_err:
        # Log and return an error if serialization fails
        await ctx.error(f"[{SERVER_NAME} Tool:get_ui_layout] Failed UI layout serialization: {json_err}")
        return [types.TextContent(type="text", text=f"Error: Could not serialize UI layout")]


# --- Background Task for Sending Notifications (Uses Logging Channel Workaround) ---
# (No changes needed in this task based on styling feedback)
async def _notification_sender_task(
    ctx: RequestContext,
    queue: asyncio.Queue,
    log_prefix: str
):
    """
    Asynchronously gets LLM chunks from a queue and sends them to the frontend
    via the standard MCP logging channel (ctx.session.send_log_message).
    """
    server_logger.debug(f"{log_prefix} SENDER_TASK (Log Workaround): Started.")
    send_error_count = 0 # Counter for consecutive send errors
    max_send_errors = 5 # Limit for consecutive send errors before stopping
    processed_count = 0 # Counter for processed chunks

    # Define the target binding string - this will be used as the 'logger' name
    raw_stream_target_binding = f"mcp_stream:{SERVER_NAME}:raw_llm_stream"

    while True: # Loop indefinitely until None is received from the queue
        chunk_text = None # Initialize chunk_text for error handling in finally block
        try:
            server_logger.debug(f"{log_prefix} SENDER_TASK: Waiting to get item from queue (Processed: {processed_count})...")
            chunk_text = await queue.get()
            server_logger.debug(f"{log_prefix} SENDER_TASK: Got item from queue. Item type: {type(chunk_text)}")

            if chunk_text is None:
                server_logger.info(f"{log_prefix} SENDER_TASK: Received None sentinel. Exiting loop.")
                break # Exit the loop

            processed_count += 1
            chunk_text_str = str(chunk_text) if chunk_text is not None else ""

            if not chunk_text_str:
                 server_logger.warning(f"{log_prefix} SENDER_TASK: Retrieved empty or non-string chunk from queue (Item {processed_count}). Original type: {type(chunk_text)}. Skipping send.")
                 queue.task_done() # Mark the empty task as done
                 continue

            server_logger.info(f"{log_prefix} SENDER_TASK: Processing chunk {processed_count} for log channel: '{chunk_text_str[:100]}...'")

            if not ctx.session or not hasattr(ctx.session, 'send_log_message'):
                 server_logger.error(f"{log_prefix} SENDER_TASK: Session or send_log_message unavailable. Stopping sender.")
                 queue.task_done() # Mark task done before breaking
                 break # Exit if sending is not possible

            try:
                server_logger.debug(f"{log_prefix} SENDER_TASK: Preparing to send Log Message (Workaround). Logger='{raw_stream_target_binding}', Data='{chunk_text_str[:50]}...'")
                await ctx.session.send_log_message(
                    level='info', # Or 'debug' etc. - doesn't affect routing here
                    data=chunk_text_str,
                    logger=raw_stream_target_binding # Critical: Backend uses this for routing
                )
                server_logger.info(f"{log_prefix} SENDER_TASK: Successfully called send_log_message (Workaround).")
                send_error_count = 0 # Reset error count on success
                await asyncio.sleep(0.01) # Small yield to prevent blocking

            except Exception as send_err:
                send_error_count += 1
                server_logger.error(f"{log_prefix} SENDER_TASK: Error during send_log_message call (Workaround): {type(send_err).__name__} - {send_err}. Count: {send_error_count}", exc_info=True)
                if send_error_count >= max_send_errors:
                    server_logger.error(f"{log_prefix} SENDER_TASK: Too many consecutive send errors. Stopping.")
                    queue.task_done() # Mark task done before breaking
                    break
            finally:
                 if chunk_text is not None:
                     queue.task_done()

        except asyncio.CancelledError:
            server_logger.warning(f"{log_prefix} SENDER_TASK: Cancelled.")
            if chunk_text is not None: queue.task_done() # Mark potentially retrieved task done
            break # Exit loop on cancellation
        except Exception as e:
            server_logger.error(f"{log_prefix} SENDER_TASK: Unexpected error in loop: {type(e).__name__} - {e}", exc_info=True)
            if chunk_text is not None: queue.task_done() # Mark potentially retrieved task done
            break # Exit loop on unexpected error

    server_logger.info(f"{log_prefix} SENDER_TASK (Log Workaround): Finished loop. Processed {processed_count} chunks.")


# --- Reasoning Chat Query Tool (History Logic Added) ---
# (No changes needed in this tool based on styling feedback)
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
        # Get the MCP request context
        ctx = mcp_server.get_context()
        if not ctx: raise RuntimeError("Failed to get MCP context.")
        server_logger.debug("Successfully obtained MCP context.")
    except Exception as ctx_err:
        server_logger.error(f"CRITICAL: Failed to get MCP context: {ctx_err}", exc_info=True)
        return [types.TextContent(type="text", text="Error: Internal Server Error - Context unavailable")]

    request_id = ctx.request_id
    timestamp = time.strftime('%H:%M:%S')
    short_req_id = str(request_id)[-6:] if request_id else "NO-ID"
    log_prefix = f"[{timestamp} Req: {short_req_id}]"
    server_logger.debug(f"{log_prefix} Entered real_reasoning_chat_query tool (Log Workaround + Batching + Answer Filter + History).")

    # --- Pre-checks ---
    if not ctx.session or not hasattr(ctx.session, 'send_log_message'):
        error_msg = "MCP Session or send_log_message unavailable (needed for workaround)."
        server_logger.error(f"{log_prefix} {error_msg}")
        try: await ctx.error(f"{log_prefix} {error_msg}")
        except Exception: pass
        return [types.TextContent(type="text", text=f"Error: Internal Server Error - {error_msg}")]
    if not async_openai_client:
        error_msg = "OpenAI Async client not configured or available."
        server_logger.error(f"{log_prefix} {error_msg}")
        try: await ctx.error(f"{log_prefix} {error_msg}")
        except Exception: pass
        return [types.TextContent(type="text", text=f"Error: {error_msg}")]

    sender_task = None
    queue = None
    raw_stream_target_binding = f"mcp_stream:{SERVER_NAME}:raw_llm_stream"

    # --- Get History ---
    request_key = str(request_id) if request_id else f"fallback_session_{uuid.uuid4()}" # Use request_id as key
    history = session_histories.get(request_key, [])
    if not history:
        session_histories[request_key] = history # Initialize if first time
        await ctx.info(f"{log_prefix} Initialized new history for request: {request_key}")
    else:
        await ctx.info(f"{log_prefix} Retrieved history for request: {request_key}. Length: {len(history)} messages.")

    # --- Setup Queue and Sender Task ---
    try:
        server_logger.debug(f"{log_prefix} Creating asyncio.Queue...")
        queue = asyncio.Queue(maxsize=100)
        server_logger.debug(f"{log_prefix} Queue created.")

        server_logger.debug(f"{log_prefix} Creating sender task (Log Workaround)...")
        sender_task = asyncio.create_task(
            _notification_sender_task(ctx, queue, log_prefix),
            name=f"sender_task_{short_req_id}"
        )
        server_logger.info(f"{log_prefix} Sender task created (Log Workaround).")

    except Exception as setup_err:
        server_logger.error(f"{log_prefix} FAILED during Queue/Task setup: {setup_err}", exc_info=True)
        return [types.TextContent(type="text", text="Error: Internal Server Error - Task setup failed")]

    await ctx.info(f"{log_prefix} Received query: '{query}'") # Standard log

    accumulated_full_response = "" # Changed variable name for clarity
    llm_error_occurred = False

    # Batching Variables
    MIN_BATCH_SIZE_CHARS = 30
    MAX_BATCH_DELAY_SECS = 0.2
    chunk_buffer = ""
    last_send_time = time.monotonic()

    # <<< Template unchanged >>>
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
        # --- Construct LLM Messages with History ---
        prompt_messages = []
        # Add history first
        prompt_messages.extend(history)
        # Add the current user query using the template
        prompt_messages.append({"role": "user", "content": template_query_content})
        await ctx.info(f"{log_prefix} Prepared prompt with {len(history)} history messages + current query.")

        # --- Call LLM ---
        await ctx.info(f"{log_prefix} System: Calling Local LLM ({LOCAL_LLM_MODEL_NAME})...") # Standard log
        stream_start_time = time.monotonic()

        stream = await async_openai_client.chat.completions.create(
            model=LOCAL_LLM_MODEL_NAME, messages=prompt_messages, temperature=0.6, stream=True
        )
        server_logger.debug(f"{log_prefix} LLM stream object created.")

        # --- Process LLM Stream (with BATCHING before queue) ---
        async for chunk in stream:
            content = chunk.choices[0].delta.content if chunk.choices and chunk.choices[0].delta else None

            if content:
                accumulated_full_response += content # Accumulate the *entire* response
                chunk_buffer += content # Append raw chunk to the buffer for the raw stream view

                # Check if buffer needs sending to the queue for raw stream view
                current_time = time.monotonic()
                if chunk_buffer and (
                    len(chunk_buffer) >= MIN_BATCH_SIZE_CHARS or
                    (current_time - last_send_time) > MAX_BATCH_DELAY_SECS
                ):
                    server_logger.debug(f"{log_prefix} Batch ready (Size: {len(chunk_buffer)}). Putting batch onto queue for raw stream.")
                    await queue.put(chunk_buffer) # Put the whole buffer onto the queue
                    chunk_buffer = "" # Clear the buffer
                    last_send_time = current_time # Reset timer
                    await asyncio.sleep(0) # Yield control briefly

        # --- End of Stream Processing ---
        stream_duration = (time.monotonic() - stream_start_time) * 1000
        await ctx.info(f"{log_prefix} System: Local LLM stream finished ({stream_duration:.0f} ms).") # Standard log

        # --- Flush any remaining buffer content to the queue for raw stream view ---
        if chunk_buffer:
            server_logger.debug(f"{log_prefix} Flushing final buffer content to queue for raw stream (Size: {len(chunk_buffer)}).")
            await queue.put(chunk_buffer)
            chunk_buffer = ""
            await asyncio.sleep(0)

    # --- Error Handling for LLM Call ---
    except openai.APIConnectionError as e:
        error_msg = f"Connection Error contacting LLM at {LOCAL_LLM_BASE_URL}"
        await ctx.error(f"{log_prefix} {error_msg} Details: {e}") # Standard log
        accumulated_full_response = f"Error: {error_msg}" # Store error in the full response
        llm_error_occurred = True
    except openai.APIError as e:
        error_msg = f"LLM API Error: {type(e).__name__} - {e}"
        await ctx.error(f"{log_prefix} {error_msg}") # Standard log
        accumulated_full_response = f"Error: LLM API Error ({type(e).__name__})" # Store error in the full response
        llm_error_occurred = True
    except asyncio.CancelledError:
        await ctx.warning(f"{log_prefix} Reasoning task cancelled during LLM stream.") # Standard log
        if queue: await queue.put(None)
        raise
    except Exception as e:
        error_msg = f"Unexpected error during LLM stream processing: {type(e).__name__}"
        await ctx.error(f"{log_prefix} {error_msg}", exc_info=True) # Standard log
        accumulated_full_response = f"Error: {error_msg}" # Store error in the full response
        llm_error_occurred = True

    # --- Cleanup Queue and Sender Task ---
    finally:
        try:
            if queue:
                server_logger.debug(f"{log_prefix} FINALLY: Putting None sentinel onto queue.")
                await queue.put(None)
                server_logger.debug(f"{log_prefix} FINALLY: Skipped queue.join() wait.")
            else:
                 server_logger.warning(f"{log_prefix} FINALLY: Queue object does not exist.")

            if sender_task:
                server_logger.debug(f"{log_prefix} FINALLY: Waiting for sender task to finish...")
                try:
                    await asyncio.wait_for(sender_task, timeout=5.0)
                    server_logger.debug(f"{log_prefix} FINALLY: Sender task finished gracefully.")
                except asyncio.TimeoutError:
                    server_logger.warning(f"{log_prefix} FINALLY: Sender task timed out during cleanup, cancelling.")
                    sender_task.cancel()
                    try: await sender_task
                    except asyncio.CancelledError: server_logger.debug(f"{log_prefix} FINALLY: Sender task cancellation confirmed.")
                    except Exception as final_task_err: server_logger.error(f"{log_prefix} FINALLY: Error awaiting cancelled sender task: {final_task_err}")
                except Exception as e:
                     server_logger.error(f"{log_prefix} FINALLY: Error during sender task final wait/cancel: {type(e).__name__}")
            else:
                server_logger.warning(f"{log_prefix} FINALLY: Sender task was not created, skipping cleanup.")
        except Exception as finally_err:
             server_logger.error(f"{log_prefix} FINALLY: Unexpected error during cleanup: {finally_err}", exc_info=True)


    # --- Prepare Final Tool Result (FILTERING LOGIC) ---
    final_chat_answer = ""
    if llm_error_occurred:
        final_chat_answer = accumulated_full_response
        await ctx.warning(f"{log_prefix} LLM call failed. Returning error message.")
    else:
        server_logger.debug(f"{log_prefix} Attempting to extract content between <answer> tags...")
        match = re.search(r"<answer>(.*?)</answer>", accumulated_full_response, re.DOTALL | re.IGNORECASE) # DOTALL matches newlines
        if match:
            extracted_answer = match.group(1).strip()
            final_chat_answer = extracted_answer
            server_logger.info(f"{log_prefix} Successfully extracted answer content. Length: {len(final_chat_answer)}")
            server_logger.debug(f"{log_prefix} Extracted Answer Preview: {final_chat_answer[:200]}...")
        else:
            server_logger.warning(f"{log_prefix} <answer> tags not found in the response. Returning full response as fallback.")
            final_chat_answer = accumulated_full_response.strip()

    if not final_chat_answer and not llm_error_occurred:
        await ctx.warning(f"{log_prefix} System: Final extracted answer appears empty after processing.")
        final_chat_answer = "(LLM stream returned no answer content within tags)"

    # --- Update History ---
    # Append the actual user query and the *final extracted* assistant answer
    history.append({"role": "user", "content": query}) # Use the original 'query' input parameter
    if not llm_error_occurred:
        history.append({"role": "assistant", "content": final_chat_answer}) # Use the extracted answer

    # Limit history length
    if len(history) > MAX_HISTORY_TURNS * 2:
        history = history[-(MAX_HISTORY_TURNS * 2):] # Keep the most recent turns
        await ctx.info(f"{log_prefix} Truncated history to {MAX_HISTORY_TURNS} turns.")

    session_histories[request_key] = history # Store the updated history
    await ctx.info(f"{log_prefix} Updated history for request: {request_key}. New length: {len(history)} messages.")

    # --- Return Final Result ---
    completion_timestamp = time.strftime('%H:%M:%S')
    await ctx.info(f"[{completion_timestamp} Req: {short_req_id}] Reasoning task completed. Returning final result for chat view.") # Standard log

    return [types.TextContent(type="text", text=final_chat_answer)]

# --- Main execution block ---
if __name__ == "__main__":
    # Entry point when the script is run directly
    print(f"--- Starting FastMCP Local LLM Reasoning Chat server ({SERVER_NAME}) ---")
    starlette_app = None

    # Check if MCP libraries were successfully imported
    if not MCP_AVAILABLE:
        print("CRITICAL: MCP SDK seems unavailable despite import attempt. Cannot start.")
        exit(1) # Exit if MCP is required but not available

    try:
        # Get the Starlette/FastAPI application object from the FastMCP instance
        starlette_app = mcp_server.sse_app()
        if starlette_app is None: raise RuntimeError("mcp_server.sse_app() returned None")
    except Exception as app_err:
        # Log critical error if app creation fails
        server_logger.critical(f"CRITICAL ERROR: Failed to initialize Starlette app from FastMCP: {app_err}")
        exit(1) # Exit if app cannot be created

    # Determine host, port, and log level for Uvicorn
    run_host = HOST; run_port = PORT; log_level = "info"
    if hasattr(mcp_server, 'settings'):
        run_host = getattr(mcp_server.settings, 'host', HOST)
        run_port = getattr(mcp_server.settings, 'port', PORT)
        log_level = getattr(mcp_server.settings, 'log_level', 'info').lower()

    # Print startup information
    print(f"Attempting to listen on: http://{run_host}:{run_port}")
    print(f"Server Name: {SERVER_NAME}")
    if async_openai_client: print(f"Configured for Local LLM: {LOCAL_LLM_BASE_URL} ({LOCAL_LLM_MODEL_NAME})")
    else: print("WARNING: Local LLM communication disabled.")
    print(" - Using Standard Logging Channel (ctx.session.send_log_message) for raw streaming.") # Indicate the current approach
    print(" - Binding passed via 'logger', chunk via 'data'.")
    print(" - Backend log handler MUST parse logger/data for raw stream updates.")
    print(" - Final answer for chat view is filtered for <answer> tags.") # Added note
    print(f" - In-memory history enabled, keeping last {MAX_HISTORY_TURNS} turns.")
    print(" - Styling configuration aligned with frontend feedback (using config/config.style).") # Updated note
    print("----------------------------------------------------")


    try:
        # Run the Uvicorn server
        uvicorn.run(starlette_app, host=run_host, port=run_port, log_level=log_level)
    except Exception as e:
        # Log critical error if Uvicorn fails to start
        server_logger.critical(f"Failed to run Uvicorn server ({SERVER_NAME}): {e}")
        exit(1) # Exit if server cannot start