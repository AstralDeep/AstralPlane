#  mcp_postgres.py

import asyncio
import json
import logging
import os
import sys  # For sys.exit()
from typing import Optional, List

from dotenv import load_dotenv
import asyncpg
import openai  # For OpenAI
from openai import AsyncOpenAI  # For async OpenAI calls

# --- MCP Imports ---
import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.shared.context import RequestContext

# Assuming these are part of your project structure
# If these are not found, the script uses a basic logging fallback.
try:
	from app.config import settings
	from app.utils.logging_config import configure_logging

	custom_config_loaded = True
except ImportError:
	custom_config_loaded = False
	settings = None  # Placeholder

# Load environment variables from .env_pg file
load_dotenv(".env_pg")  # UPDATED TO .env_pg as per your last file

# --- Configuration ---
# PostgreSQL connection details from .env_pg
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# MCP Server configuration from .env_pg
SERVER_NAME = os.getenv("MCP_SERVER_NAME", "mcp_postgres_llm")
HOST = os.getenv("MCP_HOST", "127.0.0.1")
PORT = int(os.getenv("MCP_PORT", "8127"))

# LLM Configuration from .env_pg
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "")

# --- Logging Setup ---
log_level_name = os.getenv("LOG_LEVEL", "DEBUG").upper()
log_level = getattr(logging, log_level_name, logging.INFO)

if custom_config_loaded:
	try:
		log_dir_path = os.getenv("LOG_DIR", "logs")
		debug_mode_for_logging = getattr(settings, 'DEBUG', True) if settings else True
		configure_logging(log_level=log_level, log_to_file=debug_mode_for_logging, log_dir=log_dir_path)
	except Exception as e:
		logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		logging.error(f"Error configuring custom logging with configure_logging: {e}. Using basic logging.",
					  exc_info=True)
		custom_config_loaded = False
else:
	logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	log_dir_path = os.getenv("LOG_DIR", "logs")
	if log_level <= logging.DEBUG:
		try:
			if not os.path.exists(log_dir_path): os.makedirs(log_dir_path, exist_ok=True)
			fh = logging.FileHandler(os.path.join(log_dir_path, f"{SERVER_NAME}.log"))
			fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
			logging.getLogger().addHandler(fh)
		except Exception as e:
			logging.error(f"Failed to set up basic file logging: {e}", exc_info=True)
	logging.warning(
		"Custom 'app.config.settings' or 'app.utils.logging_config' not found or failed. Using basic logging setup.")

logger = logging.getLogger(f"app.mcp_servers.{SERVER_NAME}")
logger.info(f"Logging configured for {SERVER_NAME}. Level: {log_level_name}")

# --- FastMCP Server Instance ---
mcp_server = FastMCP(
	name=SERVER_NAME,
	host=HOST,
	port=PORT,
	log_level="DEBUG",
	dependencies=["uvicorn"]
)

starlette_app = mcp_server.sse_app()
# Initialize DB pool & cleanup on startup/shutdown
starlette_app.add_event_handler("startup", lambda: asyncio.create_task(init_db_pool()))
starlette_app.add_event_handler("shutdown", lambda: asyncio.create_task(close_db_pool()))

# --- LLM Client (AsyncOpenAI) Initialization ---
async_llm_client: Optional[AsyncOpenAI] = None
should_attempt_llm_initialization = False

if LLM_BASE_URL:
	should_attempt_llm_initialization = True
	logger.info(f"LLM_BASE_URL ('{LLM_BASE_URL}') is set. Will attempt LLM client initialization.")
	if not LLM_API_KEY:
		logger.warning("LLM_API_KEY is empty. This is assumed to be acceptable for the endpoint at LLM_BASE_URL.")
elif LLM_API_KEY:
	should_attempt_llm_initialization = True
	logger.info(
		"LLM_API_KEY is set (no LLM_BASE_URL). Will attempt LLM client initialization (likely for OpenAI's public API).")
else:
	logger.warning(
		"Neither a non-empty LLM_API_KEY (for OpenAI-like services) nor an LLM_BASE_URL is set. LLM client will not be initialized.")

if should_attempt_llm_initialization:
	try:
		current_api_key = LLM_API_KEY if LLM_API_KEY else None
		client_args = {"api_key": current_api_key}
		if LLM_BASE_URL:
			client_args["base_url"] = LLM_BASE_URL

		async_llm_client = AsyncOpenAI(**client_args)
		model_display_name = f"'{LLM_MODEL}'" if LLM_MODEL else "'[default/empty string as per your endpoint's convention]'"
		logger.info(f"LLM client instance created. Effective model for API calls: {model_display_name}.")
		if not LLM_BASE_URL and not LLM_MODEL:
			logger.warning(
				"Warning: LLM_MODEL is an empty string and LLM_BASE_URL is not set. "
				"This configuration is unlikely to work with the public OpenAI API, which typically requires a specific model name. "
				"API calls may fail."
			)
		elif LLM_BASE_URL and not LLM_MODEL:
			logger.info(
				"Note: LLM_MODEL is an empty string. This will be passed to the LLM endpoint at "
				f"'{LLM_BASE_URL}' and is expected to be interpreted as a request for its default model."
			)
	except Exception as e:
		logger.error(f"Failed to create LLM client instance: {e}", exc_info=True)
		async_llm_client = None

# --- Database Schema Description (CRITICAL for NL-to-SQL) ---
DATABASE_SCHEMA_DESCRIPTION = """
-- Table: public.enrollments
-- Columns:
--   enrollment_uuid (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the enrollment.
--   participant_uuid (uuid, NOT NULL, FOREIGN KEY references public.participants.participant_uuid): Identifier for the participant.
--   protocol_type_uuid (uuid, NOT NULL, FOREIGN KEY references public.protocol_types.protocol_type_uuid): Identifier for the protocol type.
--   status (boolean, NOT NULL): Current status of the enrollment.

-- Table: public.messages
-- Columns:
--   message_uuid (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the message.
--   participant_uuid (uuid, NOT NULL, FOREIGN KEY references public.participants.participant_uuid): Identifier for the participant.
--   ts (timestamp without time zone, NOT NULL): Timestamp of when the message was recorded.
--   message_direction (character(8), NOT NULL): Direction of the message (e.g., incoming, outgoing).
--   message_json (jsonb, NOT NULL): Content of the message in JSON format.
--   study (character varying(255), NOT NULL): Identifier for the study.

-- Table: public.metrics
-- Columns:
--   metric_id (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the metric.
--   participant_id (uuid, FOREIGN KEY references public.participants.participant_uuid): Identifier for the participant.
--   ts (timestamp without time zone, NOT NULL, DEFAULT (now() AT TIME ZONE 'utc'::text)): Timestamp of when the metric was recorded.
--   metric_json (jsonb, NOT NULL): Content of the metric in JSON format.

-- Table: public.participants
-- Columns:
--   participant_uuid (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the participant.
--   study (character varying(255), NOT NULL): Identifier for the study.
--   participant_json (jsonb, NOT NULL): Details of the participant in JSON format.

-- Table: public.protocol_types
-- Columns:
--   protocol_type_uuid (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the protocol type.
--   study (character varying(255), NOT NULL): Identifier for the study.
--   name (character varying(255), NOT NULL, UNIQUE): Name of the protocol type.

-- Table: public.queued_messages
-- Columns:
--   message_uuid (uuid, PRIMARY KEY, NOT NULL): Unique identifier for the queued message.
--   participant_uuid (uuid, NOT NULL, FOREIGN KEY references public.participants.participant_uuid): Identifier for the participant.
--   tonumber (character(12), NOT NULL): Recipient phone number.
--   fromnumber (character(12), NOT NULL): Sender phone number.
--   scheduledfor (timestamp without time zone, NOT NULL): Timestamp for when the message is scheduled to be sent.
--   message_json (jsonb, NOT NULL): Content of the message in JSON format.
--   study (character varying(255), NOT NULL): Identifier for the study.

-- Table: public.save_state
-- Columns:
--   enrollment_uuid (uuid, NOT NULL, FOREIGN KEY references public.enrollments.enrollment_uuid): Identifier for the enrollment.
--   ts (timestamp without time zone, NOT NULL): Timestamp of when the state was saved.
--   state_json (jsonb, NOT NULL): Content of the saved state in JSON format.

-- Table: public.state_log
-- Columns:
--   participant_uuid (uuid, NOT NULL): Identifier for the participant.
--   ts (timestamp without time zone, NOT NULL): Timestamp of the log entry.
--   log_json (jsonb, NOT NULL): Content of the log in JSON format.

-- Table: public.surveys
-- Columns:
--   token (uuid, NOT NULL): Unique token for the survey instance.
--   participant_uuid (uuid, NOT NULL): Identifier for the participant.
--   created_at (timestamp without time zone, NOT NULL): Timestamp of when the survey was created.
--   finished_at (timestamp without time zone): Timestamp of when the survey was finished (nullable).
--   survey_json (jsonb): Content of the survey in JSON format (nullable).

-- Table: public.time_zones
-- Columns:
--   time_zone (character varying(64), NOT NULL): Full name of the time zone.
--   short_zone (character varying(32), NOT NULL): Abbreviated name of the time zone.
--   CONSTRAINT time_zones_time_zone_short_zone_key UNIQUE (time_zone, short_zone)

-- Table: public.user_roles
-- Columns:
--   role_id (integer, PRIMARY KEY, NOT NULL): Unique identifier for the user role.
--   role_name (character varying(255), NOT NULL): Name of the user role.

-- Table: public.user_sessions
-- Columns:
--   session_id (character varying(36), PRIMARY KEY, NOT NULL): Unique identifier for the user session.
--   user_id (character varying(255), NOT NULL, FOREIGN KEY references public.users.id): Identifier for the user.
--   expires (timestamp without time zone, NOT NULL): Expiration timestamp for the session.
--   token (jsonb, NOT NULL): Token details in JSON format.

-- Table: public.users
-- Columns:
--   id (character varying(255), PRIMARY KEY, NOT NULL): Unique identifier for the user.
--   email (character varying(255)): User's email address (nullable).
--   phone_number (character varying(255)): User's phone number (nullable).
--   timezone (character varying(255)): User's timezone (nullable).
--   first_name (character varying(255)): User's first name (nullable).
--   last_name (character varying(255)): User's last name (nullable).
--   full_name (character varying(255)): User's full name (nullable).
--   eppn (character varying(255)): eduPersonPrincipalName (nullable).
--   idp (character varying(255)): Identity provider identifier (nullable).
--   idp_name (character varying(255)): Name of the identity provider (nullable).
--   affiliation (character varying(255)): User's affiliation (nullable).
--   roles (integer[], DEFAULT ARRAY[0]): Array of role IDs associated with the user.

-- Relationships (FOREIGN KEYS) are described within the column definitions where applicable.
-- JSONB fields might require queries to access specific keys using operators like '->>' or '->'.
"""

# --- Database Connection Pool ---
db_pool: Optional[asyncpg.Pool] = None


async def init_db_pool():
	global db_pool
	if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
		logger.error(
			"Essential database connection parameters (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME) missing in .env_pg. DB pool not initialized.")
		return
	try:
		db_pool = await asyncpg.create_pool(
			user=DB_USER, password=DB_PASSWORD,
			database=DB_NAME, host=DB_HOST, port=DB_PORT,
			min_size=1, max_size=10
		)
		logger.info(
			f"Successfully connected to PostgreSQL ({DB_HOST}:{DB_PORT}/{DB_NAME}) and created connection pool.")
	except Exception as e:
		logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
		db_pool = None


async def close_db_pool():
	if db_pool:
		try:
			await db_pool.close()
			logger.info("PostgreSQL connection pool closed.")
		except Exception as e:
			logger.error(f"Error closing PostgreSQL connection pool: {e}", exc_info=True)


# --- UI Structure Definition ---
def construct_db_interaction_ui_layout() -> dict:
	logger.info(f"[{SERVER_NAME}] Constructing DB Interaction UI Layout...")
	input_field_id = "db-query-input"
	submit_button_id = "db-query-submit"
	result_view_id = "db-query-result"
	action_id = "execute_db_query"
	result_binding = f"mcp_stream:{SERVER_NAME}:{result_view_id}_result"
	llm_model_display = LLM_MODEL if LLM_MODEL else "[default]"

	return {
		"id": "db-interaction-root", "type": "StackLayout",
		"config": {"direction": "vertical", "padding": "16px", "gap": "16px", "height": "100vh"},
		"children": [
			{"id": "db-title", "type": "TextView",
			 "config": {"initialText": f"Database Query ({SERVER_NAME}) via LLM ({llm_model_display})",
						"variant": "headline"}},
			{"id": input_field_id, "type": "InputField",
			 "config": {"placeholder": "Ask a question about the database...", "label": "Your Question"}},
			{"id": submit_button_id, "type": "Button", "actionId": action_id, "config": {
				"label": "Run Query", "variant": "primary",
				"valueSourceElementIds": [input_field_id],
				"frontendActions": [
					{"type": "echoToView", "sourceElementId": input_field_id, "targetBinding": result_binding,
					 "role": "user"},
					{"type": "clearElement", "targetElementId": input_field_id}
				]
			}},
			{"id": result_view_id, "type": "ChatViewBasic", "config": {
				"title": "Query Results", "autoScroll": True, "renderAsMarkdown": True,
				"style": {"minHeight": "300px", "flex": "1"}
			}, "updateBinding": result_binding}
		]
	}


# --- MCP Tools ---
@mcp_server.tool(name="get_ui_layout", description="Retrieves the UI layout for DB interaction.")
async def get_ui_layout() -> List[types.TextContent]:
	ctx: RequestContext = mcp_server.get_context()
	await ctx.info(f"[{SERVER_NAME}] get_ui_layout called.")
	layout = construct_db_interaction_ui_layout()
	return [types.TextContent(type="text", text=json.dumps(layout))]


@mcp_server.tool(
	name="execute_db_query",
	description="Interprets a user question using an LLM to generate SQL, executes it, and returns a natural language result."
)
async def execute_db_query(user_question: str) -> List[types.TextContent]:
	ctx: RequestContext = mcp_server.get_context()
	await ctx.info(f"Received database question: '{user_question}'")

	if not db_pool:
		error_msg = "Database connection is not available."
		logger.error(error_msg)  # Server-side log
		await ctx.error(error_msg)  # Client-side message
		return [types.TextContent(type="text", text=f"Error: {error_msg}")]

	if not async_llm_client:
		error_msg = "LLM client is not available. Cannot process natural language question. Check LLM configurations in .env_pg."
		logger.error(error_msg)  # Server-side log
		await ctx.error(error_msg)  # Client-side message
		return [types.TextContent(type="text", text=f"Error: {error_msg}")]

	sql_query = ""
	# === Step 1: Convert Natural Language to SQL using LLM ===
	try:
		effective_model_for_api = LLM_MODEL
		model_display_for_log = effective_model_for_api if effective_model_for_api else "[default via empty string]"
		await ctx.info(f"Converting question to SQL using LLM model: '{model_display_for_log}'...")
		nl_to_sql_prompt = f"""
Given the following PostgreSQL database schema:
{DATABASE_SCHEMA_DESCRIPTION}

Translate the user's question into a syntactically correct PostgreSQL query.
Only return the SQL query and nothing else. Do not add any explanation, markdown, or comments.
If the question cannot be answered with the provided schema, is ambiguous, or potentially harmful (e.g., attempts to modify data without clear intent from a 'show' or 'list' type question, or attempts to drop tables), return "UNABLE_TO_TRANSLATE".

User question: "{user_question}"
SQL Query:
"""
		response = await async_llm_client.chat.completions.create(
			model=effective_model_for_api,
			messages=[
				{"role": "system",
				 "content": "You are an expert PostgreSQL query generator. You strictly output only SQL queries or 'UNABLE_TO_TRANSLATE'."},
				{"role": "user", "content": nl_to_sql_prompt}
			],
			temperature=0.1, max_tokens=300, stop=None
		)
		generated_sql = response.choices[0].message.content.strip()

		if not generated_sql or generated_sql.upper() == "UNABLE_TO_TRANSLATE" or len(generated_sql) < 5:
			error_msg = "Could not translate your question to a database query. Please try rephrasing or ensure your question is related to the known database schema."
			logger.warning(
				f"LLM translation failed or indicated inability. Raw Response: '{generated_sql}' for question: '{user_question}'")
			await ctx.warning(error_msg)
			return [types.TextContent(type="text", text=error_msg)]

		sql_query = generated_sql.replace("```sql", "").replace("```", "").strip()
		await ctx.info(f"Generated SQL: {sql_query}")

	except openai.APIError as e:
		error_msg = f"LLM API error during SQL generation: {type(e).__name__} - {e}"
		logger.error(error_msg, exc_info=True)
		await ctx.error(error_msg)
		return [types.TextContent(type="text",
								  text="Sorry, I had trouble understanding your question due to an LLM API issue.")]
	except Exception as e:
		error_msg = f"Error during LLM SQL generation: {type(e).__name__} - {e}"
		logger.error(error_msg, exc_info=True)
		await ctx.error(error_msg)
		return [types.TextContent(type="text",
								  text="Sorry, I encountered an unexpected error trying to understand your question.")]

	if not sql_query:
		error_msg = "Could not generate a database query for your question (SQL query is empty after generation attempt)."
		logger.warning(error_msg)
		await ctx.warning(error_msg)
		return [types.TextContent(type="text", text=error_msg)]

	# === Step 2: Execute SQL Query ===
	db_results_list_of_dicts = None
	execution_status = None
	try:
		async with db_pool.acquire() as connection:
			await ctx.info(f"Executing SQL: {sql_query}")
			if sql_query.strip().upper().startswith("SELECT"):
				records = await connection.fetch(sql_query)
				db_results_list_of_dicts = [dict(record) for record in records]
				await ctx.info(f"SELECT query returned {len(db_results_list_of_dicts)} records.")
			else:
				status = await connection.execute(sql_query)
				execution_status = status
				await ctx.info(f"Non-SELECT query executed. Status: {status}")

	except asyncpg.exceptions.PostgresSyntaxError as e:
		error_msg = f"Database Syntax Error. The AI-generated query might be incorrect: {e}"
		logger.error(f"{error_msg} Faulty Query: {sql_query}", exc_info=True)
		await ctx.error(f"There was a syntax error in the generated database query. Details: {e}")
		return [types.TextContent(type="text",
								  text=f"There was a syntax error in the generated database query. Details: {e}")]
	except asyncpg.exceptions.UndefinedTableError as e:
		error_msg = f"Database Error: A table in the query does not exist. The AI might be referencing an incorrect table: {e}"
		logger.error(f"{error_msg} Faulty Query: {sql_query}", exc_info=True)
		await ctx.error(f"A table mentioned in your query doesn't seem to exist. Details: {e}")
		return [
			types.TextContent(type="text", text=f"A table mentioned in your query doesn't seem to exist. Details: {e}")]
	except Exception as e:
		error_msg = f"Database query execution failed: {type(e).__name__} - {e}"
		logger.error(f"{error_msg} Query: {sql_query}", exc_info=True)
		await ctx.error(f"Sorry, I encountered an error querying the database. Details: {e}")
		return [
			types.TextContent(type="text", text=f"Sorry, I encountered an error querying the database. Details: {e}")]

	# === Step 3: Generate Natural Language Response from DB Results using LLM ===
	try:
		effective_model_for_api = LLM_MODEL
		model_display_for_log = effective_model_for_api if effective_model_for_api else "[default via empty string]"
		await ctx.info(
			f"Generating natural language response from DB results using LLM model: '{model_display_for_log}'...")
		results_context_for_llm = ""
		if db_results_list_of_dicts is not None:
			results_json_str = json.dumps(db_results_list_of_dicts, default=str)
			max_chars_for_results = 3500
			if not db_results_list_of_dicts:
				results_context_for_llm = "The database query returned no results."
			elif len(results_json_str) > max_chars_for_results:
				sample_results = json.dumps(db_results_list_of_dicts[:3], default=str, indent=2)
				results_context_for_llm = f"The query returned {len(db_results_list_of_dicts)} records. Here's a sample of the first few records:\n{sample_results}\n... (and {len(db_results_list_of_dicts) - 3} more records, not shown due to length limit for this display)"
			else:
				results_context_for_llm = f"The query returned the following data:\n{results_json_str}"
		elif execution_status is not None:
			results_context_for_llm = f"The database command was executed with status: {execution_status}."
		else:
			results_context_for_llm = "No specific information was retrieved or modified from the database."

		nl_response_prompt = f"""
The user asked the following question:
"{user_question}"

The system generated and executed a SQL query based on this question.
The result/status from the database query is:
---
{results_context_for_llm}
---

Based *only* on these results/status, provide a concise, polite, and helpful natural language answer to the user's original question.
If the results are empty or do not directly answer the question, state that clearly. Do not invent information not present in the results.
If it was an action (like an update or delete), confirm what happened based on the status.
Keep the answer user-friendly.
Answer:
"""
		response = await async_llm_client.chat.completions.create(
			model=effective_model_for_api,
			messages=[
				{"role": "system",
				 "content": "You are a helpful assistant that explains database query results to a non-technical user in natural language."},
				{"role": "user", "content": nl_response_prompt}
			],
			temperature=0.5, max_tokens=400
		)
		natural_language_answer = response.choices[0].message.content.strip()

		await ctx.info(f"Generated Natural Language Answer: {natural_language_answer}")
		return [types.TextContent(type="text", text=natural_language_answer)]

	except openai.APIError as e:
		error_msg = f"LLM API error during response generation: {type(e).__name__} - {e}"
		logger.error(error_msg, exc_info=True)
		await ctx.error(error_msg)
		fallback_msg = "Successfully queried the database, but had an LLM API issue generating a summary."
	except Exception as e:
		error_msg = f"Error during LLM natural language response generation: {type(e).__name__} - {e}"
		logger.error(error_msg, exc_info=True)
		await ctx.error(error_msg)
		fallback_msg = "Successfully queried the database, but encountered an unexpected error generating a summary."

	# Fallback if NL generation fails
	if db_results_list_of_dicts is not None:
		return [types.TextContent(type="text",
								  text=f"{fallback_msg} Raw data: {json.dumps(db_results_list_of_dicts, default=str)}")]
	elif execution_status is not None:
		return [types.TextContent(type="text", text=f"{fallback_msg} Command execution status: {execution_status}.")]
	else:
		return [types.TextContent(type="text",
								  text=f"Sorry, I encountered an error trying to explain the results after the query.")]


# --- Main execution block ---
if __name__ == "__main__":
	logger.info(f"--- Starting FastMCP Server ({SERVER_NAME}) ---")
	uvicorn_log_level_str = os.getenv("UVICORN_LOG_LEVEL", "info").lower()
	if uvicorn_log_level_str not in ["critical", "error", "warning", "info", "debug", "trace"]:
		logger.warning(f"Invalid Uvicorn log level '{uvicorn_log_level_str}' from .env_pg, defaulting to 'info'.")
		uvicorn_log_level_str = "info"

	import uvicorn

	uvicorn.run(starlette_app, host=HOST, port=PORT, log_level=uvicorn_log_level_str)