# client.py
import asyncio
import logging

from mcp import ClientSession, types
from mcp.client.sse import sse_client
from mcp.shared.context import RequestContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_URL = "http://127.0.0.1:8124/sse"


# 1. Define the sampling callback to mock the LLM response
async def mock_llm_sampling_callback(
		context: RequestContext["ClientSession", None],
		params: types.CreateMessageRequestParams,
) -> types.CreateMessageResult | types.ErrorData:
	"""Mocks the LLM response for sampling/createMessage requests."""
	# Extract the user's message from the request parameters
	user_message = "No message found"
	if params.messages:
		content = params.messages[-1].content  # Get the last message content
		if isinstance(content, types.TextContent):
			user_message = content.text

	logger.info(f"Client sampling_callback received request for: '{user_message}'")

	# Create a mock response
	mock_response_text = f"This is a mocked response to: '{user_message}'"

	logger.info(f"Client sampling_callback sending mock response: '{mock_response_text}'")

	# Return the mocked response in the required format
	return types.CreateMessageResult(
		role="assistant",
		content=types.TextContent(type="text", text=mock_response_text),
		model="mock-model",  # Specify the model used (even if mocked)
		stopReason="endTurn",  # Indicate why generation stopped
	)


async def main():
	logger.info(f"Connecting to server at {SERVER_URL}")
	try:
		# 2. Use the sse_client context manager
		async with sse_client(SERVER_URL) as (read_stream, write_stream):
			logger.info("SSE connection established")
			# 3. Create a ClientSession, passing the mock sampling callback
			async with ClientSession(
					read_stream, write_stream, sampling_callback=mock_llm_sampling_callback
			) as session:
				logger.info("Initializing MCP session...")
				# 4. Initialize the MCP connection
				init_result = await session.initialize()
				logger.info(f"Session initialized with server: {init_result.serverInfo}")

				# 5. Call the server's chatbot tool
				user_query = "Tell me about MCP"
				logger.info(f"Client calling 'chatbot_query' tool with query: '{user_query}'")
				tool_result = await session.call_tool(
					"chatbot_query", {"query": user_query}
				)  #

				# 6. Process the result (which came from our mock callback)
				if not tool_result.isError and tool_result.content:
					final_response = tool_result.content[0]
					if isinstance(final_response, types.TextContent):
						logger.info(f"Client received final result from server tool: '{final_response.text}'")
					else:
						logger.warning(f"Received unexpected content type: {type(final_response)}")
				elif tool_result.isError:
					logger.error(f"Tool call failed: {tool_result.content}")
				else:
					logger.warning("Tool call returned no content")

	except Exception as e:
		logger.error(f"An error occurred: {e}", exc_info=True)


if __name__ == "__main__":
	asyncio.run(main())
