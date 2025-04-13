import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Callable, Optional, List
import time
import uuid # <-- Import uuid
from fastapi import Depends

from ..config import settings

# Import your Dataplane class (customized to your needs)
try:
    from your_library.dataplane import DataplaneTest

    DATAPLANE_AVAILABLE = True
except ImportError:
    DATAPLANE_AVAILABLE = False
    logging.warning("Dataplane library not available, using simulation mode")

logger = logging.getLogger(__name__)


class AsyncDataplaneAdapter:
    """
    Adapter class to make the Dataplane service work with async/await pattern
    and maintain view state
    """

    def __init__(self, stream_config: Dict[str, Any], client=None):
        """Initialize the adapter with stream configuration"""
        self.stream_config = stream_config
        self.text_callback = None
        self.binary_callback = None
        self.client = client
        self.dataplane = None
        self.connected = False
        self.loop = asyncio.get_event_loop()

        # Generate a unique identifier for this connection
        self.id = f"{stream_config['ident_key']}:{stream_config['ident_id']}"
        self.start_time = time.time()

        # Track active views to maintain state across interactions
        self.active_views = {}

        # Session ID for this connection
        self.session_id = f"session_{uuid.uuid4().hex[:8]}"

        # Project ID if this is a project stream
        self.project_id = stream_config['ident_id'] if stream_config['ident_key'] == 'project' else None

    async def connect(self, text_callback: Callable, binary_callback: Callable) -> bool:
        """Connect to the dataplane with async callbacks"""
        self.text_callback = text_callback
        self.binary_callback = binary_callback

        if not DATAPLANE_AVAILABLE or settings.TESTING_MODE:
            logger.info(f"Using simulated dataplane for stream {self.id}")
            # In testing mode, we use a simulated dataplane
            self.connected = True

            # Send an initial metadata message
            await self.text_callback(json.dumps({
                "type": "metadata",
                "sessionId": self.session_id,
                "timestamp": time.time(),
                "simulation": True,
                "project_id": self.project_id
            }))

            return True

        try:
            # Wrap synchronous callbacks to work with async
            def text_callback_wrapper(message):
                asyncio.run_coroutine_threadsafe(
                    self.text_callback(message), self.loop)

            def binary_callback_wrapper(data):
                asyncio.run_coroutine_threadsafe(
                    self.binary_callback(data), self.loop)

            # Create real DataplaneTest instance
            self.dataplane = DataplaneTest(
                client=self.client,
                logger=logger
            )

            # Setup dataplane callbacks
            self.dataplane.text_callback = text_callback_wrapper
            self.dataplane.binary_callback = binary_callback_wrapper

            # Setup dataplane configuration
            stream_name = self.dataplane.setup_dataplane()
            logger.info(f"Dataplane setup complete for {self.id}, stream name: {stream_name}")

            # Connect to dataplane (run in thread to avoid blocking)
            connected = await self.loop.run_in_executor(
                None,
                lambda: self.dataplane.dp.connect()
            )

            self.connected = connected
            if connected:
                logger.info(f"Successfully connected to dataplane: {self.id}")
            else:
                logger.error(f"Failed to connect to dataplane: {self.id}")

            return connected

        except Exception as e:
            logger.error(f"Error connecting to dataplane: {e}", exc_info=True)
            self.connected = False
            return False

    async def _handle_project_message(self, message_data):
        """Handle project-related messages"""
        msg_type = message_data.get("action")

        if msg_type == "connect":
            project_id = message_data.get("project_id")
            return await self._connect_to_project(project_id)
        elif msg_type == "switch":
            old_project_id = message_data.get("old_project_id")
            new_project_id = message_data.get("new_project_id")
            return await self._switch_project(old_project_id, new_project_id)
        else:
            logger.warning(f"Unknown project message type: {msg_type}")
            return False

    async def _connect_to_project(self, project_id):
        """Connect to a specific project"""
        if not project_id:
            logger.error("Missing project_id for project connection")
            return False

        try:

            # Clear previous active views
            self.active_views = {}

            # Store the project ID
            self.project_id = project_id

            # We can't access the WebSocket directly here, so we'll send a message
            # to the frontend to request a reconnection to the project-specific stream
            await self.text_callback(json.dumps({
                "type": "reconnect_request",
                "payload": {
                    "stream_id": f"project:{project_id}",
                    "project_id": project_id
                }
            }))

            return True
        except Exception as e:
            logger.error(f"Error connecting to project {project_id}: {e}")
            return False

    async def _switch_project(self, old_project_id, new_project_id):
        """Switch from one project to another"""
        if old_project_id == new_project_id:
            return True

        try:
            # Send clear_all_views message
            await self.text_callback(json.dumps({
                "type": "clear_all_views",
                "payload": {"reason": "project_switch"}
            }))

            # Connect to new project
            return await self._connect_to_project(new_project_id)
        except Exception as e:
            logger.error(f"Error switching projects: {e}")
            return False

    async def send(self, message: str) -> bool:
        """Send a text message to the dataplane"""
        if not self.connected:
            logger.warning(f"Cannot send message, dataplane not connected: {self.id}")
            return False

        try:
            if not DATAPLANE_AVAILABLE or settings.TESTING_MODE:
                # In simulation mode, handle message processing
                logger.debug(f"Simulated dataplane received message: {message}")

                # Parse the message to determine response
                try:
                    json_data = json.loads(message)
                    msg_type = json_data.get("type", "unknown")

                    # Handle user input messages
                    if msg_type == "user_input":
                        content = json_data.get("content", "")
                        view_id = json_data.get("view_id", "")

                        logger.info(f"Processing user input from view {view_id}: {content}")

                        # Process user input in simulation mode
                        await self._process_user_input(content, view_id)
                        return True

                    # Handle project-related messages
                    elif msg_type == "project":
                        return await self._handle_project_message(json_data)

                    # Handle other message types - forward to appropriate handlers
                    elif msg_type == "view_action":
                        await self._handle_view_action(json_data)
                        return True

                    # Default behavior - echo back with a delay
                    await asyncio.sleep(0.5)
                    response = {
                        "type": "response",
                        "original_type": msg_type,
                        "timestamp": time.time(),
                        "content": f"Simulated response to: {json_data.get('content', 'no content')}"
                    }
                    await self.text_callback(json.dumps(response))
                    return True

                except json.JSONDecodeError:
                    # Not JSON, echo as plain text
                    await asyncio.sleep(0.5)
                    await self.text_callback(f"Echo: {message}")
                    return True

            # Send through real dataplane
            await self.loop.run_in_executor(
                None,
                lambda: self.dataplane.dp.send(message)
            )
            return True

        except Exception as e:
            logger.error(f"Error sending message to dataplane: {e}")
            return False

    async def _process_user_input(self, content: str, view_id: str):
        """
        Process user input in simulation mode

        Args:
            content: User input content
            view_id: View ID that sent the input
        """
        # Special handling for our MCP chat view
        if view_id == "mcp-chat-view":
            # Create history entries for question and response
            question_entry = {
                "id": f"msg_{uuid.uuid4().hex}", # <-- Added unique ID
                "role": "user",
                "content": content,
                "timestamp": datetime.now().isoformat()
            }

            # Send question to history
            await self.text_callback(json.dumps({
                "type": "section_update",
                "payload": {
                    "viewId": "mcp-chat-view",
                    "sectionId": "history",
                    "updateType": "append",
                    "content": question_entry # Send entry with ID
                }
            }))

            # Simulate processing time
            await asyncio.sleep(0.5)

            # Send response
            response_entry = {
                "id": f"msg_{uuid.uuid4().hex}", # <-- Added unique ID
                "role": "assistant",
                "content": f"You asked: {content}",
                "timestamp": datetime.now().isoformat()
            }

            await self.text_callback(json.dumps({
                "type": "section_update",
                "payload": {
                    "viewId": "mcp-chat-view",
                    "sectionId": "history",
                    "updateType": "append",
                    "content": response_entry # Send entry with ID
                }
            }))

            return

        # Add system log message
        if "log" in self.active_views:
            log_view_id = self.active_views["log"]
            log_update = {
                "type": "view_update",
                "payload": {
                    "id": log_view_id,
                    "content": {
                        "logs": [
                            {
                                "level": "info",
                                "message": f"Processing input: {content}",
                                "timestamp": time.time()
                            }
                        ],
                        "append": True
                    }
                }
            }
            await self.text_callback(json.dumps(log_update))
            await asyncio.sleep(0.5)

        # Generate simulated AI thinking steps in the log
        if "log" in self.active_views:
            log_view_id = self.active_views["log"]
            thinking_steps = [
                "Analyzing request...",
                "Retrieving relevant information...",
                "Generating response..."
            ]

            for step in thinking_steps:
                log_update = {
                    "type": "view_update",
                    "payload": {
                        "id": log_view_id,
                        "content": {
                            "logs": [
                                {
                                    "level": "info",
                                    "message": step,
                                    "timestamp": time.time()
                                }
                            ],
                            "append": True
                        }
                    }
                }
                await self.text_callback(json.dumps(log_update))
                await asyncio.sleep(0.5)

        # Generate response based on input
        if "response" in self.active_views:
            response_view_id = self.active_views["response"]

            # Simple response generation based on input
            response_text = self._generate_simulated_response(content)

            response_update = {
                "type": "view_update",
                "payload": {
                    "id": response_view_id,
                    "content": {
                        "response": response_text,
                        "metadata": {
                            "generated_at": time.time(),
                            "model": "simulation-model"
                        }
                    }
                }
            }
            await self.text_callback(json.dumps(response_update))

        # Final log message
        if "log" in self.active_views:
            log_view_id = self.active_views["log"]
            log_update = {
                "type": "view_update",
                "payload": {
                    "id": log_view_id,
                    "content": {
                        "logs": [
                            {
                                "level": "success",
                                "message": "Response generated successfully",
                                "timestamp": time.time()
                            }
                        ],
                        "append": True
                    }
                }
            }
            await self.text_callback(json.dumps(log_update))

    def _generate_simulated_response(self, input_text: str) -> str:
        """
        Generate a simulated AI response based on user input

        Args:
            input_text: User input text

        Returns:
            Simulated response text
        """
        # Simple keyword-based response simulation
        input_lower = input_text.lower()

        if "hello" in input_lower or "hi" in input_lower:
            return "Hello! I'm a simulated AI assistant. How can I help you today?"

        elif "help" in input_lower:
            return "I'm here to help! You can ask me questions, and I'll do my best to provide useful information. What would you like to know?"

        elif "project" in input_lower:
            return f"I'm currently connected to project {self.project_id}. This is a simulated project environment where we can test various features and interactions."

        elif "view" in input_lower:
            return "The interface uses a dynamic view system. Views can be created, updated, and removed based on interactions. Each view has a specific purpose, like showing input, responses, or logs."

        elif "how" in input_lower and "work" in input_lower:
            return "The system works by sending messages between the frontend and backend through WebSockets. When you submit input, it's processed and appropriate view updates are sent back to display the results."

        elif any(word in input_lower for word in ["weather", "temperature", "forecast"]):
            return "I don't have access to real-time weather data in simulation mode, but I can tell you that it's always sunny in the world of code!"

        else:
            return f"Thank you for your message: \"{input_text}\". This is a simulated response in the project environment. In a real implementation, this would connect to actual AI services to generate relevant responses."

    async def _handle_view_action(self, action_data: Dict[str, Any]):
        """
        Handle view action events from the frontend

        Args:
            action_data: Action data from the client
        """
        action_type = action_data.get("action")
        view_id = action_data.get("view_id")
        payload = action_data.get("payload", {})

        logger.info(f"Handling view action: {action_type} for view {view_id}")

        # Example: Handle a feedback action
        if action_type == "feedback":
            feedback_type = payload.get("type")
            feedback_value = payload.get("value")

            # Log the feedback
            if "log" in self.active_views:
                log_view_id = self.active_views["log"]
                log_update = {
                    "type": "view_update",
                    "payload": {
                        "id": log_view_id,
                        "content": {
                            "logs": [
                                {
                                    "level": "info",
                                    "message": f"Received {feedback_type} feedback: {feedback_value}",
                                    "timestamp": time.time()
                                }
                            ],
                            "append": True
                        }
                    }
                }
                await self.text_callback(json.dumps(log_update))

        # Example: Handle a clear action
        elif action_type == "clear":
            target_view_id = payload.get("target_view_id")

            if target_view_id and target_view_id in [self.active_views.get(k) for k in self.active_views]:
                clear_update = {
                    "type": "view_update",
                    "payload": {
                        "id": target_view_id,
                        "content": {
                            "clear": True
                        }
                    }
                }
                await self.text_callback(json.dumps(clear_update))

    async def send_binary(self, data: bytes) -> bool:
        """Send binary data to the dataplane"""
        if not self.connected:
            logger.warning(f"Cannot send binary data, dataplane not connected: {self.id}")
            return False

        try:
            if not DATAPLANE_AVAILABLE or settings.TESTING_MODE:
                # In testing mode, we echo the binary data back
                logger.debug(f"Simulated dataplane received binary data: {len(data)} bytes")

                # Echo metadata
                await asyncio.sleep(0.5)
                await self.text_callback(json.dumps({
                    "type": "binary_metadata",
                    "content_type": "application/octet-stream",
                    "size": len(data),
                    "timestamp": time.time()
                }))

                # Echo binary data
                await asyncio.sleep(0.1)
                await self.binary_callback(data)
                return True

            # Send through real dataplane
            await self.loop.run_in_executor(
                None,
                lambda: self.dataplane.dp.send_binary(data)
            )
            return True

        except Exception as e:
            logger.error(f"Error sending binary data to dataplane: {e}")
            return False

    async def disconnect(self) -> bool:
        """Disconnect from the dataplane"""
        if not self.connected:
            return True

        try:
            if not DATAPLANE_AVAILABLE or settings.TESTING_MODE:
                # In testing mode, just mark as disconnected
                self.connected = False

                # Clear active views
                self.active_views = {}

                return True

            # Disconnect real dataplane
            await self.loop.run_in_executor(
                None,
                lambda: self.dataplane.dp.disconnect() if hasattr(self.dataplane, 'dp') else None
            )
            self.connected = False
            return True

        except Exception as e:
            logger.error(f"Error disconnecting from dataplane: {e}")
            self.connected = False  # Still mark as disconnected even if there was an error
            return False


class DataplaneManager:
    """
    Manager for dataplane connections
    Handles creating and tracking dataplane adapters
    """

    def __init__(self):
        self.connections: Dict[str, AsyncDataplaneAdapter] = {}
        self.client = self._initialize_client()

    def _initialize_client(self):
        """Initialize the dataplane client"""
        if not DATAPLANE_AVAILABLE or settings.TESTING_MODE:
            return None

        try:
            # In a real implementation, you would initialize your client library here
            # client = YourClientLibrary()
            # return client
            return None  # Replace with actual client initialization
        except Exception as e:
            logger.error(f"Error initializing dataplane client: {e}", exc_info=True)
            return None

    async def connect_dataplane(self,
                                stream_config: Dict[str, Any],
                                text_callback: Callable,
                                binary_callback: Callable) -> Optional[AsyncDataplaneAdapter]:
        """
        Connect to a dataplane stream

        Args:
            stream_config: Dictionary with stream configuration
            text_callback: Async function to call with text messages
            binary_callback: Async function to call with binary data

        Returns:
            AsyncDataplaneAdapter instance if successful, None otherwise
        """
        stream_id = f"{stream_config['ident_key']}:{stream_config['ident_id']}"

        # If already connected, return existing connection
        if stream_id in self.connections and self.connections[stream_id].connected:
            logger.info(f"Reusing existing dataplane connection for {stream_id}")
            return self.connections[stream_id]

        # Create new adapter
        adapter = AsyncDataplaneAdapter(
            stream_config=stream_config,
            client=self.client
        )

        # Connect to dataplane
        connected = await adapter.connect(text_callback, binary_callback)

        if connected:
            self.connections[stream_id] = adapter
            logger.info(f"Connected to dataplane stream: {stream_id}")
            return adapter
        else:
            logger.error(f"Failed to connect to dataplane stream: {stream_id}")
            return None

    async def disconnect_dataplane(self, stream_config: Dict[str, Any]) -> bool:
        """Disconnect from a dataplane stream"""
        stream_id = f"{stream_config['ident_key']}:{stream_config['ident_id']}"

        if stream_id in self.connections:
            # Disconnect
            result = await self.connections[stream_id].disconnect()

            # Remove from connections if successful
            if result:
                del self.connections[stream_id]

            logger.info(f"Disconnected from dataplane stream: {stream_id}, success: {result}")
            return result

        # Not connected, so consider it a success
        return True

    async def cleanup(self):
        """Clean up all connections when shutting down"""
        for stream_id, connection in list(self.connections.items()):
            try:
                await connection.disconnect()
                logger.info(f"Cleaned up dataplane connection: {stream_id}")
            except Exception as e:
                logger.error(f"Error cleaning up dataplane connection {stream_id}: {e}")

        self.connections = {}


# Dependency for FastAPI
def get_dataplane_manager():
    from ..main import app
    return app.state.dataplane_manager