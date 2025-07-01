# app/services/auth_service.py (Complete File with Enhanced WS Auth Logging)
from fastapi import Depends, HTTPException, status, WebSocket, Query  # Added Query for potential future use
from fastapi.security import OAuth2PasswordBearer
import jwt
from jwt.exceptions import PyJWTError
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
import logging

# --- Ensure correct relative imports for your project structure ---
try:
	from ..models.schemas import UserProfile, TokenData
	from ..config import settings
	from ..repositories.user_repository import UserRepository, get_user_repository
except (ImportError, ValueError) as e:
	# Add basic fallbacks if imports fail during development/testing outside main app context
	# Or raise a more specific error if these are critical
	print(f"Warning: Failed to import schemas/config/repo in auth_service ({e}). Using fallbacks.")


	class UserProfile:
		pass


	class TokenData:
		pass


	class Settings:
		SECRET_KEY = "fallback"
		ALGORITHM = "HS256"
		ACCESS_TOKEN_EXPIRE_MINUTES = 1


	settings = Settings()


	class UserRepository:
		def verify_password(self, p, h): return False

		def get_by_username(self, u): return None

		def get_user_profile(self, u): return None


	def get_user_repository():
		return UserRepository()

logger = logging.getLogger(__name__)

# OAuth2 password bearer for token authentication (primarily for REST)
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token")  # Adjust if your token URL is different


class AuthenticationError(Exception):
	"""Exception raised for authentication errors"""
	pass


class AuthService:
	"""
    Service for user authentication and authorization
    """
	# Singleton instance
	_instance = None

	def __new__(cls, repository=None):
		if cls._instance is None:
			cls._instance = super(AuthService, cls).__new__(cls)
			cls._instance._initialized = False
		return cls._instance

	def __init__(self, repository: Optional[UserRepository] = None):
		if getattr(self, "_initialized", False):
			return
		# Ensure get_user_repository() is callable and returns a valid repository
		try:
			self.repository = repository or get_user_repository()
			logger.info("Auth service initialized")
		except Exception as e:
			logger.error(f"Failed to initialize AuthService repository: {e}", exc_info=True)
			# Handle error appropriately - maybe raise, or set repo to None and check later
			self.repository = None
		self._initialized = True

	def verify_password(self, plain_password: str, hashed_password: str) -> bool:
		"""Verify a password against a hash"""
		if not self.repository: return False  # Handle missing repo
		return self.repository.verify_password(plain_password, hashed_password)

	def get_user(self, username: str) -> Optional[Dict[str, Any]]:
		"""Get a user by username"""
		if not self.repository: return None  # Handle missing repo
		return self.repository.get_by_username(username)

	def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
		"""Authenticate a user with username and password"""
		if not self.repository:  # Check if repository is initialized
			logger.error("AuthService repository not initialized during authenticate_user.")
			return None
		user = self.get_user(username)
		if not user:
			return None
		# Ensure hashed_password field exists
		user_hashed_password = user.get("hashed_password")
		if not user_hashed_password:
			logger.error(f"User dictionary for {username} missing 'hashed_password'.")
			return None
		if not self.verify_password(password, user_hashed_password):
			return None
		# Ensure 'id' key exists
		if 'id' not in user and 'user_id' in user:
			user['id'] = user['user_id']
		elif 'id' not in user:
			logger.error(f"User dictionary for {username} missing 'id' or 'user_id'.")
			return None  # Cannot proceed without an ID
		return user

	def create_access_token(self, data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
		"""Create a JWT access token"""
		to_encode = data.copy()
		if expires_delta:
			expire = datetime.utcnow() + expires_delta
		else:
			# Use expiration time from settings
			expire_minutes = settings.ACCESS_TOKEN_EXPIRE_MINUTES
			expire = datetime.utcnow() + timedelta(minutes=expire_minutes)

		to_encode.update({"exp": expire})
		encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
		return encoded_jwt

	def get_user_profile(self, user_id: str) -> Optional[UserProfile]:
		"""Get user profile data"""
		if not self.repository: return None  # Handle missing repo
		return self.repository.get_user_profile(user_id)

	def validate_token(self, token: str, context: str = "Unknown") -> Dict[str, Any]:
		"""
        Core token validation function. Raises AuthenticationError on failure.
        Added context for logging.
        """
		logger.debug(f"({context}) Attempting to validate token: {token[:10]}...")  # Log start and context
		try:
			# Decode JWT token
			payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
			logger.debug(f"({context}) Token payload decoded: {payload}")

			# Extract and validate user ID (should be 'sub')
			user_id: str = payload.get("sub")  # Standard JWT claim for subject (user id)
			if user_id is None:
				logger.warning(f"({context}) Failed authentication: Missing 'sub' claim in token payload.")
				raise AuthenticationError("Invalid token: missing user identifier (sub)")

			# Check token expiration (JWT library does this, but log explicitly)
			exp = payload.get("exp")
			if not exp:
				logger.warning(f"({context}) Failed authentication: Missing 'exp' claim in token payload.")
				raise AuthenticationError("Invalid token: missing expiration")

			# Use utcfromtimestamp for consistency with JWT standard
			token_expiry = datetime.utcfromtimestamp(exp)
			if token_expiry < datetime.utcnow():
				logger.warning(f"({context}) Failed authentication: Token expired at {token_expiry.isoformat()}Z")
				raise AuthenticationError(f"Token expired at {token_expiry.isoformat()}Z")

			# Get user profile from repository using user_id
			logger.debug(f"({context}) Attempting to fetch profile for user ID: {user_id}")
			# Ensure repository instance is available
			if not self.repository:
				logger.error(f"({context}) User repository not available during token validation.")
				raise AuthenticationError("Internal server error: User repository unavailable")

			user_profile = self.repository.get_user_profile(user_id)
			if not user_profile:
				logger.warning(
					f"({context}) Failed authentication: User profile not found in repository for user ID: {user_id}")
				raise AuthenticationError(f"User not found: {user_id}")

			# Convert profile to dictionary for consistent return type (Pydantic v2+)
			try:
				# Check if it's a Pydantic model before calling model_dump
				if hasattr(user_profile, 'model_dump') and callable(user_profile.model_dump):
					user_dict = user_profile.model_dump()
				elif isinstance(user_profile, dict):  # If repo already returns dict
					user_dict = user_profile
				else:
					logger.error(
						f"({context}) Unexpected type for user_profile: {type(user_profile)}. Cannot convert to dict.")
					raise AuthenticationError("Internal server error: Invalid user profile format.")

			except Exception as dump_err:  # Catch potential errors during model_dump
				logger.error(f"({context}) Error converting user profile to dict: {dump_err}", exc_info=True)
				raise AuthenticationError("Internal server error: Failed processing user profile.")

			logger.info(
				f"({context}) Token successfully validated for user: {user_id}, expires: {token_expiry.isoformat()}Z")
			return user_dict  # Return the profile dictionary

		except PyJWTError as e:
			logger.error(f"({context}) JWT validation error: {str(e)}")
			raise AuthenticationError(f"Invalid token: {str(e)}")
		except AuthenticationError as e:
			# Log and re-raise specific authentication errors
			logger.warning(f"({context}) AuthenticationError during token validation: {str(e)}")
			raise
		except Exception as e:
			# Catch any other unexpected errors during validation
			logger.error(f"({context}) Unexpected error during token validation: {str(e)}", exc_info=True)
			raise AuthenticationError(f"Unexpected authentication error: {str(e)}")


# --- Create global instance ---
# Ensure repository is injected or available when AuthService is first called
# If get_user_repository raises an error, auth_service.repository might be None
try:
	auth_service = AuthService()
except Exception as init_err:
	logger.critical(f"CRITICAL: Failed to initialize global AuthService instance: {init_err}", exc_info=True)
	# Depending on application requirements, might re-raise or handle differently
	auth_service = None  # Ensure it's None if initialization fails critically


# --- WebSocket Authentication Function (REPLACEMENT STARTS HERE) ---
async def authenticate_websocket(websocket: WebSocket) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
	"""
    Authenticate a WebSocket connection using header or query param token.
    Returns (success, user_data, error_message)
    Enhanced logging and error handling.
    """
	client_info = f"{websocket.client.host}:{websocket.client.port}" if websocket.client else "unknown"
	logger.debug(f"WS Auth ({client_info}): Entering authenticate_websocket function.")  # <<< Added Entry Log

	token: Optional[str] = None
	token_source: Optional[str] = None

	try:
		# Try Authorization header first
		auth_header = websocket.headers.get("authorization")
		if auth_header:
			scheme, _, credentials = auth_header.partition(' ')
			if scheme.lower() == "bearer":
				token = credentials
				token_source = "Authorization header"
				logger.debug(f"WS Auth ({client_info}): Found token in {token_source}")

		# If no header token, check query parameters
		if not token:
			token = websocket.query_params.get("token")
			if token:
				token_source = "URL query parameter"
				logger.debug(f"WS Auth ({client_info}): Found token in {token_source}")

		# If still no token
		if not token:
			logger.warning(f"WS Auth Failed ({client_info}): No token provided in header or URL.")
			return False, None, "Authentication required: No token provided."

		logger.debug(f"WS Auth ({client_info}): Attempting validation for token from {token_source}...")

		# --- Use the auth_service instance directly ---
		# Check if auth_service was initialized correctly
		if not auth_service or not auth_service.repository:
			logger.error(f"WS Auth Error ({client_info}): AuthService or its repository not available.")
			# Don't expose internal state, return a generic error
			return False, None, "Authentication service unavailable."

		# This call might raise AuthenticationError or other exceptions
		user_data = auth_service.validate_token(token, context="WebSocket")

		user_id = user_data.get('id', 'unknown')  # Safely get id for logging
		logger.info(f"WS Auth Successful ({client_info}): Authenticated user {user_id}")
		return True, user_data, None  # Success

	except AuthenticationError as e:
		# Log specific authentication errors from validate_token
		logger.warning(f"WS Auth Failed ({client_info}): AuthenticationError: {str(e)}")
		# Return the specific error message from the exception
		return False, None, str(e)
	except PyJWTError as e:
		# Catch JWT specific errors that might occur before AuthenticationError is raised
		logger.error(f"WS Auth Failed ({client_info}): JWTError during validation: {str(e)}",
					 exc_info=True)  # Log traceback for JWT errors
		return False, None, f"Invalid token format or signature."  # Keep error message generic
	except Exception as e:
		# Catch any other unexpected errors during the process
		logger.error(f"WS Auth Error ({client_info}): Unexpected exception in authenticate_websocket: {e}",
					 exc_info=True)
		# Return a generic error message to prevent potential info leaks
		return False, None, "Internal server error during authentication."


# --- WebSocket Authentication Function (REPLACEMENT ENDS HERE) ---


# --- Helper functions for FastAPI dependency injection (for REST endpoints) ---
def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
	"""Get the current user from JWT token for REST endpoints"""
	logger.debug("REST API authentication attempt with token")  # Changed level to debug
	try:
		# Use the auth_service instance directly, pass context
		if not auth_service:  # Check instance availability
			logger.error("REST Auth Failed: AuthService not initialized.")
			raise AuthenticationError("Authentication service unavailable.")

		return auth_service.validate_token(token, context="REST")
	except AuthenticationError as e:
		# Convert to HTTP exception for REST endpoints
		logger.warning(f"REST Auth Failed: {str(e)}")  # Log REST auth failure
		raise HTTPException(
			status_code=status.HTTP_401_UNAUTHORIZED,
			detail=str(e),  # Pass the specific auth error message
			headers={"WWW-Authenticate": "Bearer"},
		)
	except Exception as e:
		# Catch unexpected errors during REST auth
		logger.error(f"REST Auth Error: Unexpected error during token validation: {e}", exc_info=True)
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
			detail="Internal server error during authentication.",
		)


async def get_user_id(current_user: Dict[str, Any] = Depends(get_current_user)) -> str:
	"""Extract user ID from current user data (obtained via get_current_user)"""
	# current_user should be valid if get_current_user didn't raise HTTPException
	user_id = current_user.get("id")
	if not user_id:
		# This case should ideally not happen if validate_token ensures 'id' exists
		logger.error(
			"User ID missing from validated token data in get_user_id. This indicates an issue in validate_token or user profile structure.")
		raise HTTPException(
			status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,  # Use 500 as it's an internal logic error
			detail="Internal server error: Invalid user data.",
		)
	return user_id
