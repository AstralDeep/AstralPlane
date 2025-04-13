# app/api/auth.py (Modified)

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

# --- Import necessary models and services ---
from ..models.schemas import (
    UserCredentials,
    UserProfile,
    Project,
    LoginResponse # <--- Import the defined LoginResponse
)

# --- MODIFIED IMPORT: Import the auth_service instance directly ---
from ..services.auth_service import (
    auth_service, # Import the instance
    get_current_user, # Required for /profile endpoint
    # authenticate_user, create_access_token, get_user_profile removed
)
# --- END MODIFICATION ---

from ..services.project_service import ProjectService # Required for /login response
from ..config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

# --- OAuth2 scheme (if using /token endpoint) ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/token") # Adjust tokenUrl if needed

# --- /token endpoint (Alternative using form data) ---
@router.post("/token", response_model=LoginResponse, include_in_schema=False) # Keep hidden if /login is primary
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """(Alternative Login) Get token, profile, project using form data."""
    logger.debug(f"Attempting login via /token for user: {form_data.username}")
    # --- MODIFIED CALL: Use auth_service instance ---
    user_dict = auth_service.authenticate_user(form_data.username, form_data.password)
    # --- END MODIFICATION ---
    if not user_dict:
        logger.warning(f"Login failed via /token for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id = user_dict.get("id")
    if not user_id:
         logger.error(f"Authenticated user dictionary missing 'id' via /token: {user_dict}")
         raise HTTPException(status_code=500, detail="Internal server error during login")

    # Create token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    # --- MODIFIED CALL: Use auth_service instance ---
    access_token = auth_service.create_access_token(data={"sub": user_id}, expires_delta=access_token_expires)
    # --- END MODIFICATION ---
    expires_at = datetime.now() + access_token_expires # Use utcnow() for consistency with token creation

    # Fetch Profile
    # --- MODIFIED CALL: Use auth_service instance ---
    user_profile = auth_service.get_user_profile(user_id)
    # --- END MODIFICATION ---
    if not user_profile:
        logger.warning(f"Could not retrieve profile for user via /token: {user_id}")
        # Consider if 500 is more appropriate if profile *should* exist after successful auth
        raise HTTPException(status_code=404, detail="User profile not found after login.")

    # Fetch Initial Project
    project_service = ProjectService()
    current_project: Optional[Project] = None
    try:
        current_project = project_service.get_project("public")
        if not current_project:
            logger.warning("Default 'public' project not found via /token.")
            # Ensure it remains None if not found
            current_project = None
    except Exception as e:
        logger.error(f"Error fetching default 'public' project via /token: {e}", exc_info=True)
        current_project = None # Ensure project is None on error


    logger.info(f"Login successful via /token for user: {form_data.username}")
    return LoginResponse(
        access_token=access_token, token_type="bearer", expires_at=expires_at,
        user=user_profile, current_project=current_project
    )

# --- Primary /login endpoint using JSON body ---
@router.post("/login", response_model=LoginResponse)
async def login(credentials: UserCredentials):
    """
    Login with username and password (JSON body).

    Returns:
        LoginResponse: Contains access token, user profile, and initial project details.
    """
    logger.debug(f"Attempting login via /login for user: {credentials.username}")
    # --- MODIFIED CALL: Use auth_service instance ---
    user_dict = auth_service.authenticate_user(credentials.username, credentials.password)
    # --- END MODIFICATION ---
    if not user_dict:
        logger.warning(f"Login failed via /login for user: {credentials.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )

    user_id = user_dict.get("id")
    if not user_id:
         logger.error(f"Authenticated user dictionary missing 'id' via /login: {user_dict}")
         raise HTTPException(status_code=500, detail="Internal server error during login")

    # Create token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    # --- MODIFIED CALL: Use auth_service instance ---
    access_token = auth_service.create_access_token(data={"sub": user_id}, expires_delta=access_token_expires)
    # --- END MODIFICATION ---
    expires_at = datetime.now() + access_token_expires # Use utcnow() for consistency

    # Fetch Profile
    # --- MODIFIED CALL: Use auth_service instance ---
    user_profile = auth_service.get_user_profile(user_id)
    # --- END MODIFICATION ---
    if not user_profile:
        logger.warning(f"Could not retrieve profile for user via /login: {user_id}")
        # Consider if 500 is more appropriate if profile *should* exist after successful auth
        raise HTTPException(status_code=404, detail="User profile not found after login.")

    # Fetch Initial Project (Assuming 'public' is default)
    project_service = ProjectService()
    current_project: Optional[Project] = None
    try:
        # Attempt to get the 'public' project
        current_project = project_service.get_project("public")
        if not current_project:
            logger.warning("Default 'public' project not found during login via /login.")
            # Ensure it remains None if explicitly not found
            current_project = None
    except Exception as e:
        # Log error but proceed, setting project to None
        logger.error(f"Error fetching default 'public' project during login: {e}", exc_info=True)
        current_project = None

    project_id_log = current_project.id if current_project else 'None'
    logger.info(f"Login successful via /login for user: {credentials.username}, Initial Project ID: {project_id_log}")

    # Return combined response
    return LoginResponse(
        access_token=access_token, token_type="bearer", expires_at=expires_at,
        user=user_profile, current_project=current_project
    )

# --- Logout endpoint ---
@router.post("/logout")
async def logout():
    """Client-side token removal indicator."""
    logger.info("Logout endpoint called.")
    # Server-side logic (e.g., token blocklisting) could be added here if needed.
    return {"status": "success", "message": "Logged out successfully"}

# --- Profile endpoints (Require Bearer Token) ---
@router.get("/profile", response_model=UserProfile)
async def get_user_profile_endpoint(current_user: Dict[str, Any] = Depends(get_current_user)):
    """Get current authenticated user's profile (Requires Bearer token)."""
    user_id = current_user.get("id")
    if not user_id:
         logger.error("User ID missing in token for GET /profile.")
         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token data.")

    logger.debug(f"Fetching profile for user ID: {user_id}")
    # --- MODIFIED CALL: Use auth_service instance ---
    profile = auth_service.get_user_profile(user_id)
    # --- END MODIFICATION ---
    if not profile:
        logger.error(f"Profile not found for validated user ID: {user_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User profile not found for authenticated user."
        )
    return profile

@router.put("/profile", response_model=UserProfile)
async def update_user_profile(
    profile_update: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Update current authenticated user's profile (Requires Bearer token)."""
    user_id = current_user.get("id")
    if not user_id:
         logger.error("User ID missing in token for PUT /profile.")
         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token data.")

    # --- MODIFIED CALL: Use auth_service instance ---
    profile = auth_service.get_user_profile(user_id)
    # --- END MODIFICATION ---
    if not profile:
        logger.error(f"Profile not found for validated user ID during update: {user_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User profile not found for authenticated user."
        )

    # --- Placeholder for actual profile update logic ---
    # Filter allowed fields and update in the repository/database
    # IMPORTANT: This requires a method on AuthService or UserRepository to handle updates.
    # Since one doesn't exist, we'll just simulate the update based on the fetched profile.
    allowed_fields = ["preference_id", "profile_tags"] # Example updatable fields
    update_data = {k: v for k, v in profile_update.items() if k in allowed_fields}

    if not update_data:
        logger.debug(f"No valid fields to update in profile for user {user_id}.")
        return profile # Return current profile if no valid fields provided

    # --- Simulate update (replace with actual repository/service call) ---
    # Example: You would need something like:
    # updated_profile = auth_service.update_profile(user_id, update_data)
    # For now, merge locally:
    current_profile_dict = profile.model_dump()
    updated_profile_dict = {**current_profile_dict, **update_data}
    # Re-validate with the model (assuming the structure matches)
    try:
        updated_profile = UserProfile(**updated_profile_dict)
        # In a real scenario, you'd save 'updated_profile' back to the repository here.
        logger.info(f"Simulated profile update for user {user_id} with data: {update_data}")
        return updated_profile
    except Exception as validation_error:
        logger.error(f"Validation error during simulated profile update for user {user_id}: {validation_error}")
        # Return the original profile or raise an error if update fails validation
        raise HTTPException(status_code=400, detail="Invalid update data provided.")