"""
User repository for authentication and profile data.
"""
import logging
from typing import Dict, Any, Optional

from .base import BaseRepository, InMemoryRepository
from ..models.schemas import UserProfile

logger = logging.getLogger(__name__)


class UserRepository(BaseRepository[UserProfile]):
    """Repository for User data access."""
    
    def get_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get a user by username."""
        pass
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against a hash."""
        pass
    
    def get_user_profile(self, user_id: str) -> Optional[UserProfile]:
        """Get a user's profile."""
        pass


class InMemoryUserRepository(InMemoryRepository[UserProfile], UserRepository):
    """In-memory implementation of user repository."""
    
    def __init__(self):
        super().__init__(UserProfile)
        # Create internal user data storage
        self.users_data: Dict[str, Dict[str, Any]] = {
            "user1": {
                "id": "public_user",
                "username": "public_user",
                "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",  # "password"
                "global_role": "admin"
            },
            "user2": {
                "id": "user2",
                "username": "johndoe",
                "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW",  # "password"
                "global_role": "user"
            }
        }
        
        # Add user profiles 
        self._add_user_profiles()
    
    def _add_user_profiles(self):
        """Add user profiles for testing."""
        # User 1 profile
        profile1 = UserProfile(
            id="public_user",
            username="public_user",
            global_role="admin",
            preference_id="default",
            profile_tags=["ai", "developer", "admin"]
        )
        
        # User 2 profile
        profile2 = UserProfile(
            id="user2",
            username="johndoe",
            global_role="user",
            preference_id="default",
            profile_tags=["ai", "researcher"]
        )
        
        self.add_demo_items([profile1, profile2])
    
    def get_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get a user by username."""
        for user_id, user_data in self.users_data.items():
            if user_data["username"] == username:
                return user_data
        return None
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verify a password against a hash."""
        # This is a placeholder - in a real app, use proper password verification
        # In a real implementation, use a password hashing library like bcrypt
        return hashed_password == "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6Lruj3vjPGga31lW"  # Any password matches in the demo
    
    def get_user_profile(self, user_id: str) -> Optional[UserProfile]:
        """Get a user's profile."""
        return self.get_by_id(user_id)


# Factory function to get the repository instance
def get_user_repository() -> UserRepository:
    """Get the user repository instance."""
    return InMemoryUserRepository()
