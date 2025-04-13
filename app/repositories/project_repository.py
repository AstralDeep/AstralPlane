# app/repositories/project_repository.py
# Full replacement with updated demo data

import logging
from abc import abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

# Use absolute imports for clarity within the package
from app.repositories.base import BaseRepository, InMemoryRepository
from app.models.schemas import Project, ViewConfig, ProjectLayout

logger = logging.getLogger(__name__)

# --- ProjectRepository Interface (Includes View/Layout specific methods) ---
class ProjectRepository(BaseRepository[Project]):
    """Repository for Project data access including views and layout hints."""

    @abstractmethod
    def get_project_views(self, project_id: str) -> Dict[str, ViewConfig]:
        """Get all stored view configurations for a project."""
        pass

    @abstractmethod
    def get_project_layout(self, project_id: str) -> Optional[ProjectLayout]:
        """Get stored layout information/hints for a project."""
        pass

    @abstractmethod
    def update_project_views(self, project_id: str, views: Dict[str, Any]) -> bool:
        """Update all stored view configurations for a project."""
        pass

    @abstractmethod
    def update_project_layout(self, project_id: str, layout_data: Dict[str, Any], user_id: Optional[str] = None) -> bool:
        """Update stored layout hints for a project."""
        pass

    @abstractmethod
    def update_view(self, project_id: str, view_id: str, view_data: Dict[str, Any], user_id: Optional[str] = None) -> Optional[ViewConfig]:
        """Update a specific stored view configuration in a project."""
        pass

    @abstractmethod
    def delete_view(self, project_id: str, view_id: str) -> bool:
        """Delete a stored view configuration from a project."""
        pass

    @abstractmethod
    def get_user_projects(self, user_id: str, skip: int = 0, limit: int = 100) -> List[Project]:
        """Get all projects a user has access to."""
        pass

    @abstractmethod
    def user_has_access(self, user_id: str, project_id: str) -> bool:
        """Check if a user has access to a project."""
        pass

    @abstractmethod
    def user_can_edit(self, user_id: str, project_id: str) -> bool:
        """Check if a user can edit project settings/views/layout."""
        pass

    @abstractmethod
    def user_is_owner(self, user_id: str, project_id: str) -> bool:
        """Check if a user is the owner of a project."""
        pass

    @abstractmethod
    def user_can_manage_members(self, user_id: str, project_id: str) -> bool:
        """Check if a user can manage members in a project."""
        pass

    @abstractmethod
    def add_project_member(self, project_id: str, user_id: str, role: str) -> bool:
        """Add a member to a project."""
        pass

    @abstractmethod
    def remove_project_member(self, project_id: str, user_id: str) -> bool:
        """Remove a member from a project."""
        pass


# --- InMemoryProjectRepository Implementation ---
class InMemoryProjectRepository(InMemoryRepository[Project], ProjectRepository):
    """In-memory implementation of project repository."""

    def __init__(self):
        super().__init__(Project)
        # Add demo projects
        self._add_demo_projects()
        logger.info("In-memory project repository initialized with demo data.")

    def _add_demo_projects(self):
        """Add demo projects for testing."""
        now = datetime.now()

        # --- Define the config for the chat view needed by the 'public' project ---
        chat_view_config_debug = ViewConfig(
            id="mcp-chat-view", # ID referenced by ProjectViewsService
            #type="ChatViewBasic",     # Type referenced by ProjectViewsService
            type="LogView",  # Type referenced by ProjectViewsService
            config={            # Config passed to the UIElement
                "title": "Chat Conversation",
                "lineFormat": "json", # How LogView should format appended items
                "autoScroll": True,
                # Add any other config the frontend LogView component needs
            },
            updateBinding='stream:chat_conversation', # Make sure binding is here
            actionId=None, # No action triggered *by* the log view itself
            # position/size/content are less relevant here as hierarchy dictates layout
            position=None,
            size=None,
            content=None, # Initial content comes via binding or is empty
            created_at=now,
            updated_at=now,
            created_by="system"
        )

        # --- Define the config for the chat view needed by the 'public' project ---
        chat_view_config_basic = ViewConfig(
            id="mcp-chat-view",  # ID referenced by ProjectViewsService
            type="ChatViewBasic",     # Type referenced by ProjectViewsService
            config={  # Config passed to the UIElement
                "title": "Chat Conversation",
                "lineFormat": "json",  # How LogView should format appended items
                "autoScroll": True,
                # Add any other config the frontend LogView component needs
            },
            updateBinding='stream:chat_conversation',  # Make sure binding is here
            actionId=None,  # No action triggered *by* the log view itself
            # position/size/content are less relevant here as hierarchy dictates layout
            position=None,
            size=None,
            content=None,  # Initial content comes via binding or is empty
            created_at=now,
            updated_at=now,
            created_by="system"
        )

        # --- Define the public project ---
        public_project = Project(
            id="public",
            name="Public Chat Project",
            description="Public demo project implementing the chat interface.",
            owner_id="user1", # Assign a default owner from user repo
            created_at=now,
            updated_at=now,
            project_type="chat_demo",
            members=[
                {"user_id": "public_user", "role": "owner"},
                {"user_id": "user1", "role": "owner"},
                {"user_id": "user2", "role": "viewer"},
                 # Add a record for anonymous access if needed, or handle in ProjectService
                # {"user_id": None, "role": "viewer"} # Example, adapt based on auth
            ],
            # Store the raw view configs required by the hierarchy builder
            views={
                chat_view_config_basic.id: chat_view_config_basic,
                # Add configs for 'chat-input' and 'send-button' if they are
                # stored as ViewConfigs rather than defined inline in the builder.
                # Example:
                # "chat-input": ViewConfig(id="chat-input", type="InputField", ...),
                # "send-button": ViewConfig(id="send-button", type="Button", ...),
            },
            # Layout hints (minimal, as builder logic is specific for 'public')
            layout=ProjectLayout(
                layout_type="interpreted_chat", # Custom type hint for the builder
                layout_data={"info": "Standard chat layout"}, # Optional data
                created_at=now,
                updated_at=now
            )
        )

        public_project_debug = Project(
            id="public_debug",
            name="Public Chat Projec Debug",
            description="Public demo project implementing the chat interface debug.",
            owner_id="user1",  # Assign a default owner from user repo
            created_at=now,
            updated_at=now,
            project_type="chat_demo",
            members=[
                {"user_id": "public_user", "role": "owner"},
                {"user_id": "user1", "role": "owner"},
                {"user_id": "user2", "role": "viewer"},
                # Add a record for anonymous access if needed, or handle in ProjectService
                # {"user_id": None, "role": "viewer"} # Example, adapt based on auth
            ],
            # Store the raw view configs required by the hierarchy builder
            views={
                chat_view_config_debug.id: chat_view_config_debug,
                # Add configs for 'chat-input' and 'send-button' if they are
                # stored as ViewConfigs rather than defined inline in the builder.
                # Example:
                # "chat-input": ViewConfig(id="chat-input", type="InputField", ...),
                # "send-button": ViewConfig(id="send-button", type="Button", ...),
            },
            # Layout hints (minimal, as builder logic is specific for 'public')
            layout=ProjectLayout(
                layout_type="interpreted_chat",  # Custom type hint for the builder
                layout_data={"info": "Standard chat layout"},  # Optional data
                created_at=now,
                updated_at=now
            )
        )

        # Add projects to the in-memory store
        self.add_demo_items([public_project, public_project_debug])

    # --- Implement Repository Interface Methods ---
    def get_project_views(self, project_id: str) -> Dict[str, ViewConfig]:
        project = self.get_by_id(project_id)
        return project.views if project else {}

    def get_project_layout(self, project_id: str) -> Optional[ProjectLayout]:
        project = self.get_by_id(project_id)
        return project.layout if project else None

    def update_project_views(self, project_id: str, views_data: Dict[str, Any]) -> bool:
        """Updates the entire views dictionary for a project."""
        project = self.get_by_id(project_id)
        if not project:
            return False
        try:
            # Validate and convert incoming data to ViewConfig objects
            validated_views = {}
            for view_id, data in views_data.items():
                # Ensure ID matches key, add timestamps etc.
                data['id'] = view_id
                if view_id in project.views: # Preserve creation time if updating
                     data['created_at'] = project.views[view_id].created_at
                data['updated_at'] = datetime.now()
                validated_views[view_id] = ViewConfig(**data)

            project.views = validated_views
            project.updated_at = datetime.now()
            self.items[project_id] = project # Update in storage
            return True
        except Exception as e:
            logger.error(f"Error validating/updating views for project {project_id}: {e}")
            return False

    def update_project_layout(self, project_id: str, layout_data: Dict[str, Any], user_id: Optional[str] = None) -> bool:
        """Updates the layout hints for a project."""
        project = self.get_by_id(project_id)
        if not project:
            return False
        try:
            # Assume layout_data is already validated if coming from ProjectViewsService
            project.layout = ProjectLayout(**layout_data) # Replace existing layout hints
            project.updated_at = datetime.now()
            self.items[project_id] = project # Update in storage
            return True
        except Exception as e:
             logger.error(f"Error updating layout hints for project {project_id}: {e}")
             return False

    def update_view(self, project_id: str, view_id: str, view_data: Dict[str, Any], user_id: Optional[str] = None) -> Optional[ViewConfig]:
        """Updates or creates a single stored view configuration."""
        project = self.get_by_id(project_id)
        if not project:
            return None
        try:
            now = datetime.now()
            view_data['id'] = view_id # Ensure ID is set
            view_data['updated_at'] = now
            if user_id: # Track who updated/created
                 view_data['updated_by'] = user_id # Assuming ViewConfig has this field

            if view_id in project.views: # Update existing
                 view_data['created_at'] = project.views[view_id].created_at
                 view_data['created_by'] = project.views[view_id].created_by
            else: # Create new
                 view_data['created_at'] = now
                 if user_id:
                     view_data['created_by'] = user_id

            updated_view = ViewConfig(**view_data)
            project.views[view_id] = updated_view # Add/replace in project's views dict
            project.updated_at = now
            self.items[project_id] = project # Update project in storage
            return updated_view
        except Exception as e:
            logger.error(f"Error updating view config {view_id} for project {project_id}: {e}")
            return None

    def delete_view(self, project_id: str, view_id: str) -> bool:
        """Deletes a stored view configuration."""
        project = self.get_by_id(project_id)
        if not project or view_id not in project.views:
            return False
        del project.views[view_id]
        project.updated_at = datetime.now()
        self.items[project_id] = project
        return True

    def get_user_projects(self, user_id: str, skip: int = 0, limit: int = 100) -> List[Project]:
        """Get all projects a user has access to."""
        user_projects = [
            project for project in self.items.values()
            if self.user_has_access(user_id, project.id) # Use the access check logic
        ]
        # Apply pagination
        return sorted(user_projects, key=lambda p: p.name)[skip : skip + limit]

    def user_has_access(self, user_id: str, project_id: str) -> bool:
        """Check if a user has access to a project."""
        project = self.get_by_id(project_id)
        if not project: return False
        # Allow access to 'public' project for anyone (adjust as needed)
        if project_id == 'public': return True
        # Check if user is a member
        return any(member["user_id"] == user_id for member in project.members)

    def user_can_edit(self, user_id: str, project_id: str) -> bool:
        """Check if a user can edit project settings/views/layout."""
        project = self.get_by_id(project_id)
        if not project: return False
        user_member = next((m for m in project.members if m["user_id"] == user_id), None)
        if not user_member: return False
        # Owners and contributors can edit
        return user_member["role"] in ["owner", "contributor"]

    def user_is_owner(self, user_id: str, project_id: str) -> bool:
        """Check if a user is the owner of a project."""
        project = self.get_by_id(project_id)
        if not project: return False
        return project.owner_id == user_id

    def user_can_manage_members(self, user_id: str, project_id: str) -> bool:
        """Check if a user can manage members in a project."""
        project = self.get_by_id(project_id)
        if not project: return False
        user_member = next((m for m in project.members if m["user_id"] == user_id), None)
        if not user_member: return False
        # Only owners can manage members
        return user_member["role"] == "owner"

    def add_project_member(self, project_id: str, user_id: str, role: str) -> bool:
        """Add or update a member in a project."""
        project = self.get_by_id(project_id)
        if not project: return False
        existing_member = next((m for m in project.members if m["user_id"] == user_id), None)
        if existing_member:
            existing_member["role"] = role # Update role
        else:
            project.members.append({"user_id": user_id, "role": role}) # Add new member
        project.updated_at = datetime.now()
        self.items[project_id] = project
        return True

    def remove_project_member(self, project_id: str, user_id: str) -> bool:
        """Remove a member from a project."""
        project = self.get_by_id(project_id)
        if not project: return False
        initial_length = len(project.members)
        project.members = [m for m in project.members if m["user_id"] != user_id]
        if len(project.members) < initial_length:
            project.updated_at = datetime.now()
            self.items[project_id] = project
            return True
        return False # Member not found


# --- Factory Function ---
# Singleton instance storage
_project_repository_instance = None

def get_project_repository() -> ProjectRepository:
    """Get the project repository instance (singleton)."""
    global _project_repository_instance
    if _project_repository_instance is None:
        _project_repository_instance = InMemoryProjectRepository()
    return _project_repository_instance

