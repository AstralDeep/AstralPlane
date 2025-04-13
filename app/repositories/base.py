"""
Base repository interface and implementations.
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, TypeVar, Generic, Type, Optional, Any
import uuid
from datetime import datetime

from ..config import settings
from ..models.schemas import BaseModel

# Generic type for models
T = TypeVar('T')

logger = logging.getLogger(__name__)


class BaseRepository(Generic[T], ABC):
    """Base repository interface defining data access methods."""

    @abstractmethod
    def get_by_id(self, id: str) -> Optional[T]:
        """Get an item by ID."""
        pass

    @abstractmethod
    def get_all(self, skip: int = 0, limit: int = 100) -> List[T]:
        """Get all items with pagination."""
        pass

    @abstractmethod
    def create(self, item: T) -> T:
        """Create a new item."""
        pass

    @abstractmethod
    def update(self, id: str, update_data: Dict[str, Any]) -> Optional[T]:
        """Update an item."""
        pass

    @abstractmethod
    def delete(self, id: str) -> bool:
        """Delete an item."""
        pass


class InMemoryRepository(BaseRepository[T]):
    """In-memory implementation of the repository interface."""

    def __init__(self, model_class: Type[T]):
        self.model_class = model_class
        self.items: Dict[str, T] = {}
        logger.info(f"Initialized in-memory repository for {model_class.__name__}")

    def get_by_id(self, id: str) -> Optional[T]:
        """Get an item by ID."""
        return self.items.get(id)

    def get_all(self, skip: int = 0, limit: int = 100) -> List[T]:
        """Get all items with pagination."""
        items = list(self.items.values())
        return items[skip:skip + limit]

    def create(self, item: T) -> T:
        """Create a new item."""
        self.items[item.id] = item
        return item

    def update(self, id: str, update_data: Dict[str, Any]) -> Optional[T]:
        """Update an item."""
        # Check if item exists
        if id not in self.items:
            return None

        # Get a copy of the item
        item = self.items[id].model_copy()

        # Update fields
        for key, value in update_data.items():
            if hasattr(item, key):
                setattr(item, key, value)

        # Update in storage
        self.items[id] = item

        return item

    def delete(self, id: str) -> bool:
        """Delete an item."""
        if id not in self.items:
            return False

        del self.items[id]
        return True

    def add_demo_items(self, demo_items: List[T]):
        """Add demo items to the repository."""
        for item in demo_items:
            self.items[item.id] = item


# Dictionary to store repository instances to implement singleton pattern
_repository_instances = {}


# Factory function to get the appropriate repository implementation
def get_repository(model_class: Type[T]) -> BaseRepository[T]:
    """Get repository instance based on current configuration."""
    # Use class name as key for singleton instance
    class_name = model_class.__name__

    # Return existing instance if we have one
    if class_name in _repository_instances:
        return _repository_instances[class_name]

    # Create a new instance
    repository = InMemoryRepository(model_class)
    _repository_instances[class_name] = repository

    return repository