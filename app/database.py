"""
Database connection management for the application.
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager

from .config import settings

logger = logging.getLogger(__name__)

# Create SQLAlchemy models base class
Base = declarative_base()

# Initialize engine and session factory based on configuration
if settings.TESTING_MODE:
    logger.info("Using in-memory SQLite database (testing mode)")
    SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
    # Echo SQL statements in debug mode
    engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=settings.DEBUG)
else:
    logger.info(f"Connecting to database: {settings.DATABASE_URL}")
    SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL
    # Create engine with appropriate connection parameters
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        echo=settings.DEBUG,
        pool_pre_ping=True,
        # Add any other database-specific settings here
    )

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_tables():
    """Create all tables in the database."""
    Base.metadata.create_all(bind=engine)


def drop_tables():
    """Drop all tables from the database."""
    Base.metadata.drop_all(bind=engine)


@contextmanager
def get_db():
    """Get a database session with automatic closing.

    Usage:
        with get_db() as db:
            db.query(MyModel).all()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_db_session():
    """Dependency for FastAPI endpoints that need a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()