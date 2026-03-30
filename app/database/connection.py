"""Database connection utilities.

The primary database access is through app.models.database.db (asyncpg pool).
This module provides compatibility helpers.
"""

from app.models.database import db


async def get_db():
    """FastAPI dependency — returns the global db instance."""
    return db
