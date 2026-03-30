"""Database initialization.

Table creation and indexing is handled by app.models.database.init_database().
This module re-exports for backwards compatibility.
"""

from app.models.database import init_database, close_database

__all__ = ["init_database", "close_database"]
