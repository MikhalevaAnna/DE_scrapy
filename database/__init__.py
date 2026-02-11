"""
Database module for PostgreSQL connection and schema management.
"""

# Используем относительный импорт
from .schema_manager import SchemaManager

__all__ = ['SchemaManager']