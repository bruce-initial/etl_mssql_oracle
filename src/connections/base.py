from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseConnection(ABC):
    """Abstract base class for database connections"""
    
    def __init__(self, credentials: dict):
        self.credentials = credentials
        self.connection: Optional[Any] = None
        self.cursor: Optional[Any] = None
    
    @abstractmethod
    def connect(self) -> None:
        """Establish database connection"""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass
    
    @abstractmethod
    def execute(self, query: str, parameters: tuple = None) -> Any:
        """Execute a query"""
        pass
    
    @abstractmethod
    def executemany(self, query: str, data: list) -> Any:
        """Execute a query with multiple parameter sets"""
        pass
    
    @abstractmethod
    def fetchone(self) -> Any:
        """Fetch one row from the result set"""
        pass
    
    @abstractmethod
    def fetchall(self) -> list:
        """Fetch all rows from the result set"""
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction"""
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction"""
        pass
    
    def is_connected(self) -> bool:
        """Check if connection is active"""
        return self.connection is not None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        self.disconnect()