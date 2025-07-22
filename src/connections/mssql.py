import pymssql
import pandas as pd
from typing import Any, List, Optional
import logging

from .base import DatabaseConnection

logger = logging.getLogger(__name__)


class MSSQLConnection(DatabaseConnection):
    """SQL Server database connection using pymssql"""
    
    def __init__(self, credentials: dict):
        super().__init__(credentials)
    
    def connect(self) -> None:
        """Establish SQL Server connection"""
        try:
            # pymssql connection - no drivers needed
            self.connection = pymssql.connect(
                server=self.credentials['host'],
                port=self.credentials['port'],
                user=self.credentials['user'],
                password=self.credentials['password'],
                database=self.credentials['database'],
                timeout=30,
                login_timeout=10
            )
            self.cursor = self.connection.cursor()
            logger.info("SQL Server connected successfully using pymssql")
            
        except Exception as e:
            logger.error(f"SQL Server connection failed: {e}")
            raise ConnectionError(f"Failed to connect to SQL Server: {e}")
    
    def disconnect(self) -> None:
        """Close SQL Server connection"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info("SQL Server connection closed")
    
    def execute(self, query: str, parameters: tuple = None) -> Any:
        """Execute a query"""
        if not self.cursor:
            raise ConnectionError("No active connection")
        
        if parameters:
            return self.cursor.execute(query, parameters)
        else:
            return self.cursor.execute(query)
    
    def executemany(self, query: str, data: list) -> Any:
        """Execute a query with multiple parameter sets"""
        if not self.cursor:
            raise ConnectionError("No active connection")
        return self.cursor.executemany(query, data)
    
    def fetchone(self) -> Any:
        """Fetch one row from the result set"""
        if not self.cursor:
            raise ConnectionError("No active connection")
        return self.cursor.fetchone()
    
    def fetchall(self) -> list:
        """Fetch all rows from the result set"""
        if not self.cursor:
            raise ConnectionError("No active connection")
        return self.cursor.fetchall()
    
    def commit(self) -> None:
        """Commit the current transaction"""
        if self.connection:
            self.connection.commit()
    
    def rollback(self) -> None:
        """Rollback the current transaction"""
        if self.connection:
            self.connection.rollback()
    
    def read_table_as_dataframe(self, query: str) -> pd.DataFrame:
        """Read table data as pandas DataFrame"""
        if not self.connection:
            raise ConnectionError("No active connection")
        
        return pd.read_sql(query, self.connection)
    
    def get_table_schema(self, table_name: str, schema: str = "dbo") -> List[dict]:
        """Get table schema information"""
        schema_query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = %s AND TABLE_SCHEMA = %s
        ORDER BY ORDINAL_POSITION
        """
        
        self.execute(schema_query, (table_name, schema))
        columns = self.fetchall()
        
        return [
            {
                'name': col[0],
                'type': col[1],
                'max_length': col[2],
                'precision': col[3],
                'scale': col[4],
                'nullable': col[5] == 'YES'
            }
            for col in columns
        ]