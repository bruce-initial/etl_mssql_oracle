# import pymssql
import pyodbc
import polars as pl
from typing import Any, List, Optional
import logging

from .base import DatabaseConnection

logger = logging.getLogger(__name__)


class MSSQLConnection(DatabaseConnection):
    """SQL Server database connection using pymssql"""
    
    def __init__(self, credentials: dict):
        super().__init__(credentials)
        # Windows-compatible ODBC drivers with more fallbacks
        self.available_drivers = [
            "ODBC Driver 18 for SQL Server", 
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 13 for SQL Server",
            "ODBC Driver 11 for SQL Server",
            "SQL Server Native Client 11.0",
            "SQL Server Native Client 10.0",
            "SQL Server",
            "SQLSRV",
            "FreeTDS"
        ]
    
    def connect(self) -> None:
        """Establish SQL Server connection"""
        connection_string = None
        
        for driver in self.available_drivers:
            try:
                # Build connection string with Windows compatibility options
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={self.credentials['host']},{self.credentials['port']};"
                    f"DATABASE={self.credentials['database']};"
                    f"UID={self.credentials['user']};"
                    f"PWD={self.credentials['password']};"
                )
                
                # Add encryption options for newer drivers
                if "18" in driver or "17" in driver:
                    connection_string += "TrustServerCertificate=yes;Encrypt=yes;"
                elif "13" in driver or "11" in driver:
                    connection_string += "TrustServerCertificate=yes;Encrypt=optional;"
                else:
                    # For older drivers, minimal options
                    connection_string += "Trusted_Connection=no;"
                
                self.connection = pyodbc.connect(connection_string)
                self.cursor = self.connection.cursor()
                logger.info(f"SQL Server connected successfully using {driver}")
                return
                
            except pyodbc.Error as e:
                logger.warning(f"Failed to connect with {driver}: {e}")
                continue
        
        if not self.connection:
            raise ConnectionError("Failed to connect to SQL Server with any available driver")
    
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
    
    def read_table_as_dataframe(self, query: str) -> pl.DataFrame:
        """Read table data as polars DataFrame"""
        if not self.connection:
            raise ConnectionError("No active connection")
        
        # Execute query and fetch data directly with pymssql
        self.execute(query)
        rows = self.fetchall()
        
        # Get column names from cursor description
        columns = [desc[0] for desc in self.cursor.description] if self.cursor.description else []
        
        # Convert to polars DataFrame directly
        if not rows:
            # Return empty DataFrame with correct column names
            return pl.DataFrame({col: [] for col in columns})
        
        # Convert rows to dictionary format for polars, handling binary data
        data_dict = {col: [] for col in columns}
        for row in rows:
            for i, col in enumerate(columns):
                val = row[i]
                # Convert binary data to hex string representation
                if isinstance(val, bytes):
                    val = val.hex().upper()
                data_dict[col].append(val)
        
        # Create explicit string schema for all columns
        string_schema = {col: pl.String for col in columns}
        return pl.DataFrame(data_dict, schema=string_schema)
    
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
    
    def execute_scalar(self, query: str, parameters: tuple = None) -> Any:
        """Execute query and return single scalar value"""
        self.execute(query, parameters)
        result = self.fetchone()
        return result[0] if result else None
    
    def execute_all(self, query: str, parameters: tuple = None) -> List[Any]:
        """Execute query and return all results"""
        self.execute(query, parameters)
        return self.fetchall()