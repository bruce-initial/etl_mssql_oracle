import oracledb
from typing import Any, List, Optional
import logging

from .base import DatabaseConnection

logger = logging.getLogger(__name__)


class OracleConnection(DatabaseConnection):
    """Oracle database connection using python-oracledb"""
    
    def __init__(self, credentials: dict):
        super().__init__(credentials)
    
    def connect(self) -> None:
        """Establish Oracle connection"""
        try:
            # python-oracledb connection
            dsn = oracledb.makedsn(
                self.credentials['host'],
                self.credentials['port'],
                service_name=self.credentials['service']
            )
            
            self.connection = oracledb.connect(
                user=self.credentials['user'],
                password=self.credentials['password'],
                dsn=dsn
            )
            self.cursor = self.connection.cursor()
            logger.info("Oracle connection established successfully")
            
        except Exception as e:
            logger.error(f"Oracle connection failed: {e}")
            logger.info("Make sure Oracle client is available (python-oracledb can work in thin mode without Instant Client)")
            raise
    
    def disconnect(self) -> None:
        """Close Oracle connection"""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        logger.info("Oracle connection closed")
    
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
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        query = """
        SELECT COUNT(*) 
        FROM USER_TABLES 
        WHERE TABLE_NAME = UPPER(:1)
        """
        self.execute(query, (table_name,))
        return self.fetchone()[0] > 0
    
    def drop_table_if_exists(self, table_name: str) -> None:
        """Drop table if it exists"""
        if self.table_exists(table_name):
            drop_sql = f"DROP TABLE {table_name} CASCADE CONSTRAINTS"
            self.execute(drop_sql)
            logger.info(f"Dropped existing table: {table_name}")
        else:
            logger.info(f"Table {table_name} doesn't exist (expected on first run)")
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table"""
        self.execute(f"SELECT COUNT(*) FROM {table_name}")
        return self.fetchone()[0]
    
    def create_constraint(self, constraint_sql: str) -> None:
        """Create a constraint (foreign key, etc.)"""
        try:
            self.execute(constraint_sql)
            logger.info(f"Created constraint successfully")
        except Exception as e:
            logger.error(f"Failed to create constraint: {e}")
            raise
    
    def create_index(self, index_sql: str) -> None:
        """Create an index"""
        try:
            self.execute(index_sql)
            logger.info(f"Created index successfully")
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise
    
    def execute_scalar(self, query: str, parameters: tuple = None) -> Any:
        """Execute query and return single scalar value"""
        self.execute(query, parameters)
        result = self.fetchone()
        return result[0] if result else None
    
    def execute_all(self, query: str, parameters: tuple = None) -> List[Any]:
        """Execute query and return all results"""
        self.execute(query, parameters)
        return self.fetchall()
    
    def read_table_as_dataframe(self, query: str):
        """Execute query and return results as polars DataFrame"""
        import polars as pl
        
        self.execute(query)
        rows = self.fetchall()
        
        if not rows:
            return pl.DataFrame()
        
        # Get column names from cursor description
        columns = [desc[0] for desc in self.cursor.description]
        
        # Create DataFrame
        return pl.DataFrame(rows, schema=columns, orient="row")