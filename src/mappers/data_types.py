import polars as pl
import re
from typing import List, Dict, Any, Tuple
import logging

from ..utils.exceptions import DataTypeError

logger = logging.getLogger(__name__)


class DataTypeMapper:
    """Maps data types between different database systems"""
    
    # Oracle reserved keywords
    ORACLE_RESERVED_KEYWORDS = {
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
        'ALTER', 'TABLE', 'INDEX', 'VIEW', 'GRANT', 'REVOKE', 'USER', 'ROLE',
        'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL', 'YEAR', 'MONTH', 'DAY',
        'HOUR', 'MINUTE', 'SECOND', 'LEVEL', 'CONNECT', 'ORDER', 'GROUP',
        'HAVING', 'UNION', 'INTERSECT', 'MINUS', 'DISTINCT', 'ALL', 'ANY',
        'SOME', 'EXISTS', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'NOT',
        'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AS', 'ON',
        'USING', 'NATURAL', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
        'OUTER', 'CROSS', 'COMMENT', 'CONSTRAINT', 'PRIMARY', 'KEY',
        'FOREIGN', 'REFERENCES', 'UNIQUE', 'CHECK', 'DEFAULT', 'SEQUENCE',
        'NEXTVAL', 'CURRVAL', 'ROWNUM', 'ROWID', 'SYSDATE', 'SYSTIMESTAMP', 'DESC'
    }
    
    # Data type mapping from polars/SQL Server to Oracle
    TYPE_MAPPINGS = {
        'object': 'VARCHAR2',
        'string': 'VARCHAR2',
        'int8': 'NUMBER',
        'int16': 'NUMBER',
        'int32': 'NUMBER',
        'int64': 'NUMBER',
        'Int8': 'NUMBER',
        'Int16': 'NUMBER',
        'Int32': 'NUMBER', 
        'Int64': 'NUMBER',
        'float32': 'NUMBER',
        'float64': 'NUMBER',
        'Float32': 'NUMBER',
        'Float64': 'NUMBER',
        'bool': 'NUMBER(1)',
        'boolean': 'NUMBER(1)',
        'datetime64[ns]': 'TIMESTAMP',
        'datetime64': 'TIMESTAMP',
        'timedelta64[ns]': 'INTERVAL DAY TO SECOND',
        'category': 'VARCHAR2'
    }
    
    def __init__(self):
        pass
    
    def sanitize_column_name(self, col_name: str) -> str:
        """Sanitize column names for Oracle compatibility"""
        if not col_name or col_name.strip() == '':
            raise DataTypeError("Empty or null column name found")
        
        # Remove leading/trailing spaces
        col_name = col_name.strip()
        
        # Replace spaces and special characters with underscores
        col_name = re.sub(r'[^\w]', '_', col_name)
        
        # Ensure it doesn't start with a number
        if col_name[0].isdigit():
            col_name = f"COL_{col_name}"
        
        # Convert to uppercase (Oracle convention)
        col_name = col_name.upper()
        
        # Handle Oracle reserved keywords
        if col_name in self.ORACLE_RESERVED_KEYWORDS:
            col_name = f'"{col_name}"'
        
        # Limit length to 30 characters (Oracle limit)
        # if len(col_name) > 30:
        #     col_name = col_name[:27] + str(hash(col_name))[-3:]
        
        return col_name
    
    def map_pandas_to_oracle_type(self, dtype: str, max_length: int = None) -> str:
        # """Map pandas dtype to Oracle data type"""
        # dtype_str = str(dtype).lower()
        
        # # Handle string/object types with length calculation
        # if any(t in dtype_str for t in ['object', 'string']):
        #     if max_length is None:
        #         length = 4000
        #     else:
        #         # Add buffer and ensure within Oracle limits
        #         length = min(max_length + 50, 4000)
        #     return f"VARCHAR2({length})"
        
        # # Handle numeric types
        # if any(t in dtype_str for t in ['int', 'integer']):
        #     return 'NUMBER'
        
        # if any(t in dtype_str for t in ['float', 'double']):
        #     return 'NUMBER'
        
        # # Handle boolean
        # if 'bool' in dtype_str:
        #     return 'NUMBER(1)'
        
        # # Handle datetime
        # if 'datetime' in dtype_str:
        #     return 'TIMESTAMP'
        
        # if 'date' in dtype_str:
        #     return 'DATE'
        
        # # Handle timedelta
        # if 'timedelta' in dtype_str:
        #     return 'INTERVAL DAY TO SECOND'
        
        # # Default fallback
        # logger.warning(f"Unknown dtype '{dtype}', using VARCHAR2(4000)")

        # return 'VARCHAR2(4000)'

        # Temporary handling: map all type to NVARCHAR2(1000)
        return 'NVARCHAR2(1000)'
    
    def analyze_dataframe_columns(self, df: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Analyze DataFrame columns to determine appropriate Oracle types"""
        column_analysis = {}
        
        for col_name in df.columns:
            original_name = col_name
            sanitized_name = self.sanitize_column_name(col_name)
            dtype = str(df[col_name].dtype)
            
            # Calculate max length for string columns
            max_length = None
            if df[col_name].dtype in [pl.Utf8, pl.String] or 'string' in str(df[col_name].dtype):
                if len(df) > 0:
                    try:
                        max_length_result = df.select(pl.col(col_name).cast(pl.Utf8).str.len_chars().max()).item()
                        if max_length_result is None:
                            max_length = 255
                        else:
                            max_length = int(max_length_result)
                    except Exception as e:
                        logger.warning(f"Error calculating max length for {col_name}: {e}")
                        max_length = 255
                else:
                    max_length = 255
            
            # Map to Oracle type
            oracle_type = self.map_pandas_to_oracle_type(dtype, max_length)
            
            column_analysis[original_name] = {
                'original_name': original_name,
                'sanitized_name': sanitized_name,
                'pandas_dtype': dtype,
                'oracle_type': oracle_type,
                'max_length': max_length,
                'nullable': df[col_name].null_count() > 0
            }
        
        return column_analysis
    
    def generate_oracle_column_definitions(self, column_analysis: Dict[str, Dict[str, Any]]) -> List[str]:
        """Generate Oracle column definitions from analysis"""
        definitions = ["    U_ID RAW(16) DEFAULT sys_guid() NOT NULL"]
        
        for col_info in column_analysis.values():
            sanitized_name = col_info['sanitized_name']
            oracle_type = col_info['oracle_type']
            
            definition = f"    {sanitized_name} {oracle_type}"
            definitions.append(definition)
        
        return definitions
    
    def create_column_mapping(self, column_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Create mapping from original to sanitized column names"""
        return {
            col_info['original_name']: col_info['sanitized_name']
            for col_info in column_analysis.values()
        }
    
    def process_data_for_oracle(self, data: List[Tuple], column_analysis: Dict[str, Dict[str, Any]]) -> List[Tuple]:
        """Process data values for Oracle insertion - convert all values to strings for NVARCHAR2 columns"""
        processed_data = []
        
        # Extract column info for length validation
        columns_info = list(column_analysis.values())
        
        for row_idx, row in enumerate(data):
            processed_row = []
            
            for col_idx, val in enumerate(row):
                try:
                    if val is None:
                        processed_row.append(None)
                    else:
                        # Convert all non-null values to strings since all target columns are NVARCHAR2(1000)
                        str_val = str(val)
                        
                        # Check if column has length limit and truncate if necessary
                        if col_idx < len(columns_info):
                            col_info = columns_info[col_idx]
                            oracle_type = col_info.get('oracle_type', '')
                            
                            # Extract NVARCHAR2 length limit
                            if 'NVARCHAR2(' in oracle_type:
                                import re
                                length_match = re.search(r'NVARCHAR2\((\d+)\)', oracle_type)
                                if length_match:
                                    max_length = int(length_match.group(1))
                                    if len(str_val) > max_length:
                                        logger.warning(f"Truncating string value from {len(str_val)} to {max_length} chars at row {row_idx}, col {col_idx}")
                                        str_val = str_val[:max_length]
                        
                        processed_row.append(str_val)
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_idx}, column {col_idx}: {e}")
                    logger.error(f"Value: {val}, Type: {type(val)}")
                    raise DataTypeError(f"Failed to process data value: {e}")
            
            processed_data.append(tuple(processed_row))
        
        return processed_data
    
    def validate_column_mapping(self, original_columns: List[str], sanitized_columns: List[str]) -> None:
        """Validate column mapping consistency"""
        if len(original_columns) != len(sanitized_columns):
            raise DataTypeError(
                f"Column count mismatch: {len(original_columns)} original vs {len(sanitized_columns)} sanitized"
            )
        
        # Check for duplicate sanitized names
        if len(set(sanitized_columns)) != len(sanitized_columns):
            duplicates = [name for name in set(sanitized_columns) if sanitized_columns.count(name) > 1]
            raise DataTypeError(f"Duplicate sanitized column names: {duplicates}")
        
        logger.info(f"Column mapping validation successful: {len(original_columns)} columns")