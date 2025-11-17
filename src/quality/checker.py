"""
Data Quality Checker Module
Performs comprehensive data quality checks after ETL transfer operations
"""

import time
import logging
import random
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import polars as pl

from ..connections.mssql import MSSQLConnection
from ..connections.oracle import OracleConnection
from ..mappers.data_types import DataTypeMapper
from ..utils.exceptions import ValidationError, SQLError
from ..utils.logging import ETLLogger

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Comprehensive data quality checking for ETL transfers"""
    
    def __init__(self, mssql_conn: MSSQLConnection, oracle_conn: OracleConnection,
                 config: Dict[str, Any], etl_logger: ETLLogger):
        self.mssql_conn = mssql_conn
        self.oracle_conn = oracle_conn
        self.config = config
        self.etl_logger = etl_logger
        self.logger = logger
        self.data_mapper = DataTypeMapper()
        
        # Extract quality check configuration
        settings = config.get('settings', {})
        self.quality_config = settings.get('data_quality_checks', {})
        self.results_table = self.quality_config.get('results_table_name', 'DATA_QUALITY_CHECKING_RLCIS')
        
        # Session ID for tracking related checks
        self.session_id = f"ETL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def initialize_results_table(self) -> None:
        """Create the results table if it doesn't exist"""
        try:
            # Check if table exists
            check_query = f"""
            SELECT COUNT(*) FROM user_tables 
            WHERE table_name = UPPER('{self.results_table}')
            """
            
            result = self.oracle_conn.execute_scalar(check_query)
            
            if result == 0:
                # Load and execute table creation script
                from pathlib import Path
                sql_file = Path(__file__).parent.parent.parent / "sql" / "oracle_create_data_quality_table.sql"
                
                with open(sql_file, 'r') as f:
                    create_sql = f.read()
                
                self.oracle_conn.execute(create_sql)
                self.oracle_conn.commit()
                self.logger.info(f"Created data quality results table: {self.results_table}")
            else:
                self.logger.info(f"Data quality results table already exists: {self.results_table}")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize results table: {e}")
            raise ValidationError(f"Results table initialization failed: {e}")
    
    def check_row_counts(self, source_table: str, target_table: str, 
                        source_schema: str = 'dbo') -> Dict[str, Any]:
        """Compare row counts between source and target tables"""
        start_time = time.time()
        
        try:
            # Get source row count
            source_query = f"SELECT COUNT(*) FROM {source_schema}.{source_table}"
            source_count = self.mssql_conn.execute_scalar(source_query)
            
            # Get target row count
            target_query = f"SELECT COUNT(*) FROM {target_table}"
            target_count = self.oracle_conn.execute_scalar(target_query)
            
            # Determine result
            row_count_match = source_count == target_count
            check_status = 'PASSED' if row_count_match else 'FAILED'
            error_message = None if row_count_match else f"Row count mismatch: source={source_count}, target={target_count}"
            
            duration = time.time() - start_time
            
            result = {
                'check_type': 'ROW_COUNT',
                'source_count': source_count,
                'target_count': target_count,
                'match': row_count_match,
                'status': check_status,
                'error_message': error_message,
                'duration': duration
            }
            
            self.logger.info(f"Row count check - {source_table}: source={source_count}, target={target_count}, match={row_count_match}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Row count check failed: {e}"
            self.logger.error(error_msg)
            
            return {
                'check_type': 'ROW_COUNT',
                'source_count': None,
                'target_count': None,
                'match': False,
                'status': 'FAILED',
                'error_message': error_msg,
                'duration': duration
            }
    
    def determine_sample_size(self, total_rows: int) -> int:
        """Calculate appropriate sample size based on configuration"""
        content_config = self.quality_config.get('content_comparison', {})
        
        sample_percentage = content_config.get('sample_percentage', 0.1)
        min_sample = content_config.get('min_sample_size', 100)
        max_sample = content_config.get('max_sample_size', 1000000)
        
        # Calculate percentage-based sample
        percentage_sample = int(total_rows * sample_percentage)
        
        # Apply min/max constraints
        sample_size = max(min_sample, min(percentage_sample, max_sample))
        
        # Don't sample more than available rows
        final_sample = min(sample_size, total_rows)
        
        self.logger.debug(f"Sample size calculation: total={total_rows}, percentage={sample_percentage}, "
                         f"min={min_sample}, max={max_sample}, final={final_sample}")
        
        return final_sample
    
    def get_expected_additional_columns(self) -> List[str]:
        """Get list of expected additional columns that are added during ETL"""
        return [
            'U_ID',  # UUID column added by ETL process
            # Add other known additional columns here
        ]
    
    def is_sanitized_column_name(self, original_col: str, target_col: str) -> bool:
        """Check if target column is a sanitized version of source column"""
        # Apply same sanitization logic as DataTypeMapper
        import re
        sanitized = re.sub(r'[^\w]', '_', original_col.strip()).upper()
        if sanitized[0].isdigit():
            sanitized = f"COL_{sanitized}"
        return target_col == sanitized
    
    def normalize_single_boolean_value(self, value: str) -> str:
        """Normalize a single boolean value to '0' or '1'"""
        if value is None:
            return None
        
        val_str = str(value).lower().strip()
        if val_str in ['true', '1', 'yes', 't', 'y']:
            return '1'
        elif val_str in ['false', '0', 'no', 'f', 'n']:
            return '0'
        else:
            return val_str  # Return as-is if not boolean-like
    
    def normalize_boolean_values(self, col_data: pl.Series) -> pl.Series:
        """Normalize boolean values to match ETL transformation (true/false -> '1'/'0')"""
        try:
            # Check if column contains boolean-like values
            string_col = col_data.cast(pl.String)
            
            # Apply boolean normalization similar to DataTypeMapper.process_data_for_oracle
            normalized = string_col.map_elements(
                lambda x: (
                    '1' if x is not None and (
                        (isinstance(x, bool) and x) or 
                        (isinstance(x, str) and x.lower() in ['true', '1', 'yes', 't', 'y']) or
                        (str(x).lower() in ['true', '1', 'yes', 't', 'y'])
                    ) else (
                        '0' if x is not None and (
                            (isinstance(x, bool) and not x) or 
                            (isinstance(x, str) and x.lower() in ['false', '0', 'no', 'f', 'n']) or
                            (str(x).lower() in ['false', '0', 'no', 'f', 'n'])
                        ) else str(x) if x is not None else None
                    )
                ),
                return_dtype=pl.String
            )
            
            return normalized
        except Exception as e:
            self.logger.debug(f"Error normalizing boolean values: {e}")
            return col_data.cast(pl.String)
    
    def is_boolean_like_column(self, col_data: pl.Series) -> bool:
        """Check if column contains boolean-like values"""
        try:
            # Get non-null unique values
            unique_values = col_data.drop_nulls().unique().to_list()
            if not unique_values:
                return False
            
            # Convert to lowercase strings for comparison
            unique_strs = [str(v).lower().strip() for v in unique_values if v is not None]
            
            # Check if all values are boolean-like
            boolean_values = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f', 'y', 'n'}
            return len(unique_strs) <= 10 and all(s in boolean_values for s in unique_strs if s)
            
        except Exception:
            return False
    
    def get_column_statistics(self, table_name: str, column_name: str, 
                            schema: str = None, is_oracle: bool = False) -> Dict[str, Any]:
        """Get statistical aggregates for a column with boolean normalization"""
        try:
            # Build query based on database type
            if is_oracle:
                base_query = f"SELECT * FROM {table_name}"
                conn = self.oracle_conn
            else:
                base_query = f"SELECT * FROM {schema}.{table_name}" if schema else f"SELECT * FROM {table_name}"
                conn = self.mssql_conn
            
            # Read data as dataframe for statistical analysis
            df = conn.read_table_as_dataframe(base_query)
            
            if column_name not in df.columns or len(df) == 0:
                return {
                    'count': 0,
                    'non_null_count': 0,
                    'null_count': 0,
                    'distinct_count': 0,
                    'min_value': None,
                    'max_value': None,
                    'avg_length': None,
                    'is_boolean_like': False
                }
            
            col_data = df[column_name]
            
            # Check if this is a boolean-like column
            is_boolean_like = self.is_boolean_like_column(col_data)
            
            # Apply boolean normalization if needed (especially for MSSQL source)
            if is_boolean_like and not is_oracle:
                # For MSSQL source, normalize boolean values to match Oracle target
                self.logger.debug(f"Applying boolean normalization to MSSQL column {column_name}")
                col_data = self.normalize_boolean_values(col_data)
            elif is_boolean_like and is_oracle:
                self.logger.debug(f"Detected boolean-like Oracle column {column_name} (already normalized)")
            
            # Basic counts
            total_count = len(col_data)
            null_count = col_data.null_count()
            non_null_count = total_count - null_count
            
            # Get distinct count
            try:
                distinct_count = col_data.n_unique()
            except Exception:
                distinct_count = None
            
            # Get min/max values (convert to string for comparison)
            min_value = None
            max_value = None
            avg_length = None
            
            if non_null_count > 0:
                try:
                    # Convert to string for consistent comparison
                    string_col = col_data.cast(pl.String)
                    non_null_strings = string_col.drop_nulls()
                    
                    if len(non_null_strings) > 0:
                        min_value = non_null_strings.min()
                        max_value = non_null_strings.max()
                        avg_length = non_null_strings.str.len_chars().mean()
                except Exception as e:
                    self.logger.debug(f"Error calculating min/max for {column_name}: {e}")
            
            return {
                'count': total_count,
                'non_null_count': non_null_count,
                'null_count': null_count,
                'distinct_count': distinct_count,
                'min_value': min_value,
                'max_value': max_value,
                'avg_length': avg_length,
                'is_boolean_like': is_boolean_like
            }
            
        except Exception as e:
            self.logger.error(f"Error getting statistics for {column_name}: {e}")
            return {
                'count': 0,
                'non_null_count': 0,
                'null_count': 0,
                'distinct_count': 0,
                'min_value': None,
                'max_value': None,
                'avg_length': None,
                'error': str(e)
            }
    
    def _apply_etl_transformations_to_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply the same data transformations that are used during ETL process - DEPRECATED: Use statistical comparison instead"""
        
        # Convert all columns to string first
        string_df = df.with_columns([
            pl.col(col).cast(pl.String) for col in df.columns
        ])
        
        # Apply ETL transformations row by row to match the process_data_for_oracle logic
        transformed_data = []
        
        for row in string_df.rows():
            processed_row = []
            
            for val in row:
                # Apply the same logic as in DataTypeMapper.process_data_for_oracle
                if val is None:
                    processed_row.append(None)
                elif val == "":
                    # Treat empty string as NULL for quality checking
                    processed_row.append(None)
                elif isinstance(val, str) and val.strip() == "":
                    # Treat whitespace-only strings as NULL for quality checking
                    processed_row.append(None)
                elif str(val).lower() in ['null', '<null>']:
                    processed_row.append(None)
                elif isinstance(val, bool):
                    # Convert boolean to '1'/'0' strings to match Oracle target
                    processed_row.append('1' if val else '0')
                elif str(val).lower() in ['true', 'false', 'yes', 'no', 't', 'f', 'y', 'n']:
                    # Handle string representations of boolean values (expanded list)
                    val_str = str(val).lower().strip()
                    if val_str in ['true', '1', 'yes', 't', 'y']:
                        processed_row.append('1')
                    elif val_str in ['false', '0', 'no', 'f', 'n']:
                        processed_row.append('0')
                    else:
                        processed_row.append(str(val))
                else:
                    # For string values, apply the same transformations as ETL
                    if isinstance(val, bytes):
                        try:
                            str_val = val.decode('utf-8')
                        except UnicodeDecodeError:
                            try:
                                str_val = val.decode('latin-1')
                            except UnicodeDecodeError:
                                str_val = val.decode('utf-8', errors='replace')
                    else:
                        # Handle datetime formatting with 3-digit precision
                        if hasattr(val, 'strftime'):
                            try:
                                str_val = val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            except (AttributeError, ValueError):
                                str_val = str(val)
                        else:
                            str_val = str(val)
                            # Apply datetime string formatting if needed
                            if isinstance(val, str) and len(str_val) > 19:
                                import re
                                datetime_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.(\d{6})'
                                match = re.match(datetime_pattern, str_val)
                                if match:
                                    base_datetime = match.group(1)
                                    microseconds = match.group(2)
                                    milliseconds = microseconds[:3]
                                    str_val = f"{base_datetime}.{milliseconds}"
                    
                    processed_row.append(str_val)
            
            transformed_data.append(processed_row)
        
        # Recreate DataFrame with transformed data using explicit string schema
        if transformed_data:
            # Create explicit string schema for all columns
            string_schema = {col: pl.String for col in df.columns}
            return pl.DataFrame(transformed_data, schema=string_schema, orient="row")
        else:
            # Return empty DataFrame with string schema
            string_schema = {col: pl.String for col in df.columns}
            return pl.DataFrame([], schema=string_schema)
    
    def get_table_count(self, table_name: str, schema: str = 'dbo') -> int:
        """Get total row count for sampling calculations"""
        query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
        return self.mssql_conn.execute_scalar(query)
    
    def compare_column_statistics(self, source_table: str, target_table: str,
                                source_schema: str = 'dbo') -> Dict[str, Any]:
        """Compare statistical aggregates between source and target columns"""
        
        try:
            # Get column lists from both tables
            source_query = f"SELECT TOP 1 * FROM {source_schema}.{source_table}"
            source_df = self.mssql_conn.read_table_as_dataframe(source_query)
            
            target_query = f"SELECT * FROM {target_table} WHERE ROWNUM <= 1"
            target_df = self.oracle_conn.read_table_as_dataframe(target_query)
            
            if len(source_df.columns) == 0 or len(target_df.columns) == 0:
                return {
                    'columns_checked': 0,
                    'columns_matched': 0,
                    'match_percentage': 0.0,
                    'details': 'No columns to compare',
                    'mismatch_details': []
                }
            
            # Map source columns to target columns
            source_cols_map = {col.upper(): col for col in source_df.columns}
            target_cols_map = {col: col for col in target_df.columns}
            
            # Find expected additional columns
            expected_additional = set(self.get_expected_additional_columns())
            
            # Find actual additional columns (excluding expected ones)
            target_only_cols = set(target_cols_map.keys()) - set(source_cols_map.keys())
            unexpected_additional = target_only_cols - expected_additional
            
            # Find common columns for comparison
            common_cols_upper = set(source_cols_map.keys()) & set(target_cols_map.keys())
            missing_columns = list(set(source_cols_map.keys()) - set(target_cols_map.keys()))
            
            mismatch_details = []
            
            if unexpected_additional:
                unexpected_list = [target_cols_map[col] for col in unexpected_additional]
                mismatch_details.append(f"Unexpected additional columns: {', '.join(unexpected_list)}")
            
            if missing_columns:
                missing_list = [source_cols_map[col] for col in missing_columns]
                mismatch_details.append(f"Missing columns in target: {', '.join(missing_list)}")
            
            if not common_cols_upper:
                return {
                    'columns_checked': 0,
                    'columns_matched': 0,
                    'match_percentage': 0.0,
                    'total_source_columns': len(source_cols_map),
                    'missing_columns': missing_columns,
                    'unexpected_additional_columns': list(unexpected_additional),
                    'details': 'No common columns found',
                    'mismatch_details': mismatch_details
                }
            
            columns_checked = len(common_cols_upper)
            columns_matched = 0
            column_details = {}
            
            # Compare statistics for each common column
            for col_upper in common_cols_upper:
                source_col = source_cols_map[col_upper]
                target_col = target_cols_map[col_upper]
                
                try:
                    # Get statistics from both tables
                    source_stats = self.get_column_statistics(source_table, source_col, source_schema, False)
                    target_stats = self.get_column_statistics(target_table, target_col, None, True)
                    
                    # Compare key statistics
                    stats_match = True
                    stat_mismatches = []
                    
                    # Check row counts
                    if source_stats['count'] != target_stats['count']:
                        stats_match = False
                        stat_mismatches.append(f"count: {source_stats['count']} vs {target_stats['count']}")
                    
                    # Check null counts
                    if source_stats['null_count'] != target_stats['null_count']:
                        stats_match = False
                        stat_mismatches.append(f"null_count: {source_stats['null_count']} vs {target_stats['null_count']}")
                    
                    # Check distinct counts (with tolerance for small differences)
                    source_distinct = source_stats['distinct_count']
                    target_distinct = target_stats['distinct_count']
                    if source_distinct is not None and target_distinct is not None:
                        if abs(source_distinct - target_distinct) > max(1, source_distinct * 0.01):  # 1% tolerance
                            stats_match = False
                            stat_mismatches.append(f"distinct_count: {source_distinct} vs {target_distinct}")
                    
                    # Check min/max values with special handling for boolean columns
                    source_is_bool = source_stats.get('is_boolean_like', False)
                    target_is_bool = target_stats.get('is_boolean_like', False)
                    
                    if source_stats['min_value'] is not None and target_stats['min_value'] is not None:
                        source_min = str(source_stats['min_value'])
                        target_min = str(target_stats['min_value'])
                        
                        # For boolean columns, normalize before comparison
                        if source_is_bool or target_is_bool:
                            source_min_norm = self.normalize_single_boolean_value(source_min)
                            target_min_norm = self.normalize_single_boolean_value(target_min)
                            if source_min_norm != target_min_norm:
                                stats_match = False
                                self.logger.debug(f"Boolean min_value mismatch in column {source_col}: source='{source_min}' -> '{source_min_norm}', target='{target_min}' -> '{target_min_norm}'")
                                stat_mismatches.append(f"min_value: '{source_stats['min_value']}' vs '{target_stats['min_value']}' (boolean normalized: '{source_min_norm}' vs '{target_min_norm}')")
                        else:
                            # Case-insensitive comparison for non-boolean columns
                            if source_min.lower() != target_min.lower():
                                stats_match = False
                                stat_mismatches.append(f"min_value: '{source_stats['min_value']}' vs '{target_stats['min_value']}'")
                    
                    if source_stats['max_value'] is not None and target_stats['max_value'] is not None:
                        source_max = str(source_stats['max_value'])
                        target_max = str(target_stats['max_value'])
                        
                        # For boolean columns, normalize before comparison
                        if source_is_bool or target_is_bool:
                            source_max_norm = self.normalize_single_boolean_value(source_max)
                            target_max_norm = self.normalize_single_boolean_value(target_max)
                            if source_max_norm != target_max_norm:
                                stats_match = False
                                self.logger.debug(f"Boolean max_value mismatch in column {source_col}: source='{source_max}' -> '{source_max_norm}', target='{target_max}' -> '{target_max_norm}'")
                                stat_mismatches.append(f"max_value: '{source_stats['max_value']}' vs '{target_stats['max_value']}' (boolean normalized: '{source_max_norm}' vs '{target_max_norm}')")
                        else:
                            # Case-insensitive comparison for non-boolean columns
                            if source_max.lower() != target_max.lower():
                                stats_match = False
                                stat_mismatches.append(f"max_value: '{source_stats['max_value']}' vs '{target_stats['max_value']}'")
                    
                    if stats_match:
                        columns_matched += 1
                    else:
                        detailed_info = f"Column '{source_col}': {'; '.join(stat_mismatches)}"
                        mismatch_details.append(detailed_info)
                    
                    column_details[source_col] = {
                        'source_stats': source_stats,
                        'target_stats': target_stats,
                        'stats_match': stats_match,
                        'mismatches': stat_mismatches
                    }
                    
                except Exception as e:
                    self.logger.warning(f"Error comparing statistics for column {source_col}: {e}")
                    mismatch_details.append(f"Column '{source_col}': Statistics comparison error - {str(e)}")
                    column_details[source_col] = {
                        'stats_match': False,
                        'error': str(e)
                    }
            
            # Calculate match percentage
            total_source_columns = len(source_cols_map)
            if total_source_columns > 0:
                match_percentage = (columns_matched / total_source_columns * 100)
            else:
                match_percentage = 0.0
            
            return {
                'columns_checked': columns_checked,
                'columns_matched': columns_matched,
                'match_percentage': match_percentage,
                'total_source_columns': total_source_columns,
                'missing_columns': missing_columns,
                'unexpected_additional_columns': list(unexpected_additional),
                'column_details': column_details,
                'mismatch_details': mismatch_details
            }
            
        except Exception as e:
            self.logger.error(f"Error in statistical comparison: {e}")
            return {
                'columns_checked': 0,
                'columns_matched': 0,
                'match_percentage': 0.0,
                'total_source_columns': 0,
                'missing_columns': [],
                'unexpected_additional_columns': [],
                'details': f'Statistical comparison failed: {e}',
                'mismatch_details': [f'Error: {e}']
            }
    
    def compare_dataframe_content(self, source_df: pl.DataFrame, 
                                 target_df: pl.DataFrame) -> Dict[str, Any]:
        """Compare content between source and target dataframes - DEPRECATED: Use compare_column_statistics instead"""
        
        if len(source_df) == 0 or len(target_df) == 0:
            return {
                'columns_checked': 0,
                'columns_matched': 0,
                'match_percentage': 0.0,
                'total_source_columns': 0,
                'missing_columns': [],
                'additional_columns': [],
                'details': 'No data to compare',
                'mismatch_details': []
            }
        
        # Handle MSSQL mixed case to Oracle uppercase mapping
        source_cols_map = {}  # key: uppercase, value: actual MSSQL column name
        target_cols_map = {}  # key: uppercase, value: Oracle column name
        
        # Build source column mapping (MSSQL mixed case -> uppercase)
        for col in source_df.columns:
            source_cols_map[col.upper()] = col
        
        # Build target column mapping (Oracle already uppercase)
        for col in target_df.columns:
            target_cols_map[col.upper()] = col
        
        # Find matching columns
        common_cols_upper = set(source_cols_map.keys()) & set(target_cols_map.keys())
        exact_matches = {}  # source_col -> target_col
        
        for col_upper in common_cols_upper:
            source_col = source_cols_map[col_upper]
            target_col = target_cols_map[col_upper]
            exact_matches[source_col] = target_col
        
        # Track missing and additional columns
        source_only_cols = set(source_cols_map.keys()) - common_cols_upper
        target_only_cols = set(target_cols_map.keys()) - common_cols_upper
        
        # Get actual column names for missing/additional columns
        missing_columns = [source_cols_map[col] for col in source_only_cols]
        additional_columns = [target_cols_map[col] for col in target_only_cols]
        
        mismatch_details = []
        
        # Report additional columns
        if additional_columns:
            mismatch_details.append(f"Additional columns in target: {', '.join(additional_columns)}")
        
        if missing_columns:
            mismatch_details.append(f"Missing columns in target: {', '.join(missing_columns)}")
        
        if not common_cols_upper:
            return {
                'columns_checked': 0,
                'columns_matched': 0,
                'match_percentage': 0.0,
                'total_source_columns': len(source_cols_map),
                'missing_columns': missing_columns,
                'additional_columns': additional_columns,
                'details': 'No common columns found',
                'mismatch_details': mismatch_details
            }
        
        columns_checked = len(common_cols_upper)
        columns_matched = 0
        column_details = {}
        
        # Compare each common column
        for source_col, target_col in exact_matches.items():
            
            try:
                # Get column values from both dataframes (preserve row order - do NOT sort)
                source_values = source_df[source_col]
                target_values = target_df[target_col]
                
                # Compare values (handling nulls and different lengths)
                if len(source_values) == len(target_values):
                    # Element-wise comparison with case-insensitive string comparison
                    source_is_null = source_values.is_null()
                    target_is_null = target_values.is_null()
                    both_null = source_is_null & target_is_null
                    
                    # Enhanced comparison with boolean normalization for non-null values
                    try:
                        # Convert to string first
                        source_str = source_values.cast(pl.String)
                        target_str = target_values.cast(pl.String)
                        
                        # Check if this might be a boolean column
                        is_bool_like = self.is_boolean_like_column(source_values) or self.is_boolean_like_column(target_values)
                        
                        if is_bool_like:
                            # Apply boolean normalization to both columns
                            self.logger.debug(f"Applying boolean normalization for column comparison: {source_col}")
                            source_normalized = self.normalize_boolean_values(source_values)
                            target_normalized = self.normalize_boolean_values(target_values)
                            both_equal = (source_normalized == target_normalized)
                        else:
                            # Standard case-insensitive comparison
                            source_lower = source_str.str.to_lowercase().fill_null("")
                            target_lower = target_str.str.to_lowercase().fill_null("")
                            both_equal = (source_lower == target_lower)
                    except Exception:
                        # Fallback to direct comparison if normalization fails
                        both_equal = (source_values == target_values).fill_null(False)
                    
                    matches = (both_null | both_equal).sum()
                    total = len(source_values)
                    match_rate = matches / total if total > 0 else 0
                    
                    if match_rate >= 0.95:  # 95% threshold for considering a column "matched"
                        columns_matched += 1
                    else:
                        # Record detailed mismatch information for failed columns
                        mismatched_count = total - matches
                        
                        # Get sample of mismatched values for detailed logging
                        mismatch_mask = ~(both_null | both_equal)
                        mismatched_indices = mismatch_mask.arg_true()
                        
                        # Limit to first 10 mismatches for detailed analysis
                        sample_mismatches = []
                        for idx in mismatched_indices[:10]:
                            try:
                                src_val = source_values[idx]
                                tgt_val = target_values[idx]
                                sample_mismatches.append(f"Row {idx}: '{src_val}' vs '{tgt_val}'")
                            except Exception:
                                continue
                        
                        detailed_info = f"Column '{source_col}': {mismatched_count}/{total} values differ ({match_rate:.1%} match)"
                        if sample_mismatches:
                            detailed_info += f" | Examples: {'; '.join(sample_mismatches[:5])}"
                            if len(sample_mismatches) > 5:
                                detailed_info += f" (and {len(sample_mismatches)-5} more)"
                        
                        mismatch_details.append(detailed_info)
                    
                    column_details[source_col] = {
                        'match_rate': match_rate,
                        'matches': matches,
                        'total': total
                    }
                else:
                    mismatch_details.append(f"Column '{source_col}': Shape mismatch (source: {len(source_values)}, target: {len(target_values)})")
                    column_details[source_col] = {
                        'match_rate': 0.0,
                        'matches': 0,
                        'total': max(len(source_values), len(target_values)),
                        'note': 'Shape mismatch'
                    }
                    
            except Exception as e:
                self.logger.warning(f"Error comparing column {source_col}: {e}")
                mismatch_details.append(f"Column '{source_col}': Comparison error - {str(e)}")
                column_details[source_col] = {
                    'match_rate': 0.0,
                    'error': str(e)
                }
        
        # Calculate match percentage considering missing columns
        total_source_columns = len(source_cols_map)
        if total_source_columns > 0:
            # Match percentage should account for missing columns
            # Only columns that exist in both source and target AND have matching content count as matched
            match_percentage = (columns_matched / total_source_columns * 100)
        else:
            match_percentage = 0.0
        
        return {
            'columns_checked': columns_checked,
            'columns_matched': columns_matched,
            'match_percentage': match_percentage,
            'total_source_columns': total_source_columns,
            'missing_columns': missing_columns,
            'additional_columns': additional_columns,
            'column_details': column_details,
            'mismatch_details': mismatch_details
        }
    
    def check_content_comparison(self, source_table: str, target_table: str,
                               source_schema: str = 'dbo', source_count: int = 0,
                               primary_key: Optional[str] = None) -> Dict[str, Any]:
        """Compare statistical aggregates between source and target tables (improved approach)"""
        start_time = time.time()
        
        try:
            # Get actual count if not provided
            actual_count = source_count if source_count > 0 else self.get_table_count(source_table, source_schema)
            
            if actual_count == 0:
                return {
                    'check_type': 'CONTENT_COMPARISON',
                    'sample_size': 0,
                    'sample_percentage': 0.0,
                    'columns_checked': 0,
                    'columns_matched': 0,
                    'match_percentage': 0.0,
                    'status': 'WARNING',
                    'error_message': 'No data to compare',
                    'duration': time.time() - start_time,
                    'missing_columns': [],
                    'unexpected_additional_columns': []
                }
            
            # Use statistical comparison instead of row-by-row sampling
            comparison_result = self.compare_column_statistics(source_table, target_table, source_schema)
            
            # Extract results
            match_percentage = comparison_result['match_percentage']
            mismatch_details = comparison_result.get('mismatch_details', [])
            missing_columns = comparison_result.get('missing_columns', [])
            unexpected_additional = comparison_result.get('unexpected_additional_columns', [])
            
            # Build comprehensive error message
            error_parts = []
            
            # Always include missing columns if any exist
            if missing_columns:
                error_parts.append(f"Missing columns in target: {', '.join(missing_columns)}")
            
            # Include unexpected additional columns (excluding expected ones like U_ID)
            if unexpected_additional:
                error_parts.append(f"Unexpected additional columns: {', '.join(unexpected_additional)}")
            
            # Include statistical mismatch details
            error_parts.extend(mismatch_details)
            
            # Determine status based on match percentage and missing columns
            if match_percentage >= 95 and not missing_columns:
                status = 'PASSED'
                error_message = ' | '.join(error_parts) if error_parts else None
            elif match_percentage >= 80 and not missing_columns:
                status = 'WARNING'
                error_message = f"Statistical match below threshold: {match_percentage:.1f}%"
                if error_parts:
                    error_message += f" | {' | '.join(error_parts)}"
            else:
                status = 'FAILED'
                if missing_columns:
                    error_message = f"Missing columns detected. Statistical match: {match_percentage:.1f}%"
                else:
                    error_message = f"Poor statistical match: {match_percentage:.1f}%"
                if error_parts:
                    error_message += f" | {' | '.join(error_parts)}"
            
            duration = time.time() - start_time
            
            result = {
                'check_type': 'CONTENT_COMPARISON',
                'sample_size': actual_count,  # Full table statistical analysis
                'sample_percentage': 1.0,  # 100% statistical coverage
                'columns_checked': comparison_result['columns_checked'],
                'columns_matched': comparison_result['columns_matched'],
                'match_percentage': match_percentage,
                'status': status,
                'error_message': error_message,
                'duration': duration,
                'details': comparison_result.get('column_details', {}),
                'mismatch_details': mismatch_details,
                'analysis_method': 'statistical_aggregates'
            }
            
            self.logger.info(f"Statistical comparison - {source_table}: "
                           f"columns_checked={comparison_result['columns_checked']}, "
                           f"match_rate={match_percentage:.1f}%")
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Statistical comparison failed: {e}"
            self.logger.error(error_msg)
            
            return {
                'check_type': 'CONTENT_COMPARISON',
                'sample_size': 0,
                'sample_percentage': 0.0,
                'columns_checked': 0,
                'columns_matched': 0,
                'match_percentage': 0.0,
                'status': 'FAILED',
                'error_message': error_msg,
                'duration': duration,
                'missing_columns': [],
                'unexpected_additional_columns': [],
                'analysis_method': 'statistical_aggregates'
            }
    
    def record_check_result(self, source_table: str, target_table: str, 
                          result: Dict[str, Any]) -> None:
        """Record quality check result in the results table"""
        try:
            # Prepare insert statement
            insert_sql = f"""
            INSERT INTO {self.results_table} (
                TABLE_NAME, SOURCE_TABLE, CHECK_TYPE, CHECK_TIME,
                SOURCE_ROW_COUNT, TARGET_ROW_COUNT, ROW_COUNT_MATCH,
                SAMPLE_SIZE, SAMPLE_PERCENTAGE, COLUMNS_CHECKED, COLUMNS_MATCHED, 
                CONTENT_MATCH_PERCENTAGE, CHECK_STATUS, ERROR_MESSAGE,
                CHECK_DURATION_SECONDS, ETL_SESSION_ID
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16
            )
            """
            
            # Prepare values
            values = (
                target_table,
                source_table,
                result['check_type'],
                datetime.now(),
                result.get('source_count'),
                result.get('target_count'),
                'Y' if result.get('match', False) else 'N',
                result.get('sample_size'),
                result.get('sample_percentage'),
                result.get('columns_checked'),
                result.get('columns_matched'),
                result.get('match_percentage'),
                result['status'],
                result.get('error_message'),
                round(result['duration'], 2),
                self.session_id
            )
            
            self.oracle_conn.execute(insert_sql, values)
            self.oracle_conn.commit()
            
            self.logger.debug(f"Recorded quality check result for {target_table}")
            
        except Exception as e:
            self.logger.error(f"Failed to record quality check result: {e}")
    
    def run_quality_checks(self, source_table: str, target_table: str,
                          source_schema: str = 'dbo', 
                          primary_key: Optional[str] = None) -> Dict[str, Any]:
        """Run comprehensive quality checks for a transferred table"""
        
        if not self.quality_config.get('enabled', True):
            self.logger.info(f"Quality checks disabled - skipping {target_table}")
            return {'enabled': False, 'checks': []}
        
        self.logger.info(f"Starting quality checks for {target_table}")
        overall_start_time = time.time()
        
        # Initialize results table if needed
        self.initialize_results_table()
        
        results = {
            'source_table': source_table,
            'target_table': target_table,
            'checks': [],
            'overall_status': 'PASSED',
            'total_duration': 0
        }
        
        try:
            # Row count check
            if self.quality_config.get('row_count_check', True):
                self.etl_logger.log_table_start(target_table, "row count check")
                row_count_result = self.check_row_counts(source_table, target_table, source_schema)
                results['checks'].append(row_count_result)
                self.record_check_result(source_table, target_table, row_count_result)
                self.etl_logger.log_table_complete(target_table, "row count check")
                
                if row_count_result['status'] == 'FAILED':
                    results['overall_status'] = 'FAILED'
            
            # Content comparison check
            content_config = self.quality_config.get('content_comparison', {})
            if content_config.get('enabled', True):
                self.etl_logger.log_table_start(target_table, "content comparison")
                
                # Get source count from row count check if available
                source_count = 0
                if results['checks']:
                    source_count = results['checks'][0].get('source_count', 0)
                
                content_result = self.check_content_comparison(
                    source_table, target_table, source_schema, source_count, primary_key
                )
                results['checks'].append(content_result)
                self.record_check_result(source_table, target_table, content_result)
                self.etl_logger.log_table_complete(target_table, "content comparison")
                
                if content_result['status'] == 'FAILED':
                    results['overall_status'] = 'FAILED'
                elif content_result['status'] == 'WARNING' and results['overall_status'] == 'PASSED':
                    results['overall_status'] = 'WARNING'
            
            results['total_duration'] = time.time() - overall_start_time
            
            self.logger.info(f"Quality checks completed for {target_table} - Status: {results['overall_status']}")
            return results
            
        except Exception as e:
            results['overall_status'] = 'FAILED'
            results['total_duration'] = time.time() - overall_start_time
            error_msg = f"Quality checks failed for {target_table}: {e}"
            results['error'] = error_msg
            self.logger.error(error_msg)
            return results