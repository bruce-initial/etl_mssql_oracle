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
    
    def get_random_sample(self, source_table: str, target_table: str, 
                         source_schema: str, sample_size: int, 
                         primary_key: Optional[str] = None) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Get random sample from both source and target tables"""
        
        # If we have a primary key, use it for consistent sampling
        if primary_key:
            # Get all primary key values
            pk_query_source = f"SELECT {primary_key} FROM {source_schema}.{source_table}"
            pk_query_target = f"SELECT {primary_key} FROM {target_table}"
            
            source_pks = self.mssql_conn.execute_all(pk_query_source)
            target_pks = self.oracle_conn.execute_all(pk_query_target)
            
            # Find common primary keys
            source_pk_set = {str(row[0]) for row in source_pks}
            target_pk_set = {str(row[0]) for row in target_pks}
            common_pks = list(source_pk_set.intersection(target_pk_set))
            
            # Sample from common keys
            sampled_pks = random.sample(common_pks, min(sample_size, len(common_pks)))
            
            # Build IN clause
            pk_list = "', '".join(sampled_pks)
            where_clause = f"WHERE {primary_key} IN ('{pk_list}')"
            
        else:
            # Without primary key, get all data first to find common columns for ordering
            source_query_temp = f"SELECT * FROM {source_schema}.{source_table}"
            target_query_temp = f"SELECT * FROM {target_table}"
            
            # Get column info to find a common column for ordering
            source_cols_query = f"SELECT TOP 1 * FROM {source_schema}.{source_table}"
            target_cols_query = f"SELECT * FROM {target_table} WHERE ROWNUM <= 1"
            
            source_sample_temp = self.mssql_conn.read_table_as_dataframe(source_cols_query)
            target_sample_temp = self.oracle_conn.read_table_as_dataframe(target_cols_query)
            
            # Find first common column (case-insensitive)
            source_cols_lower = [col.lower() for col in source_sample_temp.columns]
            target_cols_lower = [col.lower() for col in target_sample_temp.columns]
            
            common_col = None
            for i, source_col_lower in enumerate(source_cols_lower):
                if source_col_lower in target_cols_lower:
                    source_col_actual = source_sample_temp.columns[i]
                    target_col_idx = target_cols_lower.index(source_col_lower)
                    target_col_actual = target_sample_temp.columns[target_col_idx]
                    common_col = (source_col_actual, target_col_actual)
                    break
            
            if common_col:
                source_order_col, target_order_col = common_col
                where_clause = f"ORDER BY [{source_order_col}] OFFSET 0 ROWS FETCH FIRST {sample_size} ROWS ONLY"
                target_order_clause = f"ORDER BY {target_order_col}"
            else:
                # Fallback to first column if no common columns found
                where_clause = f"ORDER BY 1 OFFSET 0 ROWS FETCH FIRST {sample_size} ROWS ONLY"
                target_order_clause = "ORDER BY 1"
        
        # Get source sample
        source_query = f"SELECT * FROM {source_schema}.{source_table} {where_clause}"
        self.logger.info(f"Quality Check (source query): {source_query}")
        source_df = self.mssql_conn.read_table_as_dataframe(source_query)
        
        # For Oracle, use the same sampling approach to ensure we compare identical rows
        if primary_key and 'WHERE' in where_clause:
            target_query = f"SELECT * FROM {target_table} {where_clause}"
        else:
            # Use consistent ordering with source
            actual_sample_size = len(source_df)
            target_query = f"SELECT * FROM (SELECT * FROM {target_table} {target_order_clause}) WHERE ROWNUM <= {actual_sample_size}"
        
        self.logger.info(f"Quality Check (target query): {target_query}")
        target_df = self.oracle_conn.read_table_as_dataframe(target_query)

        # Ensure both DataFrames are sorted by first column to guarantee row alignment
        if len(source_df) > 0 and len(target_df) > 0:
            first_col_source = source_df.columns[0]
            first_col_target = target_df.columns[0]
            source_df = source_df.sort(first_col_source)
            target_df = target_df.sort(first_col_target)

        return source_df, target_df
    
    def _apply_etl_transformations_to_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply the same data transformations that are used during ETL process"""
        
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
                elif str(val).lower() in ['null', 'none', '<null>']:
                    processed_row.append(None)
                elif isinstance(val, bool):
                    processed_row.append('1' if val else '0')
                elif str(val).lower() in ['true', 'false']:
                    processed_row.append('1' if str(val).lower() == 'true' else '0')
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
    
    def compare_dataframe_content(self, source_df: pl.DataFrame, 
                                 target_df: pl.DataFrame) -> Dict[str, Any]:
        """Compare content between source and target dataframes"""
        
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
        
        # Handle case sensitivity differences: MSSQL is case-insensitive, Oracle is case-sensitive
        # For MSSQL source: use case-insensitive matching (normalize to lowercase)
        # For Oracle target: preserve exact case but allow fuzzy matching for missing columns
        
        source_cols = {}  # key: normalized name, value: list of actual column names
        target_cols = {}  # key: actual column name, value: actual column name
        target_cols_normalized = {}  # key: normalized name, value: list of actual column names
        
        # Build source column mapping (MSSQL - case insensitive)
        for col in source_df.columns:
            col_lower = col.lower()
            if col_lower not in source_cols:
                source_cols[col_lower] = []
            source_cols[col_lower].append(col)
        
        # Build target column mappings (Oracle - case sensitive)
        for col in target_df.columns:
            target_cols[col] = col  # Exact case mapping
            
            # Also build normalized mapping for fuzzy matching
            col_lower = col.lower()
            if col_lower not in target_cols_normalized:
                target_cols_normalized[col_lower] = []
            target_cols_normalized[col_lower].append(col)
        
        # Find matches with case sensitivity handling
        # 1. Try exact case matches first (for Oracle target columns)
        # 2. Fall back to case-insensitive matches
        common_cols = set()
        exact_matches = {}  # source_col_norm -> target_col_exact
        
        for source_norm in source_cols.keys():
            # Try to find exact case match first
            source_actual_names = source_cols[source_norm]
            
            if source_norm in target_cols_normalized:
                target_actual_names = target_cols_normalized[source_norm]
                
                # Prefer exact case match if available
                target_match = None
                for source_actual in source_actual_names:
                    if source_actual in target_actual_names:
                        target_match = source_actual  # Exact case match
                        break
                
                # If no exact match, use the first available target column
                if target_match is None:
                    target_match = target_actual_names[0]
                
                common_cols.add(source_norm)
                exact_matches[source_norm] = target_match
        
        # Track missing and additional columns
        source_only_cols = set(source_cols.keys()) - common_cols
        target_only_cols = set(target_cols_normalized.keys()) - common_cols
        
        # Get the actual column names for missing/additional columns
        missing_columns = []
        for col_norm in source_only_cols:
            missing_columns.extend(source_cols[col_norm])
        
        additional_columns = []
        for col_norm in target_only_cols:
            additional_columns.extend(target_cols_normalized[col_norm])
        
        mismatch_details = []
        
        # Report additional columns
        if additional_columns:
            mismatch_details.append(f"Additional columns in target: {', '.join(additional_columns)}")
        
        if missing_columns:
            mismatch_details.append(f"Missing columns in target: {', '.join(missing_columns)}")
        
        if not common_cols:
            return {
                'columns_checked': 0,
                'columns_matched': 0,
                'match_percentage': 0.0,
                'total_source_columns': len(source_cols),
                'missing_columns': missing_columns,
                'additional_columns': additional_columns,
                'details': 'No common columns found',
                'mismatch_details': mismatch_details
            }
        
        columns_checked = len(common_cols)
        columns_matched = 0
        column_details = {}
        
        # Compare each common column with case sensitivity handling
        for col_norm in common_cols:
            source_col_list = source_cols[col_norm]
            target_col = exact_matches[col_norm]
            
            # Check for case variations in source and report them
            if len(source_col_list) > 1:
                mismatch_details.append(f"Source has multiple case variations for '{col_norm}': {', '.join(source_col_list)}")
            
            # Use the first source column name for comparison
            source_col = source_col_list[0]
            
            # Report case mismatch if source and target column names differ in case
            if source_col != target_col:
                mismatch_details.append(f"Case mismatch: source '{source_col}' vs target '{target_col}'")
            
            try:
                # Get column values from both dataframes (already cast to strings)
                source_values = source_df[source_col].sort()
                target_values = target_df[target_col].sort()
                
                # Compare values (handling nulls and different lengths)
                if len(source_values) == len(target_values):
                    # Element-wise comparison using Polars with string casting
                    # Handle nulls by checking both null or both equal
                    source_is_null = source_values.is_null()
                    target_is_null = target_values.is_null()
                    both_null = source_is_null & target_is_null
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
        total_source_columns = len(source_cols)
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
        """Compare sample content between source and target tables"""
        start_time = time.time()
        
        try:
            # Determine sample size
            actual_count = source_count if source_count > 0 else self.get_table_count(source_table, source_schema)
            sample_size = self.determine_sample_size(actual_count)
            
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
                    'additional_columns': []
                }
            
            # Get random samples
            source_df, target_df = self.get_random_sample(
                source_table, target_table, source_schema, sample_size, primary_key
            )
            
            # Convert both dataframes to all strings to ensure schema consistency
            # Apply the same data transformations to source data that are applied during ETL
            source_df = self._apply_etl_transformations_to_dataframe(source_df)
            target_df = target_df.with_columns([
                pl.col(col).cast(pl.String) for col in target_df.columns
            ])
            
            # Compare content
            comparison_result = self.compare_dataframe_content(source_df, target_df)
            
            # Determine overall status and build detailed error message
            match_percentage = comparison_result['match_percentage']
            mismatch_details = comparison_result.get('mismatch_details', [])
            missing_columns = comparison_result.get('missing_columns', [])
            additional_columns = comparison_result.get('additional_columns', [])
            
            # Build comprehensive error message that includes missing/additional columns
            error_parts = []
            
            # Always include missing columns if any exist
            if missing_columns:
                error_parts.append(f"Missing columns in target: {', '.join(missing_columns)}")
            
            # Include additional columns if any exist
            if additional_columns:
                error_parts.append(f"Additional columns in target: {', '.join(additional_columns)}")
            
            # Include other mismatch details (excluding missing/additional column details to avoid duplication)
            other_details = [detail for detail in mismatch_details 
                           if not detail.startswith('Missing columns in target:') 
                           and not detail.startswith('Additional columns in target:')]
            error_parts.extend(other_details)
            
            # Determine status based on match percentage and missing columns
            if match_percentage >= 95 and not missing_columns:
                status = 'PASSED'
                error_message = ' | '.join(error_parts) if error_parts else None
            elif match_percentage >= 80 and not missing_columns:
                status = 'WARNING'
                error_message = f"Content match below threshold: {match_percentage:.1f}%"
                if error_parts:
                    error_message += f" | {' | '.join(error_parts)}"
            else:
                status = 'FAILED'
                if missing_columns:
                    error_message = f"Missing columns detected. Content match: {match_percentage:.1f}%"
                else:
                    error_message = f"Poor content match: {match_percentage:.1f}%"
                if error_parts:
                    error_message += f" | {' | '.join(error_parts)}"
            
            duration = time.time() - start_time
            actual_sample_percentage = (len(source_df) / actual_count) if actual_count > 0 else 0
            
            result = {
                'check_type': 'CONTENT_COMPARISON',
                'sample_size': len(source_df),
                'sample_percentage': actual_sample_percentage,
                'columns_checked': comparison_result['columns_checked'],
                'columns_matched': comparison_result['columns_matched'],
                'match_percentage': match_percentage,
                'status': status,
                'error_message': error_message,
                'duration': duration,
                'details': comparison_result.get('column_details', {}),
                'mismatch_details': mismatch_details
            }
            
            self.logger.info(f"Content comparison - {source_table}: sampled={len(source_df)}, "
                           f"columns_checked={comparison_result['columns_checked']}, "
                           f"match_rate={match_percentage:.1f}%")
            
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Content comparison failed: {e}"
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
                'additional_columns': []
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