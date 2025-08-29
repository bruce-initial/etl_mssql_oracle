import duckdb
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import time
import logging

from ..connections.mssql import MSSQLConnection
from ..connections.oracle import OracleConnection
from ..config.manager import ConfigurationManager
from ..mappers.data_types import DataTypeMapper
from ..dependency.resolver import DependencyResolver
from ..quality.checker import DataQualityChecker
from ..utils.logging import ETLLogger
from ..utils.exceptions import TransferError, ValidationError, SQLError

logger = logging.getLogger(__name__)


class TransferEngine:
    """Main ETL transfer engine"""
    
    def __init__(self, config_path: str = "config/tables_config.yaml"):
        self.config_manager = ConfigurationManager(config_path)
        self.data_mapper = DataTypeMapper()
        self.mssql_conn: Optional[MSSQLConnection] = None
        self.oracle_conn: Optional[OracleConnection] = None
        self.duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        self.transferred_tables: Dict[str, Dict[str, Any]] = {}
        self.quality_checker: Optional[DataQualityChecker] = None
        
        # Setup enhanced logging
        self.etl_logger = ETLLogger(
            logs_dir=self.config_manager.get_logs_path(),
            log_level="INFO"
        )
        self.logger = self.etl_logger.get_logger()
    
    def initialize_connections(self) -> None:
        """Initialize all database connections"""
        try:
            # Initialize DuckDB
            self.duckdb_conn = duckdb.connect()
            self.logger.info("DuckDB connection initialized")
            
            # Initialize MSSQL connection
            mssql_creds = self.config_manager.get_database_credentials('mssql')
            self.mssql_conn = MSSQLConnection(mssql_creds)
            self.mssql_conn.connect()
            self.logger.info("MSSQL connection initialized")
            
            # Initialize Oracle connection
            oracle_creds = self.config_manager.get_database_credentials('oracle')
            self.oracle_conn = OracleConnection(oracle_creds)
            self.oracle_conn.connect()
            self.logger.info("ORACLE connection initialized")
            
            # Initialize data quality checker
            config_dict = self.config_manager.get_full_config()
            self.quality_checker = DataQualityChecker(
                self.mssql_conn, self.oracle_conn, config_dict, self.etl_logger
            )
            self.logger.info("Data quality checker initialized")
            
            self.logger.info("All database connections initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize connections: {e}")
            self.close_connections()
            raise
    
    def close_connections(self) -> None:
        """Close all database connections"""
        if self.mssql_conn:
            self.mssql_conn.disconnect()
        if self.oracle_conn:
            self.oracle_conn.disconnect()
        if self.duckdb_conn:
            self.duckdb_conn.close()
        self.logger.info("All database connections closed")
    
    def load_sql_query(self, filename: str) -> str:
        """Load SQL query from file"""
        sql_path = Path(self.config_manager.get_sql_path()) / filename
        try:
            with open(sql_path, 'r') as file:
                return file.read().strip()
        except FileNotFoundError:
            raise SQLError(f"SQL file not found: {sql_path}")
    
    def read_source_data(self, table_config: Dict[str, Any]) -> Dict[str, Any]:
        """Read data from source SQL Server table"""
        start_time = time.time()
        
        # Build SQL query
        if table_config.get('custom_query'):
            query_template = self.load_sql_query(table_config['custom_query'])
            query = query_template.format(
                schema=table_config.get('source_schema', 'dbo'),
                table_name=table_config['source_table']
            )
        else:
            schema = table_config.get('source_schema', 'dbo')
            table_name = table_config['source_table']
            query = f"SELECT * FROM {schema}.{table_name}"
        
        self.etl_logger.log_table_start(table_config['source_table'], "read")
        self.logger.debug(f"SQL Query: {query}")
        
        try:
            # Read data using polars
            df = self.mssql_conn.read_table_as_dataframe(query)
            
            # Register with DuckDB for potential processing
            temp_table_name = f"temp_{table_config['target_table'].lower()}"
            # Register polars DataFrame with DuckDB using PyArrow
            try:
                # Convert polars to arrow table for DuckDB
                arrow_table = df.to_arrow()
                self.duckdb_conn.register(temp_table_name, arrow_table)
            except Exception as e:
                self.logger.warning(f"Could not register with DuckDB: {e}")
                # DuckDB registration is optional, continue without it
                temp_table_name = None
            
            duration = time.time() - start_time
            row_count = len(df)
            
            self.etl_logger.log_table_complete(
                table_config['source_table'], 
                "read", 
                row_count
            )
            
            self.etl_logger.log_performance_metric(
                f"Read {table_config['source_table']}", 
                duration,
                {'rows_per_second': row_count / duration if duration > 0 else 0}
            )
            
            # Convert date/datetime columns to appropriate string formats
            datetime_cols = []
            for col in df.columns:
                if df[col].dtype.is_temporal():
                    if df[col].dtype == pl.Date:
                        datetime_cols.append(
                            pl.col(col).dt.strftime('%Y-%m-%d').alias(col)
                        )
                    else:
                        datetime_cols.append(
                            pl.col(col).dt.strftime('%Y-%m-%d %H:%M:%S.%f').alias(col)
                        )
            
            if datetime_cols:
                datetime_fixed_df = df.with_columns(datetime_cols)
            else:
                datetime_fixed_df = df
            
            return {
                'dataframe': datetime_fixed_df,
                'data': datetime_fixed_df.rows(),
                'columns': datetime_fixed_df.columns,
                'temp_table': temp_table_name,
                'row_count': row_count
            }
            
        except Exception as e:
            self.logger.error(f"Failed to read table {table_config['source_table']}: {e}")
            raise TransferError(f"Source data read failed: {e}")
    
    def create_target_table(self, table_config: Dict[str, Any], source_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create target Oracle table with proper schema"""
        target_table = table_config['target_table']
        df = source_data['dataframe']
        
        start_time = time.time()
        self.etl_logger.log_table_start(target_table, "create table")
        
        try:
            # Drop existing table if configured
            if self.config_manager.should_drop_existing_tables():
                self.oracle_conn.drop_table_if_exists(target_table)
            
            # Analyze DataFrame columns
            column_analysis = self.data_mapper.analyze_dataframe_columns(df)
            
            # Generate Oracle column definitions
            oracle_columns = self.data_mapper.generate_oracle_column_definitions(column_analysis)
            
            # Create column mapping
            column_mapping = self.data_mapper.create_column_mapping(column_analysis)
            
            # Build CREATE TABLE statement
            all_definitions = oracle_columns.copy()
            
            # Add primary key constraint
            if table_config.get('primary_key'):
                pk_columns = table_config['primary_key']
                if isinstance(pk_columns, list):
                    sanitized_pk_columns = [column_mapping.get(col, self.data_mapper.sanitize_column_name(col)) for col in pk_columns]
                    pk_constraint = f"CONSTRAINT pk_{target_table} PRIMARY KEY ({', '.join(sanitized_pk_columns)})"
                else:
                    sanitized_pk_col = column_mapping.get(pk_columns, self.data_mapper.sanitize_column_name(pk_columns))
                    pk_constraint = f"CONSTRAINT pk_{target_table} PRIMARY KEY ({sanitized_pk_col})"
                
                all_definitions.append(f"    {pk_constraint}")
            
            # Create table
            create_sql = f"""CREATE TABLE {target_table} (
{','.join([chr(10) + def_line for def_line in all_definitions])}
)"""
            
            self.logger.debug(f"CREATE TABLE statement:\n{create_sql}")
            self.oracle_conn.execute(create_sql)
            
            duration = time.time() - start_time
            self.etl_logger.log_table_complete(target_table, "create table")
            self.etl_logger.log_performance_metric(f"Create table {target_table}", duration)
            
            return {
                'column_analysis': column_analysis,
                'column_mapping': column_mapping,
                'sanitized_columns': [info['sanitized_name'] for info in column_analysis.values()]
            }
            
        except Exception as e:
            self.logger.error(f"Failed to create Oracle table {target_table}: {e}")
            raise TransferError(f"Table creation failed: {e}")
    
    def insert_data(self, table_config: Dict[str, Any], source_data: Dict[str, Any], 
                   table_info: Dict[str, Any]) -> None:
        """Insert data into Oracle table"""
        target_table = table_config['target_table']
        data = source_data['data']
        column_mapping = table_info['column_mapping']
        sanitized_columns = table_info['sanitized_columns']
        
        start_time = time.time()
        self.etl_logger.log_table_start(target_table, "insert data")
        
        try:
            # Validate data
            self.data_mapper.validate_column_mapping(
                source_data['columns'], 
                sanitized_columns
            )
            
            # Process data for Oracle
            processed_data = self.data_mapper.process_data_for_oracle(
                data, 
                table_info['column_analysis']
            )
            
            # Prepare insert statement
            insert_template = self.load_sql_query('oracle_insert_table.sql')
            placeholders = ", ".join([f":{i+1}" for i in range(len(sanitized_columns))])
            
            insert_sql = insert_template.format(
                table_name=target_table,
                columns=', '.join(sanitized_columns),
                placeholders=placeholders
            )
            
            # Insert data in batches
            batch_size = table_config.get('batch_size', self.config_manager.get_default_batch_size())
            total_rows = len(processed_data)
            total_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(0, total_rows, batch_size):
                batch = processed_data[i:i+batch_size]
                batch_num = i // batch_size + 1
                
                try:
                    self.oracle_conn.executemany(insert_sql, batch)
                    self.etl_logger.log_batch_progress(
                        batch_num, total_batches, len(batch), target_table
                    )
                except Exception as e:
                    self.logger.error(f"Error inserting batch {batch_num}: {e}")
                    raise
            
            duration = time.time() - start_time
            self.etl_logger.log_table_complete(target_table, "insert data", total_rows)
            self.etl_logger.log_performance_metric(
                f"Insert data {target_table}", 
                duration,
                {
                    'rows_per_second': total_rows / duration if duration > 0 else 0,
                    'total_batches': total_batches
                }
            )
            
        except Exception as e:
            self.logger.error(f"Failed to insert data into {target_table}: {e}")
            raise TransferError(f"Data insertion failed: {e}")
    
    def create_indexes(self, table_config: Dict[str, Any], column_mapping: Dict[str, str]) -> None:
        """Create indexes on Oracle table"""
        if not self.config_manager.should_create_indexes():
            return
        
        indexes = table_config.get('indexes', [])
        if not indexes:
            return
        
        target_table = table_config['target_table']
        
        for index_config in indexes:
            try:
                index_name = index_config.get('name', f"idx_{target_table}_{index_config['columns'][0]}")
                
                # Map column names
                mapped_columns = []
                for col in index_config['columns']:
                    mapped_col = column_mapping.get(col, self.data_mapper.sanitize_column_name(col))
                    mapped_columns.append(mapped_col)
                
                index_template = self.load_sql_query('oracle_create_index.sql')
                index_sql = index_template.format(
                    index_name=index_name,
                    table_name=target_table,
                    columns=', '.join(mapped_columns),
                    unique='UNIQUE' if index_config.get('unique', False) else ''
                )
                
                self.oracle_conn.create_index(index_sql)
                self.logger.info(f"Created index: {index_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to create index {index_name}: {e}")
    
    def verify_transfer(self, table_config: Dict[str, Any], expected_count: int) -> bool:
        """Verify data transfer success"""
        if not self.config_manager.should_verify_transfers():
            return True
        
        target_table = table_config['target_table']
        
        try:
            actual_count = self.oracle_conn.get_row_count(target_table)
            
            if actual_count == expected_count:
                self.logger.info(f"✓ Verification successful: {target_table} contains {actual_count} rows")
                return True
            else:
                self.logger.error(f"✗ Verification failed: {target_table} - Expected {expected_count}, got {actual_count}")
                return False
                
        except Exception as e:
            self.logger.error(f"Verification failed for {target_table}: {e}")
            return False
    
    def transfer_single_table(self, table_config: Dict[str, Any]) -> bool:
        """Transfer a single table from SQL Server to Oracle"""
        table_name = table_config['target_table']
        
        try:
            # Validate configuration
            self.config_manager.validate_table_config(table_config)
            
            # Read source data
            source_data = self.read_source_data(table_config)
            
            # Create target table
            table_info = self.create_target_table(table_config, source_data)
            
            # Insert data
            self.insert_data(table_config, source_data, table_info)
            
            # Create indexes
            self.create_indexes(table_config, table_info['column_mapping'])
            
            # Verify transfer
            success = self.verify_transfer(table_config, source_data['row_count'])
            
            # Run data quality checks
            quality_results = None
            if success and self.quality_checker:
                try:
                    primary_key = table_config.get('primary_key')
                    # Convert list to string if needed
                    if isinstance(primary_key, list):
                        primary_key = primary_key[0] if primary_key else None
                    
                    quality_results = self.quality_checker.run_quality_checks(
                        source_table=table_config['source_table'],
                        target_table=table_name,
                        source_schema=table_config.get('source_schema', 'dbo'),
                        primary_key=primary_key
                    )
                    
                    # Update success based on quality check results
                    if quality_results.get('overall_status') == 'FAILED':
                        success = False
                        self.logger.warning(f"Quality checks failed for {table_name}")
                    
                except Exception as e:
                    self.logger.error(f"Quality checks failed for {table_name}: {e}")
                    # Don't fail the entire transfer if quality checks fail
            
            # Store transfer info
            self.transferred_tables[table_name] = {
                'config': table_config,
                'success': success,
                'row_count': source_data['row_count'],
                'column_mapping': table_info['column_mapping'],
                'quality_results': quality_results
            }
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to transfer table {table_name}: {e}")
            return False
    
    def create_foreign_keys(self) -> None:
        """Create foreign key relationships between transferred tables"""
        if not self.config_manager.should_create_foreign_keys():
            return
        
        self.logger.info("Creating foreign key relationships...")
        
        for table_name, table_info in self.transferred_tables.items():
            if not table_info['success']:
                continue
            
            table_config = table_info['config']
            foreign_keys = table_config.get('foreign_keys', [])
            column_mapping = table_info['column_mapping']
            
            for fk_config in foreign_keys:
                try:
                    fk_name = fk_config.get('name', f"fk_{table_name}_{fk_config['column']}")
                    
                    # Map column names
                    mapped_column = column_mapping.get(
                        fk_config['column'], 
                        self.data_mapper.sanitize_column_name(fk_config['column'])
                    )
                    
                    fk_template = self.load_sql_query('oracle_create_foreign_key.sql')
                    fk_sql = fk_template.format(
                        table_name=table_name,
                        constraint_name=fk_name,
                        column=mapped_column,
                        referenced_table=fk_config['referenced_table'],
                        referenced_column=fk_config['referenced_column']
                    )
                    
                    self.oracle_conn.create_constraint(fk_sql)
                    self.logger.info(f"Created foreign key: {fk_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to create foreign key {fk_name}: {e}")
    
    def transfer_all_tables(self) -> Dict[str, Any]:
        """Transfer all tables according to configuration"""
        start_time = time.time()
        
        try:
            # Validate all configurations
            self.config_manager.validate_all_configurations()
            
            # Initialize connections
            self.initialize_connections()
            
            # Resolve dependencies
            tables_config = self.config_manager.get_tables_config()
            dependency_resolver = DependencyResolver(tables_config)
            dependency_resolver.validate_dependencies()
            
            # Get transfer order
            ordered_tables = dependency_resolver.resolve_dependencies()
            
            # Transfer tables
            success_count = 0
            total_count = len(ordered_tables)
            
            for table_config in ordered_tables:
                table_name = table_config['target_table']
                self.logger.info(f"Starting transfer of {table_name}...")
                
                if self.transfer_single_table(table_config):
                    success_count += 1
                    self.oracle_conn.commit()
                    self.logger.info(f"Successfully transferred {table_name}")
                else:
                    self.logger.error(f"Failed to transfer {table_name}")
                    self.oracle_conn.rollback()
            
            # Create foreign key relationships
            if success_count > 0:
                self.create_foreign_keys()
                self.oracle_conn.commit()
            
            total_duration = time.time() - start_time
            
            # Generate quality check summary
            quality_summary = self._generate_quality_summary()
            
            # Log final results
            self.logger.info(f"Transfer completed: {success_count}/{total_count} tables successful")
            if quality_summary:
                self.logger.info(f"Quality checks: {quality_summary['passed']}/{quality_summary['total']} passed")
            
            self.etl_logger.log_performance_metric(
                "Complete ETL process", 
                total_duration,
                {
                    'successful_tables': success_count,
                    'total_tables': total_count,
                    'success_rate': (success_count / total_count) * 100 if total_count > 0 else 0,
                    'quality_summary': quality_summary
                }
            )
            
            return {
                'success': success_count == total_count,
                'successful_tables': success_count,
                'total_tables': total_count,
                'duration': total_duration,
                'transferred_tables': self.transferred_tables,
                'quality_summary': quality_summary
            }
            
        except Exception as e:
            self.logger.error(f"Transfer process failed: {e}")
            if self.oracle_conn:
                self.oracle_conn.rollback()
            raise
        finally:
            self.close_connections()
    
    def _generate_quality_summary(self) -> Optional[Dict[str, Any]]:
        """Generate summary of all quality check results"""
        if not self.quality_checker:
            return None
        
        quality_tables = [
            table_info for table_info in self.transferred_tables.values() 
            if table_info.get('quality_results')
        ]
        
        if not quality_tables:
            return None
        
        total_checks = len(quality_tables)
        passed_checks = sum(
            1 for table_info in quality_tables 
            if table_info['quality_results'].get('overall_status') == 'PASSED'
        )
        warning_checks = sum(
            1 for table_info in quality_tables 
            if table_info['quality_results'].get('overall_status') == 'WARNING'
        )
        failed_checks = sum(
            1 for table_info in quality_tables 
            if table_info['quality_results'].get('overall_status') == 'FAILED'
        )
        
        return {
            'total': total_checks,
            'passed': passed_checks,
            'warning': warning_checks,
            'failed': failed_checks,
            'success_rate': (passed_checks / total_checks * 100) if total_checks > 0 else 0
        }