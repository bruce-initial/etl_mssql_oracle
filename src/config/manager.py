import os
import yaml
from typing import Dict, Any, List
from pathlib import Path
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Manages configuration and environment settings"""
    
    def __init__(self, config_path: str = "config/tables_config.yaml"):
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.credentials: Dict[str, Dict[str, str]] = {}
        self._load_environment()
        self._load_config()
        self._load_credentials()
    
    def _load_environment(self) -> None:
        """Load environment variables"""
        load_dotenv()
        logger.info("Environment variables loaded")
    
    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
            
            with open(config_file, 'r') as file:
                self.config = yaml.safe_load(file)
                logger.info(f"Configuration loaded from {self.config_path}")
                
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML configuration: {e}")
    
    def _load_credentials(self) -> None:
        """Load database credentials from environment variables"""
        self.credentials = {
            'mssql': {
                'host': os.getenv('MSSQL_HOST'),
                'port': os.getenv('MSSQL_PORT', '1433'),
                'user': os.getenv('MSSQL_USER'),
                'password': os.getenv('MSSQL_PASSWORD'),
                'database': os.getenv('MSSQL_DATABASE')
            },
            'oracle': {
                'host': os.getenv('ORACLE_HOST'),
                'port': os.getenv('ORACLE_PORT', '1521'),
                'user': os.getenv('ORACLE_USER'),
                'password': os.getenv('ORACLE_PASSWORD'),
                'service': os.getenv('ORACLE_SERVICE')
            }
        }
        
        # Validate credentials
        for db_type, creds in self.credentials.items():
            missing = [key for key, value in creds.items() if not value]
            if missing:
                raise ValueError(f"Missing {db_type.upper()} credentials: {missing}")
        
        logger.info("Database credentials validated")
    
    def get_tables_config(self) -> List[Dict[str, Any]]:
        """Get tables configuration"""
        tables = self.config.get('tables', [])
        if not tables:
            raise ValueError("No tables defined in configuration")
        return tables
    
    def get_settings(self) -> Dict[str, Any]:
        """Get global settings"""
        return self.config.get('settings', {})
    
    def get_full_config(self) -> Dict[str, Any]:
        """Get complete configuration including settings"""
        return self.config
    
    def get_database_credentials(self, db_type: str) -> Dict[str, str]:
        """Get credentials for specific database type"""
        if db_type not in self.credentials:
            raise ValueError(f"Unknown database type: {db_type}")
        return self.credentials[db_type]
    
    def get_sql_path(self) -> str:
        """Get SQL files directory path"""
        return self.config.get('sql_path', 'sql')
    
    def get_logs_path(self) -> str:
        """Get logs directory path"""
        return self.config.get('logs_path', 'logs')
    
    def get_default_batch_size(self) -> int:
        """Get default batch size for operations"""
        return self.get_settings().get('default_batch_size', 1000)
    
    def should_create_foreign_keys(self) -> bool:
        """Check if foreign keys should be created"""
        return self.get_settings().get('create_foreign_keys', True)
    
    def should_create_indexes(self) -> bool:
        """Check if indexes should be created"""
        return self.get_settings().get('create_indexes', True)
    
    def should_drop_existing_tables(self) -> bool:
        """Check if existing tables should be dropped"""
        return self.get_settings().get('drop_existing_tables', True)
    
    def should_verify_transfers(self) -> bool:
        """Check if transfers should be verified"""
        return self.get_settings().get('verify_transfers', True)
    
    def validate_table_config(self, table_config: Dict[str, Any]) -> None:
        """Validate a single table configuration"""
        required_fields = ['source_table', 'target_table']
        for field in required_fields:
            if field not in table_config:
                raise ValueError(f"Missing required field '{field}' in table configuration")
        
        # Validate foreign keys configuration
        if 'foreign_keys' in table_config:
            for fk in table_config['foreign_keys']:
                fk_required = ['column', 'referenced_table', 'referenced_column']
                for field in fk_required:
                    if field not in fk:
                        raise ValueError(f"Missing required field '{field}' in foreign key configuration")
        
        # Validate indexes configuration
        if 'indexes' in table_config:
            for idx in table_config['indexes']:
                if 'columns' not in idx:
                    raise ValueError("Missing 'columns' field in index configuration")
                if not isinstance(idx['columns'], list):
                    raise ValueError("Index 'columns' must be a list")
    
    def validate_all_configurations(self) -> None:
        """Validate all table configurations"""
        tables = self.get_tables_config()
        for i, table_config in enumerate(tables):
            try:
                self.validate_table_config(table_config)
            except ValueError as e:
                raise ValueError(f"Invalid configuration for table {i}: {e}")
        
        logger.info(f"All {len(tables)} table configurations validated successfully")
    
    def reload_config(self, new_config_path: str = None) -> None:
        """Reload configuration from file"""
        if new_config_path:
            self.config_path = new_config_path
        self._load_config()
        self._load_credentials()
        logger.info("Configuration reloaded")