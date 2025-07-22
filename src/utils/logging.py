import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional


class ETLLogger:
    """Enhanced logging configuration for ETL operations"""
    
    def __init__(self, 
                 name: str = "etl_migration",
                 log_level: str = "INFO",
                 logs_dir: str = "logs",
                 enable_console: bool = True,
                 enable_file: bool = True):
        
        self.name = name
        self.log_level = getattr(logging, log_level.upper())
        self.logs_dir = Path(logs_dir)
        self.enable_console = enable_console
        self.enable_file = enable_file
        
        # Create logs directory if it doesn't exist
        self.logs_dir.mkdir(exist_ok=True)
        
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup and configure logger"""
        logger = logging.getLogger(self.name)
        logger.setLevel(self.log_level)
        
        # Clear existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # Console handler
        if self.enable_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        if self.enable_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = self.logs_dir / f"etl_migration_{timestamp}.log"
            
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # Error-only file handler
            error_log_file = self.logs_dir / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
            error_handler = logging.FileHandler(error_log_file)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            logger.addHandler(error_handler)
        
        return logger
    
    def get_logger(self) -> logging.Logger:
        """Get the configured logger"""
        return self.logger
    
    def log_table_start(self, table_name: str, operation: str = "transfer") -> None:
        """Log the start of a table operation"""
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Starting {operation} for table: {table_name}")
        self.logger.info(f"{'='*60}")
    
    def log_table_complete(self, table_name: str, operation: str = "transfer", 
                          row_count: Optional[int] = None) -> None:
        """Log the completion of a table operation"""
        message = f"Completed {operation} for table: {table_name}"
        if row_count is not None:
            message += f" ({row_count} rows)"
        self.logger.info(message)
        self.logger.info(f"{'='*60}")
    
    def log_batch_progress(self, batch_num: int, total_batches: int, 
                          batch_size: int, table_name: str) -> None:
        """Log batch processing progress"""
        percentage = (batch_num / total_batches) * 100
        self.logger.info(
            f"Processed batch {batch_num}/{total_batches} ({percentage:.1f}%) "
            f"for {table_name} - {batch_size} rows"
        )
    
    def log_database_operation(self, operation: str, details: str = "") -> None:
        """Log database operations with consistent formatting"""
        self.logger.info(f"Database operation: {operation}")
        if details:
            self.logger.debug(f"Details: {details}")
    
    def log_error_with_context(self, error: Exception, context: dict) -> None:
        """Log error with additional context information"""
        self.logger.error(f"Error occurred: {str(error)}")
        self.logger.error(f"Error type: {type(error).__name__}")
        
        for key, value in context.items():
            self.logger.error(f"Context - {key}: {value}")
    
    def log_performance_metric(self, operation: str, duration: float, 
                             additional_metrics: dict = None) -> None:
        """Log performance metrics"""
        self.logger.info(f"Performance - {operation}: {duration:.2f} seconds")
        
        if additional_metrics:
            for metric, value in additional_metrics.items():
                self.logger.info(f"Performance - {metric}: {value}")


def setup_logging(log_level: str = "INFO", 
                 logs_dir: str = "logs",
                 enable_console: bool = True,
                 enable_file: bool = True) -> logging.Logger:
    """Convenience function to setup logging"""
    etl_logger = ETLLogger(
        log_level=log_level,
        logs_dir=logs_dir,
        enable_console=enable_console,
        enable_file=enable_file
    )
    return etl_logger.get_logger()