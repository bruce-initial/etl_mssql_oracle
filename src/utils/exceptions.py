"""Custom exceptions for ETL operations"""


class ETLError(Exception):
    """Base exception for ETL operations"""
    pass


class ConnectionError(ETLError):
    """Raised when database connection fails"""
    pass


class ConfigurationError(ETLError):
    """Raised when configuration is invalid"""
    pass


class DataTypeError(ETLError):
    """Raised when data type mapping fails"""
    pass


class TransferError(ETLError):
    """Raised when data transfer fails"""
    pass


class ValidationError(ETLError):
    """Raised when data validation fails"""
    pass


class DependencyError(ETLError):
    """Raised when dependency resolution fails"""
    pass


class SQLError(ETLError):
    """Raised when SQL execution fails"""
    pass


class SchemaError(ETLError):
    """Raised when schema operations fail"""
    pass