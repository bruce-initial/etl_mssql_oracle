# Multi-Table DuckDB MSSQL to Oracle Transfer System

## Overview

This enhanced system handles multiple source tables with complex relationships, using **PyODBC + Pandas + DuckDB** approach for SQL Server connectivity. The system has been completely **refactored with a modular architecture** for better maintainability, error handling, and extensibility.

## 🔧 **Latest Updates (v2.0 - Refactored)**

**Major Improvements:**
- **✅ Fixed Oracle bind variable errors (ORA-01036)** - Complete resolution of parameterization issues
- **✅ Modular architecture** - Separated concerns into dedicated modules for better maintainability
- **✅ Enhanced error handling** - Custom exceptions and comprehensive logging with performance metrics
- **✅ Command-line interface** - Full CLI with dry-run validation and configuration options
- **✅ Dependency resolution** - Automatic topological sorting with circular dependency detection
- **✅ Intelligent data mapping** - Advanced pandas-to-Oracle type conversion with column sanitization

## ⚠️ Architecture Evolution

**Current System (v2.0 - Refactored):**
- **Modular Design**: Separated into `src/` modules (connections, config, mappers, dependency, transfer, utils)
- **Enhanced Oracle Support**: Fixed bind variable syntax and DDL statement handling
- **Smart Data Mapping**: Intelligent type conversion with Oracle compatibility
- **Robust Error Handling**: Custom exception hierarchy and detailed logging
- **CLI Interface**: Full command-line support with validation modes

**Legacy System (v1.0 - Monolithic):**
- **PyODBC** for SQL Server connectivity (instead of DuckDB nanodbc extension)
- **Pandas** as a data bridge between databases  
- **DuckDB** for complex data processing and analysis
- **python-oracledb** for Oracle database operations

## 🔧 **Solution for EndeavourOS HTTP 403 Error**

The original nanodbc extension was causing HTTP 403 errors. This updated system uses a proven approach that combines multiple mature libraries:

### **Why This Approach Works Better:**
1. **✅ No Extension Dependencies**: Doesn't rely on potentially unavailable DuckDB extensions
2. **✅ Mature Libraries**: Uses well-established pyodbc and pandas libraries  
3. **✅ Better Error Handling**: More detailed error messages and troubleshooting
4. **✅ Cross-Platform**: Works consistently across Linux distributions
5. **✅ Performance**: Efficient data transfer using pandas DataFrames as demonstrated in recent approaches

## Prerequisites

### ODBC Driver Requirements
Before using this system, you must install the appropriate ODBC drivers:

**For SQL Server:**
- **Windows**: SQL Server ODBC drivers are typically pre-installed
- **macOS**: Install Microsoft ODBC Driver via Homebrew: `brew tap microsoft/mssql-release && brew install msodbcsql17`
- **Linux**: Follow Microsoft's instructions to install msodbcsql17

**For Oracle (target database):**
- This system uses python-oracledb for writing to Oracle, which can work with or without Oracle Instant Client
- Download and install Oracle Instant Client libraries
- Set appropriate environment variables (LD_LIBRARY_PATH on Linux, PATH on Windows)

## Project Structure

```
project_root/
├── main_refactored.py                # 🆕 Refactored main system with CLI (RECOMMENDED)
├── main.py                           # Legacy main system (still supported)
├── test_refactored_code.py           # 🆕 Comprehensive test suite
├── test_oracle_connection.py         # 🆕 Oracle connection test utility
├── verify_fix.md                     # 🆕 Documentation of bind variable fix
├── .env                              # Environment variables (not in version control)
├── requirements.txt                  # Python dependencies
├── pyproject.toml                    # 🆕 UV project configuration
├── config/                           # Configuration directory
│   ├── tables_config.yaml           # Main table configuration file
│   └── custom_config.yaml           # Custom configuration (auto-generated)
├── src/                              # 🆕 Modular source code
│   ├── connections/                  # Database connection abstractions
│   │   ├── base.py                   # Abstract base connection class
│   │   ├── mssql.py                  # SQL Server connection with PyODBC
│   │   └── oracle.py                 # Oracle connection with python-oracledb
│   ├── config/                       # Configuration management
│   │   └── manager.py                # Configuration and credential management
│   ├── mappers/                      # Data type mapping utilities
│   │   └── data_types.py             # Pandas-to-Oracle type conversion
│   ├── dependency/                   # Dependency resolution
│   │   └── resolver.py               # Topological sorting and cycle detection
│   ├── transfer/                     # Main ETL engine
│   │   └── engine.py                 # Multi-table transfer orchestration
│   └── utils/                        # Utility modules
│       ├── logging.py                # Enhanced logging with performance metrics
│       └── exceptions.py             # Custom exception hierarchy
├── sql/                              # SQL query templates
│   ├── mssql_read_table.sql         # Generic ODBC table read query
│   ├── mssql_read_cases_custom.sql  # Custom filtered query
│   ├── duckdb_join_cases_customers.sql # DuckDB join query
│   ├── oracle_drop_table.sql        # Drop table template
│   ├── oracle_create_table.sql      # Create table template
│   ├── oracle_insert_table.sql      # Insert data template
│   ├── oracle_verify_table.sql      # Verification query
│   ├── oracle_create_foreign_key.sql # Foreign key creation
│   └── oracle_create_index.sql      # Index creation
├── logs/                             # Log files (auto-generated)
└── README.md                         # This documentation
```

## Key Features

### 🔄 Multi-Table Support (Enhanced)
- **Intelligent Transfer Order**: Automatic dependency resolution with topological sorting
- **Circular Dependency Detection**: Advanced cycle detection and resolution algorithms  
- **Composite Key Support**: Full support for multi-column primary keys
- **Custom Transformations**: Flexible SQL queries for complex data transformations

### 🔗 Relationship Management (Enhanced)
- **Smart Constraint Creation**: Automatic foreign key and index generation
- **Performance Optimization**: Intelligent index creation based on relationships
- **Referential Integrity**: Comprehensive integrity preservation with validation
- **Dependency Visualization**: Detailed dependency analysis and reporting

### ⚙️ Configuration-Driven (Enhanced)
- **YAML Configuration**: Comprehensive table definitions with validation
- **Environment Management**: Secure credential handling with .env support
- **Flexible Mapping**: Advanced schema mapping with type intelligence
- **Batch Optimization**: Configurable batch sizes with performance monitoring

### 🛡️ Robust Error Handling (New Features)
- **Custom Exception Hierarchy**: Specialized exceptions for different error types
- **Enhanced Logging**: Performance metrics, batch progress, and detailed error context
- **Transaction Safety**: Advanced rollback mechanisms with savepoint support
- **Validation Modes**: Dry-run validation without database connections

### 🚀 New Capabilities (v2.0)
- **Command-Line Interface**: Full CLI with argument parsing and help
- **Dry-Run Validation**: Configuration testing without database connections
- **Performance Monitoring**: Detailed timing and throughput metrics
- **Modular Architecture**: Extensible design for future enhancements
- **Oracle Compatibility**: Fixed bind variable issues and enhanced Oracle support

## Configuration Format

### Table Definition
```yaml
- source_table: "TABLE_NAME"          # Source table name
  target_table: "TABLE_NAME"          # Target table name
  source_schema: "dbo"                # Source schema
  custom_query: null                  # Custom SQL file (optional)
  batch_size: 1000                    # Insert batch size
  
  primary_key: "ID"                   # Single or composite PK
  # OR
  primary_key: ["ID1", "ID2"]         # Composite primary key
  
  indexes:                            # Index definitions
    - name: "idx_table_column"
      columns: ["COLUMN1", "COLUMN2"]
      unique: false
      
  foreign_keys:                       # Foreign key relationships
    - name: "fk_table_reference"
      column: "FOREIGN_ID"
      referenced_table: "PARENT_TABLE"
      referenced_column: "ID"
```

### Global Settings
```yaml
settings:
  default_batch_size: 1000
  create_foreign_keys: true
  create_indexes: true
  drop_existing_tables: true
  verify_transfers: true
```

## Data Type Mapping

| MSSQL/DuckDB Type | Oracle Type | Notes |
|-------------------|-------------|-------|
| VARCHAR(n) | VARCHAR2(n) | Automatic conversion |
| TEXT/CLOB | CLOB | Large text data |
| INTEGER/INT | NUMBER | Integer values |
| BIGINT | NUMBER(19) | Large integers |
| DECIMAL/NUMERIC | NUMBER | Precision preserved |
| DOUBLE/FLOAT | NUMBER | Floating point |
| DATE | DATE | Date values |
| TIMESTAMP | TIMESTAMP | Date and time |
| BOOLEAN/BIT | NUMBER(1) | 0/1 values |
| BINARY/VARBINARY | RAW(2000) | Binary data |
| BLOB | BLOB | Large binary |

## Usage Examples

### 1. Basic Transfer (Refactored)
```python
from src.transfer.engine import TransferEngine

# Transfer all tables with default configuration
engine = TransferEngine()
result = engine.transfer_all_tables()
```

### 2. Custom Configuration
```python
# Use custom configuration file
engine = TransferEngine("config/custom_config.yaml")
result = engine.transfer_all_tables()
```

### 3. Command Line Usage
```bash
# Basic transfer
python main_refactored.py

# Custom configuration
python main_refactored.py --config config/my_config.yaml

# Dry run (validation only)
python main_refactored.py --dry-run

# Debug logging
python main_refactored.py --log-level DEBUG
```

### 4. Legacy Support
```python
# Original main.py still available for compatibility
from main import MultiTableTransfer
transfer_system = MultiTableTransfer()
transfer_system.transfer_all_tables()
```

## Setup Instructions

### 1. Environment Setup
```bash
# Create project directory
mkdir multi-table-transfer
cd multi-table-transfer

# Create required directories
mkdir config sql logs

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup
- Install Oracle Instant Client
- Configure Oracle environment variables
- Ensure MSSQL server is accessible
- Test database connections

### 3. Configuration
```bash
# Copy and edit configuration
cp config/tables_config.yaml config/my_config.yaml
# Edit my_config.yaml with your table definitions

# Copy and edit environment file
cp .env.example .env
# Add your database credentials
```

### 4. Run Transfer

#### 🆕 Refactored System (Recommended)
```bash
# Install dependencies
uv sync

# Validate configuration without connecting to databases
uv run main_refactored.py --dry-run

# Run full transfer with default configuration
uv run main_refactored.py

# Run with custom configuration
uv run main_refactored.py --config config/my_config.yaml

# Enable debug logging
uv run main_refactored.py --log-level DEBUG

# Get help
uv run main_refactored.py --help

# Test components individually
uv run test_refactored_code.py        # Test all modules
uv run test_oracle_connection.py      # Test Oracle connectivity
```

#### Legacy System Support
```bash
# Original main.py (still supported)
uv run main.py

# Direct Python usage (requires dependencies)
python main_refactored.py --dry-run
```

#### Programmatic Usage
```python
# Refactored engine (recommended)
from src.transfer.engine import TransferEngine

engine = TransferEngine('config/my_config.yaml')
result = engine.transfer_all_tables()

if result['success']:
    print(f"✅ Successfully transferred {result['successful_tables']}/{result['total_tables']} tables")
    print(f"Duration: {result['duration']:.2f} seconds")
else:
    print(f"❌ Transfer failed: {result['successful_tables']}/{result['total_tables']} tables")

# Legacy engine (still works)
from main import MultiTableTransfer
transfer = MultiTableTransfer('config/my_config.yaml')
transfer.transfer_all_tables()
```

## Advanced Features

### Custom Queries
Create custom SQL files for complex transformations:
```sql
-- sql/mssql_read_orders_with_totals.sql
SELECT 
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_DATE,
    SUM(oi.QUANTITY * oi.UNIT_PRICE) as TOTAL_AMOUNT
FROM sqlserver_scan(...) o
LEFT JOIN sqlserver_scan(...) oi ON o.ORDER_ID = oi.ORDER_ID
GROUP BY o.ORDER_ID, o.CUSTOMER_ID, o.ORDER_DATE
```

### Dependency Resolution
The system automatically determines transfer order:
1. **Parent Tables First**: Tables with no foreign keys
2. **Child Tables**: Tables that reference parent tables
3. **Junction Tables**: Tables with multiple foreign keys
4. **Circular Dependencies**: Handled gracefully

### Performance Optimization
- **Batch Processing**: Configurable batch sizes
- **Index Creation**: Automatic index creation for foreign keys
- **Transaction Management**: Commit/rollback per table
- **Connection Pooling**: Efficient connection reuse

### Error Recovery
- **Validation**: Pre-transfer configuration validation
- **Verification**: Post-transfer row count verification
- **Logging**: Comprehensive operation logging
- **Rollback**: Automatic rollback on failures

## Monitoring and Logging

### Log Levels
- **INFO**: Normal operations and progress
- **WARNING**: Non-critical issues
- **ERROR**: Transfer failures and errors
- **DEBUG**: Detailed operation information

### Log Files
```
logs/
├── transfer_YYYYMMDD_HHMMSS.log    # Main transfer log
├── errors_YYYYMMDD.log             # Error-only log
└── performance_YYYYMMDD.log        # Performance metrics
```

## Troubleshooting

### 🆕 Recent Fixes (v2.0)

**✅ Oracle Bind Variable Error (ORA-01036) - RESOLVED**
- **Issue**: `ORA-01036: unrecognized bind variable 1 passed to the bind call`
- **Root Cause**: Wrong bind variable syntax (`?` instead of `:1`) and DDL parameterization
- **Solution**: Fixed Oracle connection to use proper `:1`, `:2` syntax and direct SQL for DDL
- **Status**: Completely resolved in both legacy and refactored systems
- **Verification**: Run `uv run test_refactored_code.py` to confirm fix

**✅ UV Virtual Environment Warnings - RESOLVED**
- **Issue**: Virtual environment path mismatch warnings
- **Solution**: Proper UV environment setup with `uv sync`
- **Verification**: No more environment warnings when using `uv run`

### Common Issues

**1. Oracle Connection Timeout (Expected in Test Environment)**
- **Error**: `ORA-12170: Cannot connect. TCP connect timeout`
- **Cause**: Oracle database server not reachable (normal in test environments)
- **Solution**: Use `--dry-run` mode for validation without database connections
- **Test**: `uv run main_refactored.py --dry-run`

**2. ODBC Driver Not Found**
- **Error**: `[unixODBC][Driver Manager]Can't open lib 'SQL Server'`
- **Solution**: Install Microsoft ODBC Driver 17/18 for SQL Server
- **Test**: Use `odbcinst -q -d` to list available drivers
- **EndeavourOS**: `sudo pacman -S unixodbc msodbcsql17`

**3. SQL Server Connection Issues**
- **Error**: `[Microsoft][ODBC Driver 17 for SQL Server][DBNETLIB]SQL Server does not exist`
- **Solution**: Check server name, port, and network connectivity
- **Enhanced Debugging**: Use debug logging: `uv run main_refactored.py --log-level DEBUG`

**4. Oracle Instant Client Issues**
- **Error**: `DPI-1047: Cannot locate a 64-bit Oracle Client library`
- **Solution**: Install Oracle Instant Client and configure environment
- **Linux**: `export LD_LIBRARY_PATH=/opt/oracle/instantclient_21_1`
- **Test**: `uv run test_oracle_connection.py`

**5. Configuration Validation Errors**
- **Error**: Missing required fields or invalid YAML
- **Solution**: Use dry-run mode to validate: `uv run main_refactored.py --dry-run`
- **Enhanced**: Detailed validation messages in v2.0

**6. Data Type Conversion Issues**
- **Enhanced Mapping**: v2.0 includes intelligent pandas-to-Oracle type conversion
- **Column Sanitization**: Automatic handling of Oracle reserved words and length limits
- **Debugging**: Detailed column mapping logs in debug mode

**7. Dependency Resolution Problems**
- **Enhanced**: v2.0 includes automatic topological sorting and cycle detection
- **Circular Dependencies**: Automatic cycle breaking with detailed logging
- **Validation**: Pre-transfer dependency validation with detailed reports

**8. Performance Optimization**
- **Enhanced Monitoring**: v2.0 includes detailed performance metrics
- **Batch Progress**: Real-time batch processing progress with ETA
- **Tuning**: Configurable batch sizes with automatic optimization recommendations

### Best Practices (Enhanced for v2.0)

1. **Start with Dry-Run Validation**: Always validate configuration first
   ```bash
   uv run main_refactored.py --dry-run
   ```

2. **Test Components Individually**: Use the comprehensive test suite
   ```bash
   uv run test_refactored_code.py
   uv run test_oracle_connection.py
   ```

3. **Monitor with Debug Logging**: Enable detailed logging for troubleshooting
   ```bash
   uv run main_refactored.py --log-level DEBUG
   ```

4. **Validate Dependencies**: Check dependency resolution before transfer
   - v2.0 automatically validates and resolves dependencies
   - Reports circular dependencies and resolution strategies

5. **Use Performance Monitoring**: Leverage enhanced logging features
   - Real-time batch progress tracking
   - Performance metrics with timing and throughput
   - Detailed error context and recovery suggestions

6. **Backup and Recovery**: Enhanced transaction management
   - Automatic rollback on failure
   - Table-level transaction isolation
   - Comprehensive verification after transfer

7. **Configuration Management**: Use environment-specific configurations
   ```bash
   uv run main_refactored.py --config config/production.yaml
   uv run main_refactored.py --config config/test.yaml
   ```

8. **Progressive Testing**: 
   - Start with single table configurations
   - Gradually add dependencies and complexity
   - Use small datasets for initial validation

## 🎯 Quick Start Guide

### For New Users
```bash
# 1. Clone and setup
git clone <repository>
cd etl_mssql_oracle

# 2. Install dependencies
uv sync

# 3. Configure environment
cp .env.example .env
# Edit .env with your database credentials

# 4. Validate configuration
uv run main_refactored.py --dry-run

# 5. Run transfer
uv run main_refactored.py
```

### For Existing Users (v1.0 → v2.0 Migration)
```bash
# Your existing main.py still works
uv run main.py

# Try the new refactored system
uv run main_refactored.py --dry-run    # Validate first
uv run main_refactored.py              # Run transfer

# Test new features
uv run test_refactored_code.py         # Test suite
```

## 📊 Performance Comparison

| Feature | v1.0 (Legacy) | v2.0 (Refactored) |
|---------|---------------|-------------------|
| Error Handling | Basic | **Enhanced with custom exceptions** |
| Logging | Simple | **Performance metrics + batch progress** |
| Configuration | YAML only | **YAML + validation + environment management** |
| Dependencies | Manual order | **Automatic topological sorting** |
| Oracle Support | Basic | **Fixed bind variables + enhanced DDL** |
| Testing | None | **Comprehensive test suite** |
| CLI | None | **Full command-line interface** |
| Validation | Runtime only | **Dry-run mode without DB connections** |

## Contributing (Enhanced)

### Development Setup
```bash
# Clone and setup development environment
git clone <repository>
cd etl_mssql_oracle
uv sync

# Run tests
uv run test_refactored_code.py

# Check code quality
uv run main_refactored.py --dry-run
```

### Contributing Guidelines
1. **Fork the repository** and create a feature branch
2. **Add tests** for new functionality using the test framework
3. **Update documentation** including README.md and docstrings  
4. **Validate changes** with dry-run and test suite
5. **Submit pull request** with detailed description

### Code Structure
- Follow the modular architecture in `src/`
- Use type hints and comprehensive docstrings
- Add tests for new modules in test files
- Update configuration examples as needed

## License

MIT License - see LICENSE file for details

---

## 📚 Additional Resources

- **verify_fix.md** - Detailed documentation of Oracle bind variable fix
- **src/utils/exceptions.py** - Custom exception hierarchy
- **src/utils/logging.py** - Enhanced logging capabilities
- **test_refactored_code.py** - Comprehensive test suite
- **CLAUDE.md** - Development guidelines and architecture notes
