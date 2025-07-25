# ETL Migration Tool: SQL Server to Oracle

Modular ETL system for transferring data from SQL Server to Oracle with automatic dependency resolution and intelligent data mapping.

## Quick Start

```bash
# Install dependencies
uv sync

# Validate configuration
uv run main_refactored.py --dry-run

# Run transfer
uv run main_refactored.py
```

## Architecture

**Technology Stack:**
- `pymssql` - SQL Server connectivity
- `polars` - High-performance data processing  
- `python-oracledb` - Modern Oracle client
- `duckdb` - Complex data analysis

**Modular Design:**
```
src/
├── connections/     # Database abstractions (MSSQL, Oracle)
├── transfer/        # Main ETL engine
├── config/          # YAML configuration management
├── mappers/         # Data type conversion
├── dependency/      # Topological sorting
└── utils/           # Logging and exceptions
```

## Key Features

- **Automatic Dependency Resolution** - Tables transferred in correct order
- **Intelligent Type Mapping** - SQL Server to Oracle conversion
- **Batch Processing** - Configurable sizes with progress tracking
- **Transaction Safety** - Rollback on failure
- **Custom Exception Hierarchy** - Detailed error handling
- **CLI Interface** - Dry-run validation, debug logging
- **Performance Monitoring** - Timing and throughput metrics

## Configuration

**tables_config.yaml:**
```yaml
tables:
  - source_table: "Orders"
    target_table: "ORDERS"
    source_schema: "dbo"
    batch_size: 1000
    primary_key: "ID"
    foreign_keys:
      - column: "CUSTOMER_ID"
        referenced_table: "CUSTOMERS"
        referenced_column: "ID"

settings:
  create_foreign_keys: true
  create_indexes: true
  verify_transfers: true
```

**Environment (.env):**
```
MSSQL_SERVER=localhost
MSSQL_DATABASE=source_db
ORACLE_HOST=localhost
ORACLE_DATABASE=target_db
```

## Usage

```bash
# Basic commands
uv run main_refactored.py                    # Default config
uv run main_refactored.py --config custom.yaml  # Custom config
uv run main_refactored.py --log-level DEBUG     # Debug mode

# Testing
uv run test_refactored_code.py           # Test all modules
uv run test_oracle_connection.py         # Test Oracle connectivity

# Legacy support
uv run main.py                           # Original monolithic version
```

## Data Type Mapping

| SQL Server | Oracle | Notes |
|------------|--------|-------|
| VARCHAR(n) | VARCHAR2(n) | Auto-conversion |
| INTEGER | NUMBER | Precision preserved |
| DATETIME | TIMESTAMP | Full datetime support |
| TEXT | CLOB | Large text data |
| BIT | NUMBER(1) | 0/1 values |

## Prerequisites

**Database Drivers:**
- SQL Server: Microsoft ODBC Driver 17/18
- Oracle: Instant Client libraries with LD_LIBRARY_PATH

**Python Requirements:**
- Python 3.10+
- UV package manager (recommended) or pip

## Development

```bash
# Setup development environment
git clone <repository>
cd etl_mssql_oracle
uv sync

# Run tests
uv run test_refactored_code.py

# Validate without DB connections
uv run main_refactored.py --dry-run
```

## Troubleshooting

**Oracle Bind Errors (Fixed):** ORA-01036 resolved in v2.0 with proper bind syntax
**Connection Timeouts:** Use `--dry-run` for configuration validation
**Missing Drivers:** Install ODBC drivers per platform requirements
**Circular Dependencies:** Automatic detection and resolution