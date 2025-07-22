# Windows Setup Guide for ETL Migration Tool

## SQL Server ODBC Driver Installation

The error "Data source name not found and no default driver specified" indicates that SQL Server ODBC drivers are not properly installed on your Windows system.

### Solution 1: Install Microsoft ODBC Driver for SQL Server

**Download and install the latest ODBC driver:**

1. **ODBC Driver 18 for SQL Server** (Recommended):
   - Download: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
   - File: `msodbcsql.msi` (x64 for 64-bit Windows)

2. **Alternative: ODBC Driver 17 for SQL Server**:
   - Download: https://www.microsoft.com/en-us/download/details.aspx?id=56567

### Solution 2: Verify Installed ODBC Drivers

**Check what drivers are available on your system:**

```cmd
# In Command Prompt or PowerShell:
python -c "import pyodbc; print('Available ODBC drivers:'); [print(f'  - {driver}') for driver in pyodbc.drivers()]"
```

### Solution 3: Use Windows ODBC Data Source Administrator

1. **Open ODBC Data Source Administrator**:
   - Press `Win + R`, type `odbcad32.exe`, press Enter
   - Or search "ODBC" in Start menu

2. **Check System DSN or User DSN tabs** for SQL Server entries

3. **Drivers tab** should show installed SQL Server drivers

### Solution 4: Alternative Connection Methods

If ODBC drivers can't be installed, modify your configuration:

**Option A: Use SQL Server Native Client (if available)**
```python
# The connection will automatically try these fallbacks:
# - SQL Server Native Client 11.0
# - SQL Server Native Client 10.0
# - SQL Server (basic driver)
```

**Option B: Use different connection parameters**
```yaml
# In your config file, try different host formats:
mssql:
  host: "localhost"          # instead of IP
  host: "127.0.0.1"         # instead of hostname
  host: "server\\instance"   # for named instances
```

### Solution 5: Troubleshooting Steps

1. **Check Windows Architecture**:
   ```cmd
   python -c "import platform; print(f'Python: {platform.architecture()}')"
   ```
   Ensure you install the matching ODBC driver (x64 vs x86).

2. **Test Connection Manually**:
   ```python
   import pyodbc
   
   # List available drivers
   print("Available drivers:", pyodbc.drivers())
   
   # Test connection string
   conn_str = "DRIVER={SQL Server};SERVER=your_server;DATABASE=your_db;UID=user;PWD=pass;"
   try:
       conn = pyodbc.connect(conn_str)
       print("✓ Connection successful")
       conn.close()
   except Exception as e:
       print(f"❌ Connection failed: {e}")
   ```

3. **Environment Variables**:
   ```cmd
   # Check if SQL Server client tools are in PATH
   echo %PATH%
   
   # Look for SQL Server installation
   dir "C:\Program Files\Microsoft SQL Server"
   ```

### Solution 6: Docker Alternative (If All Else Fails)

If ODBC drivers can't be installed on Windows, consider using Docker:

```dockerfile
# Use a Linux container with pre-installed drivers
FROM python:3.11-slim
RUN apt-get update && apt-get install -y \
    unixodbc-dev \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*
```

### Updated Driver Priority

The ETL tool now tries these drivers in order:
1. **ODBC Driver 18 for SQL Server** (latest, recommended)
2. **ODBC Driver 17 for SQL Server** 
3. **ODBC Driver 13 for SQL Server**
4. **ODBC Driver 11 for SQL Server**
5. **SQL Server Native Client 11.0**
6. **SQL Server Native Client 10.0**
7. **SQL Server** (basic fallback)
8. **SQLSRV** (alternative)
9. **FreeTDS** (open-source fallback)

### Quick Fix Commands

```cmd
# 1. Install via Chocolatey (if you have it)
choco install sqlserver-odbcdriver

# 2. Install via winget (Windows 10/11)
winget install Microsoft.ODBCDriverforSQLServer

# 3. Download and install manually
# Visit: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

After installing any ODBC driver, restart your command prompt/IDE and try running the ETL tool again.