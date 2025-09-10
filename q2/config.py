import pyodbc as odbc

# Define connection parameters
DRIVER = "{ODBC Driver 17 for SQL Server}"
SERVER = "RITVI-SHAH"        # your server name
DATABASE = "ritvi_python"    # your database name
TRUSTED_CONNECTION = "yes"

# Build connection string
conn_str = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection={TRUSTED_CONNECTION};"

# Connect to SQL Server
conn = odbc.connect(conn_str)
cursor = conn.cursor()
print(" Connection successful")
