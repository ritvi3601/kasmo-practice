import pandas as pd
from sqlalchemy import create_engine
import config  # your config.py

# Create SQLAlchemy engine
engine = create_engine(
    f"mssql+pyodbc://@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)
# Extract Customers
query_customers = "SELECT * FROM us_customer_data"
df_customers = pd.read_sql(query_customers, engine)
print(" Customers extracted:", df_customers.shape)

# Extract Orders
query_orders = "SELECT * FROM order_data"
df_orders = pd.read_sql(query_orders, engine)
print("Orders extracted:", df_orders.shape)

# Split 'Name' into 'FirstName' and 'LastName'
df_customers[['FirstName', 'LastName']] = df_customers['name'].str.split(pat=' ', n=1, expand=True)

# Optional: drop original 'Name' column
df_customers = df_customers.drop(columns=['name'])

tier_mapping = {
    'Gold': 2,
    'Silver': 1,
    'Bronze': 0
}

# Create new column based on mapping
df_customers['Customer_Tier'] = df_customers['loyalty_status'].map(tier_mapping)

print(df_customers)
# Define the desired column order
column_order = [
    'customer_id',
    'FirstName',
    'LastName',
    'email',
    'phone',
    'address',
    'registration_date',
    'loyalty_status',
    'Customer_Tier'
]

# Reorder DataFrame
df_customers_transformed = df_customers[column_order]

# Verify
print(df_customers_transformed.head())


# Load transformed data into SQL Server
df_customers_transformed.to_sql(
    name="us_customer_data_transformed",  # new table name in SQL Server
    con=engine,
    if_exists='replace',  # options: 'fail', 'replace', 'append'
    index=False  # do not write DataFrame index as a column
)

print(" Transformed data loaded back into SQL Server successfully!")

