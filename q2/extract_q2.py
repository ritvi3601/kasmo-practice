import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import config
# SQLAlchemy engine
engine = create_engine(
    f"mssql+pyodbc://@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Load CSV files into DataFrames
df_order = pd.read_csv("order_data.csv")
df_customer = pd.read_csv("us_customer_data.csv")
df_customer = pd.read_csv("us_customer_data_clean.csv")
df_transaction = pd.read_csv("transaction_data.csv")

# Load each DataFrame into SQL Server
df_order.to_sql("order_data", engine, if_exists='replace', index=False)
df_customer.to_sql("us_customer_data", engine, if_exists='replace', index=False)
df_transaction.to_sql("transaction_data", engine, if_exists='replace', index=False)

print(" All CSV files loaded successfully!")
