from sqlalchemy import create_engine
import config
from src import extract
from src import transform_scd1
from src import transform_scd2, transform_scd3, transform_scd5
from src import load  

# Create SQLAlchemy engine once
engine = create_engine(
    f"mssql+pyodbc://@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Extract CSV
df_customer_master_csv = extract.extract_csv("Customer_Master.csv")
print(df_customer_master_csv.head())

df_customer_updates_csv = extract.extract_csv("Customer_Updates.csv")
print(df_customer_updates_csv.head())
# Load into SQL (this just writes, doesn’t return a DataFrame)
df_customer_master_csv.to_sql(
    "customer_master", engine, if_exists='replace', index=False
)
df_customer_updates_csv.to_sql(
    "customer_updates", engine, if_exists='replace', index=False
)


# Call function with 2 DataFrames
df_customer_master_scd1 = transform_scd1.transform_scd_1(df_customer_master_csv, df_customer_updates_csv)
print(df_customer_master_scd1.head())

df_customer_master_scd2 = transform_scd2.transform_scd_2(df_customer_master_csv, df_customer_updates_csv)
print(df_customer_master_scd2.head())

df_customer_master_scd3 = transform_scd3.transform_scd_3(df_customer_master_csv, df_customer_updates_csv)
print(df_customer_master_scd3.head())

df_customer_master_scd5 = transform_scd5.transform_scd_5(df_customer_master_csv, df_customer_updates_csv)
print(df_customer_master_scd5)

#Load into SQL Server
df_load=load.load_to_sql(df_customer_master_scd1, "customer_master_scd1", engine)
print(df_load)
df_load2=load.load_to_sql(df_customer_master_scd2, "customer_master_scd2", engine)
print(df_load2)
df_load3=load.load_to_sql(df_customer_master_scd3, "customer_master_scd3", engine)
print(df_load3)
df_load5=load.load_to_sql(df_customer_master_scd5, "customer_master_scd5", engine)
print(df_load5)