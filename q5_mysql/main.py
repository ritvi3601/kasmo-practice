import pandas as pd
from sqlalchemy import create_engine
import config  # Your config.py containing DB credentials
from src import extract 
from src import transform
from src import load

# Create SQLAlchemy engine using credentials from config.py
engine = create_engine(
    f"mysql+mysqlconnector://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DB_DATABASE}"
)

# Extract CSV
df_orders_csv = extract.extract_csv("orders.csv")

df_orders_csv.to_sql(
    "orders", engine, if_exists='replace', index=False
)

df_customers_csv = extract.extract_csv("customers.csv")

df_customers_csv.to_sql(
    "customers", engine, if_exists='replace', index=False
)
df_order_items_csv = extract.extract_csv("order_items.csv")

df_order_items_csv.to_sql(
    "order_items", engine, if_exists='replace', index=False
)
df_products_csv = extract.extract_csv("products.csv")

df_products_csv.to_sql(
    "products", engine, if_exists='replace', index=False
)

df_transformed1,df_transformed2,df_transformed3,df_transformed4 = transform.transform_purchase(df_products_csv, df_customers_csv,df_orders_csv,df_order_items_csv)
print(df_transformed1.head())
print(df_transformed2.head())
print(df_transformed3.head())
print(df_transformed4.head())

df_load=load.load_to_sql(df_transformed1, "merged_df", engine)
print(df_load)
df_load1=load.load_to_sql(df_transformed2, "order_summary", engine)
print(df_load)
df_load2=load.load_to_sql(df_transformed3, "region_month_summary", engine)
print(df_load)
df_load3=load.load_to_sql(df_transformed4, "category_summary", engine)
print(df_load)


