from sqlalchemy import create_engine
import config
from src import extract, transform, load  # now this works

# Create SQLAlchemy engine once
engine = create_engine(
    f"mssql+pyodbc://@{config.SERVER}/{config.DATABASE}?driver=ODBC+Driver+17+for+SQL+Server"
)

# Extract CSV
df_product_inventory_csv = extract.extract_csv("product_inventory.csv")
print(df_product_inventory_csv.head())

# Load into SQL (this just writes, doesn’t return a DataFrame)
df_product_inventory_csv.to_sql(
    "product_inventory_data", engine, if_exists='replace', index=False
)

# Transform the CSV DataFrame (not the int)
df_product_transformed = transform.transform_product_inventory(df_product_inventory_csv)
print(df_product_transformed.head())

#Load into SQL Server
df_load=load.load_to_sql(df_product_transformed, "product_inventory_transformed", engine)
print(df_load)