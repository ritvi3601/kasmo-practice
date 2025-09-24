# load.py
from sqlalchemy import create_engine
from config import SERVER, DATABASE, DRIVER, TABLE_NAME

def load_to_mssql(df):
    engine = create_engine(
        f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes"
    )
    df.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
    print(f"✅ {len(df)} emails loaded to table {TABLE_NAME}")
