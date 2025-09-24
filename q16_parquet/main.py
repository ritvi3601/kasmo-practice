from config import bucket
from src import extract, transform, load
from config import server_conn

mssql = (
    f"mssql+pyodbc://@{server_conn['server']}/{server_conn['database']}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

df1, df2 = extract.extract(bucket)
print(df1.columns)
print(df2.columns)
df1, df2 = transform.transform(df1, df2)
load.load(df1, df2, mssql)
