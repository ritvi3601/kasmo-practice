from src.extract import extract
from src.transform import transform
from config import connector
from src.load import load

def main():
    conn = connector()   # call the function ✅
    cursor = conn.cursor()

    df1, df2, df3, df4, df5 = extract()
    df1, df2, df3, df4, df5 = transform(df1, df2, df3, df4, df5)
    load(conn, cursor, df1, df2, df3, df4, df5)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
