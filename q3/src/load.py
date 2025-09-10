def load_to_sql(df, table_name, engine, if_exists='replace'):
    """
    Load a DataFrame into SQL Server using SQLAlchemy engine
    """
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f" Loaded table {table_name} successfully!")



