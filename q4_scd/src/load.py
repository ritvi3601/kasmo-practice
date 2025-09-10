def load_to_sql(df, table_name, engine, if_exists='replace'):

    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f" Loaded table {table_name} successfully!")