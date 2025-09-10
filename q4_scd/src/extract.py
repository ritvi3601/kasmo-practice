import pandas as pd

def extract_csv(path: str):
    return pd.read_csv(path)

def extract_excel(path: str, sheet_name=0):
    return pd.read_excel(path, sheet_name=sheet_name)


def extract_sql(query, engine):
    """Load SQL query into DataFrame"""
    return pd.read_sql(query, engine)
