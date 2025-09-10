import pandas as pd

def extract_csv(file_path):
    """Load CSV into DataFrame"""
    return pd.read_csv(file_path)

def extract_sql(query, engine):
    """Load SQL query into DataFrame"""
    return pd.read_sql(query, engine)
