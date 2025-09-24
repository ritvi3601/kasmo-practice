# transform.py
import pandas as pd

def transform(data):
    df = pd.DataFrame(data)
    # Example: clean empty strings in body
    df["body"] = df["body"].replace("", pd.NA)
    return df
