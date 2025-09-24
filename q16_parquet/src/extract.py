import io
import pandas as pd

def extract(bucket):
    files = [ "house-price.parquet", "weather.parquet"]

    # Upload files
    for file_name in files:
        with open(file_name, 'rb') as f:
            bucket.put_object(Key=file_name, Body=f)

    # Read files back from S3
    df_list = []
    for file_name in files:
        obj = bucket.Object(file_name).get()
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        df_list.append(df)

    return df_list[0], df_list[1]
