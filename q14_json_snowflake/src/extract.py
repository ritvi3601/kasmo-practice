import pandas as pd

def extract():

    df1=pd.read_json('product_dimension.json')
    df2=pd.read_json('sales_dimensions.json')
    df3=pd.read_json('sales_fact.json')
    df4=pd.read_json('store_dimension.json')
    df5=pd.read_json('time_dimension.json')
    
    return df1,df2,df3,df4,df5