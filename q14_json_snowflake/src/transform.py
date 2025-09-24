import pandas as pd
def transform(prod_dim,sales_dim,sales_fact,store_dim,time_dim):
    prod_dim['description']=prod_dim['description'].str.strip()
    sales_dim['start_date']=pd.to_datetime(sales_dim['start_date'],errors='coerce').dt.date
    sales_dim['end_date']=pd.to_datetime(sales_dim['start_date'],errors='coerce').dt.date
    sales_fact['discount_applied']=sales_fact['discount_applied'].round(2)
    sales_fact['net_amount']=sales_fact['net_amount'].round(2)
    sales_fact['discount_applied']=sales_fact['discount_applied'].round(2)
    sales_fact['net_amount']=sales_fact['net_amount'].round(2)
    store_dim['opening_date']=pd.to_datetime(store_dim['opening_date'],errors='coerce').dt.date
    time_dim['date']=pd.to_datetime(time_dim['date'],errors='coerce').dt.date

    

    return prod_dim,sales_dim,sales_fact,store_dim,time_dim