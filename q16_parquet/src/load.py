def load(df1,df2,engine):
    df1.to_sql(name='House_price',con=engine,if_exists='replace',index=False)
    df2.to_sql(name='Weather_data',con=engine,if_exists='replace',index=False)