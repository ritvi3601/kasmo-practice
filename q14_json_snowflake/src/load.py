from snowflake.connector.pandas_tools import write_pandas # type: ignore
import pandas as pd
def load(conn,curs,prod_dim,sales_dim,sales_fact,store_dim,time_dim):
    # print(sales_dim.columns)

    sales_tab="""
        CREATE OR REPLACE TABLE sales_dim (
            supplier_id INT PRIMARY KEY,
            supplier_name STRING,
            contact_email STRING,
            supplier_country STRING,
            reliability_score FLOAT,
            region_id INT,
            region_name STRING,
            region_country STRING,
            regional_manager STRING,
            promotion_id INT,
            promotion_name STRING,
            discount_percentage FLOAT,
            start_date DATE,
            end_date DATE
        )
    """

    curs.execute(sales_tab)

    time_tab="""
            CREATE OR REPLACE TABLE time_dim (
            date_id INT PRIMARY KEY,
            date DATE,
            day_of_week STRING,
            month STRING,
            quarter STRING,
            year INT,
            fiscal_year STRING,
            fiscal_quarter STRING,
            is_holiday BOOLEAN,
            holiday_name STRING,
            week_number INT,
            is_weekend BOOLEAN,
            is_business_day BOOLEAN)
    """

    curs.execute(time_tab)

    prod_tab="""CREATE OR REPLACE TABLE prod_dim (
  product_id INT PRIMARY KEY,
  product_name STRING,
  category STRING,
  product_line STRING,
  brand STRING,
  price FLOAT,
  sku STRING,
  description STRING,
  weight_kg FLOAT,
  supplier_id INT,
  is_active BOOLEAN,
  stock_level INT,
  FOREIGN KEY (supplier_id) REFERENCES sales_dim(supplier_id))"""
    
    curs.execute(prod_tab)

    store_tab="""
        CREATE OR REPLACE TABLE store_dim (
            store_id INT PRIMARY KEY,
            store_name STRING,
            city STRING,
            state STRING,
            country STRING,
            store_type STRING,
            manager_name STRING,
            opening_date DATE,
            region_id INT,
            square_footage FLOAT,
            employee_count INT
        )
    """

    curs.execute(store_tab)

    sales_tab_1="""
        CREATE OR REPLACE TABLE sales_fact (
            sale_id INT PRIMARY KEY,
            product_id INT,
            date_id INT,
            store_id INT,
            promotion_id INT,
            quantity_sold INT,
            discount_applied FLOAT,
            tax_amount FLOAT,
            net_amount FLOAT,
            total_amount FLOAT,
            customer_id STRING,
            payment_method STRING,
            transaction_type STRING,
            FOREIGN KEY(product_id) REFERENCES prod_dim(product_id),
            FOREIGN KEY(date_id) REFERENCES time_dim(date_id),
            FOREIGN KEY (store_id) REFERENCES store_dim(store_id))
            """
    curs.execute(sales_tab_1)
    # pd.to_csv(sales_dim)
    # print(sales_dim)
    # sales_dim.to_sql('sales_dim',conn,index=False,if_exists='replace')
    result_1=write_pandas(conn,sales_dim,'SALES_DIM',quote_identifiers=False)
    result_2=write_pandas(conn,time_dim,'TIME_DIM',quote_identifiers=False)
    result_3=write_pandas(conn,prod_dim,'PROD_DIM',quote_identifiers=False)
    result_4=write_pandas(conn,store_dim,'STORE_DIM',quote_identifiers=False)
    result_5=write_pandas(conn,sales_fact,'SALES_FACT',quote_identifiers=False)

    # print(success_1)






    

    


    