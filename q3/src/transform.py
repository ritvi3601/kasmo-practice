import pandas as pd

def transform_product_inventory(df):
    # --- Task 1: Remove rows where product_name is null/empty ---
    df = df.dropna(subset=['product_name'])
    df = df[df['product_name'].str.strip() != '']

    # --- Task 2: Clean product_name (remove spaces, Title Case) ---
    df['product_name'] = (
        df['product_name']
        .str.strip()
        .str.replace(r'\s+', ' ', regex=True)
        .str.title()
    )

    # --- Task 3: Convert prices to USD format with 2 decimals ---
    df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)  # ensure numeric
    df['price'] = df['price'].round(2)  # two decimals

    # --- Task 4: Validate prices and stock quantities ---
    df['price'] = df['price'].apply(lambda x: max(x, 0))  # no negatives
    df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce').fillna(0).astype(int)
    df['stock_quantity'] = df['stock_quantity'].apply(lambda x: max(x, 0))  # no negatives

    # --- Task 5: Data Enrichment ---
    # Stock Status
    def stock_status(qty):
        if qty < 20:
            return "Low"
        elif 20 <= qty <= 50:
            return "Medium"
        else:
            return "High"

    df['Stock Status'] = df['stock_quantity'].apply(stock_status)

    # Total Inventory Value (price * quantity)
    df['Total Inventory Value'] = (df['price'] * df['stock_quantity']).round(2)

    return df
