import pandas as pd

# Load both CSVs
df1 = pd.read_csv("us_customer_date_clean.csv")
df2 = pd.read_csv("transactions_over_1000.csv")

# Merge on customer_id (inner join)
merged_df = pd.merge(df1, df2, on="customer_id", how="inner")

# Save to new file
merged_df.to_csv("merged.csv", index=False)

print("CSV files merged on customer_id successfully!")
