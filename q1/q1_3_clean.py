
import pandas as pd

# Load CSV
df = pd.read_csv("transaction_data.csv")

# Make sure 'amount' column is numeric (remove $ sign if present)
df['amount'] = df['amount'].replace('[\$,]', '', regex=True).astype(float)

# Filter transactions greater than 1000
high_value_txns = df[df['amount'] > 1000]
high_value_txns['amount'] = high_value_txns['amount'].apply(lambda x: f"${x:,.2f}")

print(high_value_txns)

high_value_txns.to_csv("transactions_over_1000.csv", index=False)
