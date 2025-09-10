import pandas as pd
import numpy as np


df = pd.read_csv("us_customer_data.csv")

# Show first 5 rows
print(df.head())
df['phone'] = df['phone'].str.replace(r'[-.() +\s]', '', regex=True)
df['phone'] = df['phone'].astype(str).str.replace(r'^00', '', regex=True)
df['phone'] = df['phone'].astype(str).str.replace(r'x.*', '', regex=True)
df['phone'] = df['phone'].apply(lambda x: x if len(x) >= 10 else ' ')
df['phone'] = df['phone'].apply(lambda x: x[1:] if len(x) > 10 and x.startswith('1') else x)
def format_us_number(number):
    if pd.isna(number) or number == 'nan':
        return np.nan
    number = ''.join(filter(str.isdigit, number))  # remove any non-digit chars
    if len(number) == 10:  # format 10-digit number
        return f"+1 ({number[:3]}) {number[3:6]}-{number[6:]}"
    elif len(number) == 11 and number.startswith('1'):  # remove leading 1 if present
        number = number[1:]
        return f"+1 ({number[:3]}) {number[3:6]}-{number[6:]}"
    else:
        return np.nan  # if not 10 digits, make null

df['phone'] = df['phone'].apply(format_us_number)


# Show rows with null phone numbers
print(df[df['phone'].isnull()])
print(df[['phone']])

mask = df['email'].isnull()

# Split full_name into firstname and lastname
def generate_email(name):
    if pd.isna(name):  # if full_name is also null
        return np.nan
    parts = name.strip().split()
    if len(parts) >= 2:
        first = parts[0].lower()
        last = parts[-1].lower()
        return f"{first}.{last}@example.com"
    else:
        # If only one name is present
        return f"{parts[0].lower()}@example.com"

# Apply only where email is null
df.loc[mask, 'email'] = df.loc[mask, 'name'].apply(generate_email)

print(df[['name', 'email']])

df.to_csv("us_customer_date_clean.csv", index=False)
