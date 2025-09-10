import pandas as pd
import numpy as np

def transform_scd_5(df_master, df_updates):
    
    # Make copies to avoid modifying the original dataframes
    df_master = df_master.copy()
    df_updates = df_updates.copy()

    # Ensure necessary SCD columns exist in the master table with initial values
    if 'Version' not in df_master.columns:
        df_master['Version'] = 1
    if 'CurrentFlag' not in df_master.columns:
        df_master['CurrentFlag'] = 1
    if 'PrevLoyaltyTier' not in df_master.columns:
        df_master['PrevLoyaltyTier'] = np.nan
    if 'SubscriptionEnd' not in df_master.columns:
        df_master['SubscriptionEnd'] = pd.NaT
    if 'SubscriptionStart' not in df_master.columns:
        df_master['SubscriptionStart'] = pd.NaT

    # Identify new customers and append them to the master table
    new_customers = df_updates[~df_updates['CustomerID'].isin(df_master['CustomerID'])].copy()
    if not new_customers.empty:
        new_customers['Version'] = 1
        new_customers['CurrentFlag'] = 1
        new_customers['PrevLoyaltyTier'] = np.nan
        new_customers['SubscriptionEnd'] = pd.NaT
        # Add a SubscriptionStart column if it's not present in the master
        if 'SubscriptionStart' not in df_master.columns and 'SubscriptionStart' not in new_customers.columns:
            new_customers['SubscriptionStart'] = pd.Timestamp.now().normalize()
        df_master = pd.concat([df_master, new_customers], ignore_index=True)

    # Perform a left merge to handle updates for all customers present in the updates table
    df_merged = df_master.merge(df_updates, on="CustomerID", how="left", suffixes=('_master', '_update'))

    # --- Step 1: Handle SCD Type 1 (Email and Phone) and SCD Type 3 (LoyaltyTier) ---
    # Apply updates for existing records in a single pass
    for _, row in df_updates.iterrows():
        customer_id = row['CustomerID']
        
        # Check if the customer exists in the master table
        master_rows = df_master[df_master['CustomerID'] == customer_id].copy()
        if master_rows.empty:
            continue
        
        # We only care about the current version of the customer
        current_master_row_index = master_rows[master_rows['CurrentFlag'] == 1].index
        if current_master_row_index.empty:
            continue
        
        # SCD Type 1: Overwrite Email and Phone
        if pd.notna(row.get('Email')) and row['Email'] != df_master.loc[current_master_row_index, 'Email'].iloc[0]:
            df_master.loc[current_master_row_index, 'Email'] = row['Email']
        if pd.notna(row.get('Phone')) and row['Phone'] != df_master.loc[current_master_row_index, 'Phone'].iloc[0]:
            df_master.loc[current_master_row_index, 'Phone'] = row['Phone']

        # SCD Type 3: Update LoyaltyTier and PrevLoyaltyTier
        if pd.notna(row.get('LoyaltyTier')) and row['LoyaltyTier'] != df_master.loc[current_master_row_index, 'LoyaltyTier'].iloc[0]:
            df_master.loc[current_master_row_index, 'PrevLoyaltyTier'] = df_master.loc[current_master_row_index, 'LoyaltyTier']
            df_master.loc[current_master_row_index, 'LoyaltyTier'] = row['LoyaltyTier']

    # --- Step 2: Handle SCD Type 2 (Address) ---
    # Identify address changes
    address_changes = df_updates[
        (df_updates['Address'].notna()) &
        (df_updates['CustomerID'].isin(df_master['CustomerID']))
    ]

    new_rows_to_add = []
    for _, row in address_changes.iterrows():
        customer_id = row['CustomerID']
        master_row = df_master[(df_master['CustomerID'] == customer_id) & (df_master['CurrentFlag'] == 1)]

        # Check for actual address change
        if not master_row.empty and master_row['Address'].iloc[0] != row['Address']:
            # Deactivate the old record
            df_master.loc[master_row.index, 'CurrentFlag'] = 0
            df_master.loc[master_row.index, 'SubscriptionEnd'] = pd.Timestamp.now().normalize()
            
            # Create a new record
            new_record = master_row.copy()
            new_record['Address'] = row['Address']
            new_record['Version'] = master_row['Version'].iloc[0] + 1
            new_record['CurrentFlag'] = 1
            new_record['SubscriptionStart'] = pd.Timestamp.now().normalize()
            new_record['SubscriptionEnd'] = pd.NaT
            
            new_rows_to_add.append(new_record)

    if new_rows_to_add:
        df_master = pd.concat([df_master] + new_rows_to_add, ignore_index=True)

    return df_master.reset_index(drop=True)