import pandas as pd

def transform_scd_1(df_master, df_updates):
    # Ensure CustomerID is key in both
    df_master = df_master.set_index("CustomerID")
    df_updates = df_updates.set_index("CustomerID")

    # Loop through updates
    for cid, row in df_updates.iterrows():
        if cid in df_master.index:
            # Check if email/phone differ
            if row["Email"] != df_master.at[cid, "Email"]:
                df_master.at[cid, "Email"] = row["Email"]
            if row["Phone"] != df_master.at[cid, "Phone"]:
                df_master.at[cid, "Phone"] = row["Phone"]
            if row["Address"] != df_master.at[cid, "Address"]:
                df_master.at[cid, "Address"] = row["Address"]
            if row["LoyaltyTier"] != df_master.at[cid, "LoyaltyTier"]:
                df_master.at[cid, "LoyaltyTier"] = row["LoyaltyTier"]
        else:
            # New customer → add entire row
            df_master.loc[cid] = row

    return df_master.reset_index()
