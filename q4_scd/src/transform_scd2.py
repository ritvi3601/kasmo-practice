import pandas as pd
from datetime import datetime

def transform_scd_2(df_master, df_updates):
    # Columns to track changes
    scd_columns = ["Address", "Phone", "Email", "LoyaltyTier"]

    # Ensure required columns exist
    if not all(col in df_master.columns for col in ["CustomerID", "CurrentFlag", "Version"]):
        raise ValueError("df_master must contain CustomerID, CurrentFlag, and Version columns")

    # Iterate through updates
    for _, update_row in df_updates.iterrows():
        cust_id = update_row["CustomerID"]

        # Find current active record in master
        current_record = df_master[(df_master["CustomerID"] == cust_id) & (df_master["CurrentFlag"] == 1)]

        if not current_record.empty:
            current_record = current_record.iloc[0]

            # Check if any SCD-tracked column changed
            changes = any(current_record[col] != update_row[col] for col in scd_columns)

            if changes:
                # Expire the current record
                df_master.loc[
                    (df_master["CustomerID"] == cust_id) & (df_master["CurrentFlag"] == 1),
                    ["CurrentFlag"]
                ] = 0

                # Add new version with incremented version number
                new_version = current_record["Version"] + 1
                new_row = update_row.to_dict()
                new_row["Version"] = new_version
                new_row["CurrentFlag"] = 1

                df_master = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)

        else:
            # New customer → just insert
            new_row = update_row.to_dict()
            new_row["Version"] = 1
            new_row["CurrentFlag"] = 1
            df_master = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)
            df_master = df_master.sort_values(by=["CustomerID", "Version"]).reset_index(drop=True)


    return df_master
