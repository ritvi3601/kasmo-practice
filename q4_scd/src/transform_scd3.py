def transform_scd_3(df_master, df_updates):
    # Copy to avoid mutating original
    df_result = df_master.copy()

    # Merge master with updates
    df_merged = df_result.merge(df_updates, on="CustomerID", how="left", suffixes=("", "_upd"))

    # Columns to track
    cols_to_track = ["Email", "Address", "LoyaltyTier"]

    for col in cols_to_track:
        # Define previous column name
        if col.lower() == "loyaltytier":
            prev_col = "PrevLoyaltyTier"
        else:
            prev_col = f"Previous_{col}"

        # Create previous col if it doesn't exist
        if prev_col not in df_result.columns:
            df_result[prev_col] = None

        # Detect change (ignore null updates)
        changed = (df_merged[f"{col}_upd"].notna()) & (df_merged[col] != df_merged[f"{col}_upd"])

        # Update previous column only on change
        df_result.loc[changed, prev_col] = df_merged.loc[changed, col]

        # Update actual column
        df_result.loc[changed, col] = df_merged.loc[changed, f"{col}_upd"]

        # --- Reorder so prev_col is right next to col ---
        cols = list(df_result.columns)
        if prev_col in cols:
            cols.remove(prev_col)
            idx = cols.index(col) + 1
            cols.insert(idx, prev_col)
            df_result = df_result[cols]

    return df_result
