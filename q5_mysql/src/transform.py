import pandas as pd
def transform_purchase(products, customers,orders,order_items):
        merged_df = pd.merge(
        orders,
        order_items,
        on="order_id",   # Join on order_id
        how="inner"      # Only matching rows (change to 'left' if you want all orders even without items)
    )
        
        merged_df["line_total"] = merged_df["quantity"] * merged_df["price"]
        merged_df = merged_df[merged_df["status"].str.upper() == "COMPLETE"]
        merged_df["discount"] = merged_df.apply(
        lambda row: row["line_total"] * 0.10 if row["quantity"] >= 5 else 0, axis=1
    )
        merged_df["net_total"] = merged_df["line_total"] - merged_df["discount"]


        merged_df["order_date"] = pd.to_datetime(merged_df["order_date"])
        merged_df["order_month"] = merged_df["order_date"].dt.month
        merged_df["order_year"] = merged_df["order_date"].dt.year

    
        order_summary = merged_df.groupby(
        ["order_id", "customer_id", "order_date", "order_month", "order_year"]
    ).agg(
        order_total=("net_total", "sum"),
        total_quantity=("quantity", "sum")
    ).reset_index()
        order_summary1=order_summary
        order_summary1 = order_summary1.merge(customers[["customer_id", "region"]], on="customer_id", how="left")

    # Regional and monthly aggregation
        region_month_summary = order_summary1.groupby(
        ["region", "order_year", "order_month"]
    ).agg(
        total_revenue=("order_total", "sum"),
        order_count=("order_id", "count"),
        region_rank=("region","count")
    ).reset_index()
    
    # Region ranking by revenue
        region_month_summary["region_rank"] = region_month_summary.groupby(
        ["order_year", "order_month"]
    )["total_revenue"].rank(method="dense", ascending=False)
    
        merged_df1=merged_df
        merged_df1 = merged_df1.merge(products[["product_id", "category"]], on="product_id", how="left")

    # Category-wise sales
        category_summary = merged_df1.groupby(
        ["category", "order_year", "order_month"]
    ).agg(
        total_revenue=("net_total", "sum"),
        total_quantity=("quantity", "sum")
    ).reset_index()
    
    # Outlier flag: orders > 95th percentile
        threshold = merged_df["net_total"].quantile(0.95)
        merged_df["is_outlier"] = merged_df["net_total"] > threshold
    
    # Sort region_month_summary
        region_month_summary = region_month_summary.sort_values(
        ["order_year", "order_month", "total_revenue"], ascending=[True, True, False]
    ).reset_index(drop=True)
    
        return merged_df, order_summary, region_month_summary, category_summary
