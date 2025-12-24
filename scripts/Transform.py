import pandas as pd
import numpy as np

def transform(df: pd.DataFrame):
    """
    Applies all your cleaning operations on the DataFrame.
    """

    # 1. CONVERT EMPTY / WHITESPACE TO NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)

    # 1st it fill empty spaces to NaN and after this in the name coulumn the NaN filled to UNKNOWN

    # 1.a FILLING NAMES WITH NOT GIVEN TO UNKNOWN
    '''df["name"] = df["name"].apply(
        lambda x: "Unknown" if pd.isna(x) or str(x).strip() == "" else x
    )'''  # or
    df["name"] = df["name"].replace("", np.nan)
    df["name"] = df["name"].fillna("Unknown")

    # 2. REMOVE DUPLICATE CUSTOMERS
    df = df.drop_duplicates(subset="email", keep="first")

    # 3. VALIDATE EMAIL FORMAT
    df = df[df["email"].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$',
                                     regex=True, na=False)]

    # 4. CONVERT NAMES TO UPPERCASE
    df["name"] = df["name"].str.upper()

    # 5. TRIM SPACES IN ALL STRING COLUMNS
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # 6. SORT BY MOST RECENT ORDER DATE
    df["last_order_date"] = pd.to_datetime(df["last_order_date"], errors="coerce")
    df = df.sort_values(by="last_order_date", ascending=False)

    ''''df["last_order_date"] = pd.to_datetime(df["last_order_date"],unit="ms",errors="coerce" )

    df = df.sort_values(by="last_order_date", ascending=False)'''

    # 7. AGE COLUMN TYPECASTING (STR TO INT) AND ALSO FILL THE MISSING VALUES TO MEAN AGE
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["age"] = df["age"].fillna(df["age"].mean()).astype(int)

    # 8. GLOBAL AGGREGATIONS ON ORDERS COLUMN (NO GROUP BY)

    max_orders = df["orders"].max()
    min_orders = df["orders"].min()
    total_orders = df["orders"].sum()

    orders_summary_df = pd.DataFrame([{
        "max_orders": max_orders,
        "min_orders": min_orders,
        "total_orders": total_orders
    }])

    # Save summary in BOTH formats
    orders_summary_df.to_csv("data/processed/orders_summary.csv", index=False)
    orders_summary_df.to_excel("data/processed/orders_summary.xlsx", index=False)

    # 8.b GROUP BY STATE AGGREGATIONS (ORDERS)

    state_summary_df = (
        df.groupby("state")["orders"].agg(
              max_orders="max",
              min_orders="min",
              total_orders="sum"
          ).reset_index()
    )

    # Append to SAME summary files
    state_summary_df.to_csv("data/processed/orders_summary.csv",mode="a",index=False)

    with pd.ExcelWriter("data/processed/orders_summary.xlsx", engine="openpyxl") as writer:
        orders_summary_df.to_excel(writer, sheet_name="global_summary", index=False)
        state_summary_df.to_excel(writer, sheet_name="state_summary", index=False)

    # 9.a CUSTOMER TYPE FEATURE
    if "orders" in df.columns:
        df["customer_type"] = "REGULAR"
        df.loc[df["orders"] > 100, "customer_type"] = "LOYAL"

    # 9.b CUSTOMER TENURE (DAYS)
    df["customer_tenure_days"] = ( pd.Timestamp.today() - df["last_order_date"] ).dt.days   # dt = data time accessor

    # 10. DATE FEATURES
    df["order_year"] = df["last_order_date"].dt.year
    df["order_month"] = df["last_order_date"].dt.month
    df["order_day"] = df["last_order_date"].dt.day_name()

    # 11. NORMALIZATION & SCALING (MIN-MAX) FOR TOTAL_SPEND
    if "total_spend" in df.columns:
        min_spend = df["total_spend"].min()
        max_spend = df["total_spend"].max()

        df["total_spend_normalized"] = ( df["total_spend"] - min_spend ) / (max_spend - min_spend)

    # 12. CONVERT DATETIME TO STRING FORMAT FOR CSV OUTPUT
    df["last_order_date"] = df["last_order_date"].dt.strftime('%Y-%m-%d')

    return df