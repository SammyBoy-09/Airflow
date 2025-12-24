import pandas as pd
from sqlalchemy import create_engine

# TODO: FUTURE ENHANCEMENT - Option 2 (Smart Incremental Load)
# Implement date-based incremental loading with metadata tracking:
# 1. Create etl_metadata table to store last_load_date per table
# 2. Query metadata before load to get last successful load timestamp
# 3. Filter DataFrame based on last_order_date > last_load_date
# 4. Update metadata table after successful load
# 5. Handle first run (no metadata) as full load
# Benefits: Only processes new/updated records, more efficient
# Estimated: ~30-35 lines across Extract/Transform/Load files

def load(
    df: pd.DataFrame,
    csv_path: str = "cleaned_data.csv",
    xlsx_path: str = "cleaned_data.xlsx",
    load_type: str = "full",
    bulk_chunk_size: int = 1000
):
    """
    Save cleaned data to CSV, Excel, and PostgreSQL table.
    
    Args:
        df: DataFrame to load
        csv_path: Path to save CSV file
        xlsx_path: Path to save Excel file
        load_type: 'full' (replace all data) or 'incremental' (append new data)
        bulk_chunk_size: Number of rows per bulk insert (improves performance)
    """

    # 1) Save to files (optional – keep if you still want them)
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)
    print(f"[LOAD] Saved files: {csv_path}, {xlsx_path}")

    # 2) Save to PostgreSQL (inside Docker)
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    # 3) Determine load strategy based on load_type
    if load_type == "incremental":
        if_exists_mode = "append"  # Add new rows to existing table
        print(f"[LOAD] Incremental load: Appending {len(df)} rows")
    else:
        if_exists_mode = "replace"  # Drop and recreate table
        print(f"[LOAD] Full load: Replacing table with {len(df)} rows")

    # 4) Write to table 'customers_cleaned' with bulk loading
    df.to_sql(
        "customers_cleaned",
        engine,
        if_exists=if_exists_mode,
        index=False,
        chunksize=bulk_chunk_size  # Bulk load in chunks for better performance
    )
    print(f"[LOAD] Written to PostgreSQL table: customers_cleaned (mode: {load_type})")