from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import your ETL functions
import sys
sys.path.append("/opt/airflow/scripts")

from Extract import extract
from Transform import transform
from Load import load


# ---------------------------------------
# DEFAULT DAG SETTINGS
# ---------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

# ---------------------------------------
# DEFINE DAG
# ---------------------------------------
with DAG(
    dag_id="customer_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",       # Runs daily (you can change)
    catchup=False
):

    # --------- TASK 1: EXTRACT ---------
    def extract_task():
        input_path = "/opt/airflow/data/raw/customers_200.xlsx"   # change if needed
        df = extract(input_path)
        return df.to_json()   # pass dataframe to next task

    extract_op = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task
    )

    # --------- TASK 2: TRANSFORM ---------
    def transform_task(ti):
        df_json = ti.xcom_pull(task_ids="extract_data")
        import pandas as pd
        df = pd.read_json(df_json)
        df = transform(df)
        return df.to_json()

    transform_op = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    # --------- TASK 3: LOAD ---------
    def load_task(ti):
        df_json = ti.xcom_pull(task_ids="transform_data")
        import pandas as pd
        df = pd.read_json(df_json)
        load(
            df,
            csv_path="/opt/airflow/data/processed/cleaned_data.csv",
            xlsx_path="/opt/airflow/data/processed/cleaned_data.xlsx",
            load_type="full",  # Options: 'full' or 'incremental'
            bulk_chunk_size=1000  # Bulk load in chunks of 1000 rows
        )

    load_op = PythonOperator(
        task_id="load_data",
        python_callable=load_task
    )

    # ORDER OF EXECUTION
    extract_op >> transform_op >> load_op