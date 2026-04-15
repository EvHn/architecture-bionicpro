from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from clickhouse_driver import Client

POSTGRES_CONFIG = {
    'host': 'crm_db',
    'port': 5432,
    'database': 'crm_db',
    'user': 'crm_user',
    'password': 'crm_password'
}

CLICKHOUSE_CONFIG = {
    'host': 'olap_db',
    'port': 9000,
    'user': 'default',
    'password': '',
    'database': 'default'
}

def join_data(**context):
    pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
    users_df = pd.read_sql("SELECT id, name, email FROM customers", pg_conn)
    pg_conn.close()

    ch_client = Client(
        host=CLICKHOUSE_CONFIG['host'],
        port=CLICKHOUSE_CONFIG['port'],
        user=CLICKHOUSE_CONFIG['user'],
        password=CLICKHOUSE_CONFIG['password'],
        database=CLICKHOUSE_CONFIG['database']
    )
    ch_result = ch_client.execute("SELECT user_id, prosthesis_type, muscle_group, signal_frequency, signal_duration, signal_amplitude, signal_time FROM emg_sensor_data")
    ch_df = pd.DataFrame(ch_result, columns=['user_id', 'prosthesis_type', 'muscle_group', 'signal_frequency', 'signal_duration', 'signal_amplitude', 'signal_time'])

    joined_df = pd.merge(users_df, ch_df, left_on='id', right_on='user_id', how='left')

    joined_df = pd.DataFrame(joined_df, columns=['id', 'email', 'prosthesis_type', 'muscle_group', 'signal_frequency', 'signal_duration', 'signal_amplitude', 'signal_time'])


    create_query = f"""
    CREATE TABLE IF NOT EXISTS customer_reports (
        id UInt32,
        email String,
        prosthesis_type String,
        muscle_group String,
        signal_frequency UInt32,
        signal_duration UInt32,
        signal_amplitude Decimal(5,2),
        signal_time DateTime
    ) ENGINE = MergeTree()
    ORDER BY id
    """

    ch_client.execute(create_query)
    ch_client.execute(f"TRUNCATE TABLE customer_reports")
    joined_df = joined_df.where(pd.notnull(joined_df), None)
    data_tuples = list(joined_df.itertuples(index=False, name=None))

    ch_client.execute(
        f"INSERT INTO customer_reports (id, email, prosthesis_type, muscle_group, signal_frequency, signal_duration, signal_amplitude, signal_time) VALUES",
        data_tuples
    )

    print(f"Saved {len(data_tuples)} rows to ClickHouse table customer_reports")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='user_data_join_dag',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
) as dag:
    join_task = PythonOperator(
        task_id='join_users_and_sensors_data',
        python_callable=join_data,
    )

    join_task