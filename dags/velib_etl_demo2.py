
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
# from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.operators.postgres_operator import PostgresOperator

from bike_data_functions import _station_create_csv_table, _fetch_and_create_csv_append_db, _path_condition_function


    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id="bike_data_dag_demo2",
         default_args=default_args,
         schedule_interval=timedelta(seconds=120),
         catchup=False) as dag:


    
    
    start = DummyOperator(task_id="start")



    condition_path = BranchPythonOperator(
        task_id="condition_path",
        python_callable=_path_condition_function,
    )

    fetch_and_create_csv_append_db = PythonOperator(
        task_id="fetch_and_create_csv_append_db",
        python_callable=_fetch_and_create_csv_append_db,
        provide_context=True
    )

    with TaskGroup("station_data_branch") as station_data_branch:
        create_station_information_table = PostgresOperator(
            task_id="create_station_information_table",
            postgres_conn_id="postgres_supabase",
            sql="""
                CREATE TABLE IF NOT EXISTS station_information_demo2 (
                    id SERIAL PRIMARY KEY,
                    station_id BIGINT UNIQUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    name VARCHAR,
                    lat FLOAT8,
                    lon FLOAT8,
                    capacity INT4
                );
            """
        )

        create_status_station_table = PostgresOperator(
            task_id="create_status_station_table",
            postgres_conn_id="postgres_supabase",
            sql="""
                CREATE TABLE IF NOT EXISTS status_station_demo2 (
                    id SERIAL PRIMARY KEY,
                    station_id BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    ebike INT4,
                    mechanical INT4,
                    num_bikes_available INT4,
                    num_docks_available INT4,
                    last_reported TIMESTAMPTZ,
                    is_renting INT2,
                    is_installed INT2,
                    FOREIGN KEY (station_id) REFERENCES station_information_demo2 (station_id)
                );
            """
        )

        station_branch = PythonOperator(task_id="station_create_csv_table",
                                        python_callable=_station_create_csv_table,
                                        provide_context=True)

        first_fetch_and_create_csv_append_db = PythonOperator(task_id="first_fetch_and_create_csv_append_db",
                                                              python_callable=_fetch_and_create_csv_append_db,
                                                              provide_context=True)

        create_station_information_table >> create_status_station_table >> station_branch >> first_fetch_and_create_csv_append_db

    csv_psql_appended = DummyOperator(task_id="csv_psql_appended", trigger_rule="one_success")

    (
        start
        >> condition_path
        >> [station_data_branch, fetch_and_create_csv_append_db]
        >> csv_psql_appended
    )