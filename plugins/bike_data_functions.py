import logging
import requests
import pandas as pd
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable

def _station_create_csv_table(**context):
    """Fetches station status data from Velib Metropole, appends it to a CSV on S3."""
    logging.info(f"Fetching and appending station data")

    # Fetching data
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json")
    json_data = response.json()

    df = pd.json_normalize(json_data["data"]["stations"])
    df = df[["station_id", "name", "lat", "lon", "capacity"]]

    csv_filename = f"station_information_demo_data2.csv"

    s3_hook = S3Hook(aws_conn_id="aws_default")
    local_file_path = f"/tmp/{csv_filename}"

    df.to_csv(local_file_path, header=True, index=False)

    # Upload the updated CSV back to S3
    s3_hook.load_file(filename=local_file_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"), replace=True)
    logging.info(f"Appended data to {csv_filename} and uploaded to S3")

    postgres_hook = PostgresHook(postgres_conn_id="postgres_supabase")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql("station_information_demo2", engine, if_exists="append", index=False)

def _fetch_and_create_csv_append_db(**context):
    """Fetches station status data from Velib Metropole, appends it to a CSV on S3."""
    logging.info(f"Fetching and appending station data")

    # Fetching data
    response = requests.get("https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json")

    def flatten_json_data(data):
        # Normalize the data
        csv_df = pd.json_normalize(data['data']['stations'], 'num_bikes_available_types', [
            'station_id', 'num_bikes_available', 'num_docks_available', 'last_reported', 'is_renting', 'is_installed'
        ])

        # Replace NaN values with zeros
        csv_df.fillna(0, inplace=True)

        # Aggregate the data to have mechanical and ebike in the same row for each station_id
        csv_df = csv_df.groupby('station_id').agg({
            'mechanical': 'sum',
            'ebike': 'sum',
            'num_bikes_available': 'first',
            'num_docks_available': 'first',
            'last_reported': 'first',
            'is_renting': 'first',
            'is_installed': 'first',
        }).reset_index()

        csv_df['last_reported'] = pd.to_datetime(csv_df['last_reported'], unit='s')

        return csv_df

    json_data = response.json()
    df = flatten_json_data(json_data)

    csv_filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_status_data.csv"

    s3_hook = S3Hook(aws_conn_id="aws_default")
    local_file_path = f"/tmp/{csv_filename}"

    df.to_csv(local_file_path, header=True, index=False)

    # Upload the updated CSV back to S3
    s3_hook.load_file(filename=local_file_path, key=csv_filename, bucket_name=Variable.get("S3BucketName"), replace=True)
    logging.info(f"Appended data to {csv_filename} and uploaded to S3")

    postgres_hook = PostgresHook(postgres_conn_id="postgres_supabase")
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql("status_station_demo2", engine, if_exists="append", index=False)

    counter = Variable.get('counter')
    Variable.set('counter', int(counter) + 1)

def _path_condition_function():
    counter_str = Variable.get('counter')
    counter = int(counter_str)

    print(f"Counter value: {counter}")
    if counter == 0:
        # return 'station_data_branch.station_create_csv_table'
        return 'station_data_branch.create_station_information_table'
    else:
        return 'fetch_and_create_csv_append_db'
