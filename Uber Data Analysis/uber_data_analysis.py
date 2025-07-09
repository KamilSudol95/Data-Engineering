import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

default_args = {
    'owner': 'kamil',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

project_id = "uber-data-analysis-464616"
dataset_id = "uber_data_engineering"

def etl_process(**kwargs):
    key_path = "/home/kamil_sudol5/credentials/uber-data-project-bigquery-access.json"
    credentials = service_account.Credentials.from_service_account_file(key_path)

    # DataLoad
    df = pd.read_parquet("https://storage.googleapis.com/uber-data-analysis-storage/yellow_tripdata_2024-01.parquet")
    df2 = pd.read_csv("https://storage.googleapis.com/uber-data-analysis-storage/taxi_zone_lookup.csv")

    # Data Transformation
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].copy()
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

    pickup_location_dim = df[['PULocationID']].drop_duplicates().reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index.astype(int)
    pickup_location_dim = pickup_location_dim.merge(df2, left_on='PULocationID', right_on='LocationID', how='left')
    pickup_location_dim['pickup_location_name'] = (pickup_location_dim['Borough'].fillna('') + ', ' + pickup_location_dim['Zone'].fillna('') + ', ' + pickup_location_dim['service_zone'].fillna(''))
    pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_location_name']]

    drop_location_dim = df[['DOLocationID']].drop_duplicates().reset_index(drop=True)
    drop_location_dim['drop_location_id'] = drop_location_dim.index.astype(int)
    drop_location_dim = drop_location_dim.merge(df2, left_on='DOLocationID', right_on='LocationID', how='left')
    drop_location_dim['drop_location_name'] = (drop_location_dim['Borough'].fillna('') + ', ' + drop_location_dim['Zone'].fillna('') + ', ' + drop_location_dim['service_zone'].fillna(''))
    drop_location_dim = drop_location_dim[['drop_location_id', 'drop_location_name']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    fact_table = df.merge(rate_code_dim, on='RatecodeID') \
        .merge(pickup_location_dim, left_on='PULocationID', right_on='pickup_location_id', how='left') \
        .merge(drop_location_dim, left_on='DOLocationID', right_on='drop_location_id', how='left') \
        .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
        .merge(payment_type_dim, on='payment_type') \
        [['VendorID', 'datetime_id', 
          'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id',
          'drop_location_id', 'payment_type_id', 'passenger_count', 'trip_distance',
          'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
          'improvement_surcharge', 'total_amount']]

    # BigQuery Load
    def load_to_bq(df, table_name):
        pandas_gbq.to_gbq(
            df,
            f"{dataset_id}.{table_name}",
            project_id=project_id,
            credentials=credentials,
            if_exists='replace'
        )

    load_to_bq(datetime_dim, "datetime_dim")
    load_to_bq(rate_code_dim, "rate_code_dim")
    load_to_bq(pickup_location_dim, "pickup_location_dim")
    load_to_bq(drop_location_dim, "drop_location_dim")
    load_to_bq(payment_type_dim, "payment_type_dim")
    load_to_bq(fact_table, "fact_table")

    print("Data loaded to BigQuery.")

with DAG(
    'uber_data_etl',
    default_args=default_args,
    description='ETL pipeline for Uber data into BigQuery',
    schedule_interval='@once',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['uber', 'bigquery', 'etl'],
) as dag:

    run_etl = PythonOperator(
        task_id='run_uber_etl',
        python_callable=etl_process,
    )

    run_etl