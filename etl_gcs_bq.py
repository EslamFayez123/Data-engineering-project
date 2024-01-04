import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import os
import numpy as np
import time
import requests
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime

@task(retries=3)
def extract_from_gcs(path) -> Path:
    """Download trip data from GCS"""
    gcs_path = f'us-accidents.zip'
    gcs_block = GcsBucket.load('fayez-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path='./')
    return Path(gcs_path)

@task()
def transform_data(df_us_accidents: pd.DataFrame) -> pd.DataFrame:
    """Fill missing values and fix dtypes"""
    df_us_accidents = pd.read_csv('us-accidents.zip',compression='zip')
    print(f"Pre : missing values : {df_us_accidents.isna().sum().sum()}")
    columns_with_missing_values = ['Street', 'City', 'Weather_Condition', 'Sunrise_Sunset']
    df_us_accidents[columns_with_missing_values] = df_us_accidents[columns_with_missing_values].fillna('Unknown')
    df_us_accidents[columns_with_missing_values] = df_us_accidents[columns_with_missing_values].replace(np.nan, 'Unknown')
    columns_list = ['ID', 'Severity', 'Start_Time', 'End_Time', 'Description', 'Street', 'City', 'State', 'Country', 'Weather_Condition', 'Sunrise_Sunset','Distance']
    df_us_accidents = df_us_accidents.loc[:, df_us_accidents.columns.isin(columns_list)]


    print(f"Post: Missing values: {df_us_accidents.isna().sum().sum()}")

    # Convert some columns to string type
    df_us_accidents.ID = df_us_accidents.ID.astype('str')
    df_us_accidents.Description = df_us_accidents.Description.astype('str')
    df_us_accidents.Street = df_us_accidents.Street.astype('str')
    df_us_accidents.City = df_us_accidents.City.astype('str')
    df_us_accidents.State = df_us_accidents.State.astype('str')
    df_us_accidents.Country = df_us_accidents.Country.astype('str')
    df_us_accidents.Weather_Condition = df_us_accidents.Weather_Condition.astype('str')
    df_us_accidents.Sunrise_Sunset = df_us_accidents.Sunrise_Sunset.astype('str')

    # Convert some columns to date and time format
    df_us_accidents['Start_Date'] = pd.to_datetime(df_us_accidents['Start_Time']).dt.strftime('%Y-%m-%d')
    df_us_accidents['End_Date'] = pd.to_datetime(df_us_accidents['End_Time']).dt.strftime('%Y-%m-%d')
    
    df_us_accidents['End_Hour'] = pd.to_datetime(df_us_accidents['End_Time']).dt.strftime('%H:%M:%S')
    df_us_accidents['Start_Hour'] = pd.to_datetime(df_us_accidents['Start_Time']).dt.strftime('%H:%M:%S')

    df_us_accidents.drop(['Start_Time', 'End_Time'], axis="columns", inplace=True)
    
    return df_us_accidents

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load('zoom-gcp-creds')
    df.to_gbq(
        destination_table='us_traffic_accidents_data.us_traffic_accidents',
        project_id='glossy-alliance-383920',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into BigQuery"""
    path = 'us-acus-accidents.zip'
    path = extract_from_gcs(path)
    df = transform_data(path)
    write_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()