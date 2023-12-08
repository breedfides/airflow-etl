##########################################################
## Description: The fetch_cdc_radiation_DAG is a ...
##########################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
## Status: prod/dev
##########################################################
## Comments:
##########################################################

import json 
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from src.utility import download_geodata, clip_data

####################
## DAG definition ##
####################
default_args = {
    "owner": "thünen_institute",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 6, tz='UTC'),
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    "fetch_cdc_radiation",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "OGC", "radiation_global"]
)

with dag:
    input = PythonOperator(
        task_id='input',
        python_callable=download_geodata,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )
    
    clip = PythonOperator(
        task_id='clip',
        python_callable=clip_data,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )
    
    # output = LocalFilesystemToS3Operator(
    #     task_id='load',
    #     filename=,
    #     dest_key=,
    #     dest_bucket=,
    #     aws_conn_id=,
    #     replace=True
    # )
    
    
    input >> clip