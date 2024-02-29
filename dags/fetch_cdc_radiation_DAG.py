##########################################################
## Description: The fetch_cdc_radiation_DAG is a chained sequence of tasks that fetches geospatial climate data from a provided FTP site,
##              clips the downloaded dataset using input lat/long positions and loads the output to a datalake on S3
##########################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
## Status: prod/dev
##########################################################
## Comments:
##########################################################

import json 
import os
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.utility import download_geodata, clip_data, get_latest_files, write_to_s3

####################
## DAG definition ##
####################
default_args = {
    "owner": "thünen_institute",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 6, tz='UTC'),
    "retries": 4,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    "fetch_cdc_radiation",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "DWD", "CDC", "radiation_global"]
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
    
    output = PythonOperator(
        task_id='output',
        python_callable=write_to_s3,
        provide_context=True,
        op_kwargs={'local_files': get_latest_files(directory='output/radiation_global/', extensions=('.nc', '.txt'))}

    )
    
    
    input >> clip >> output
