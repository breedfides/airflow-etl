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
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from src.utility import download_geodata, clip_data, get_most_recent_dag_run, get_latest_file

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
    dag_sensor = ExternalTaskSensor(
        task_id = 'sensor',
        external_dag_id = 'fetch_soil_data',
        external_task_id = 'clip',
        mode = 'reschedule',
        execution_date_fn = lambda dt: get_most_recent_dag_run("fetch_soil_data"),
        poke_interval = 5
    )

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
    
    output = LocalFilesystemToS3Operator(
        task_id='output',
        filename=get_latest_file('output/radiation_global/'),
        dest_key=f"radiation_global/{os.path.basename(get_latest_file('output/radiation_global/'))}", 
        dest_bucket='BreedFidesETL-OBS',
        aws_conn_id='aws_breedfides_obs',
        replace=True
    )
    
    
    dag_sensor >> input >> clip >> output
