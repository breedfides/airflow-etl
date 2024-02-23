##########################################################
## Description: The fetch_cdc_soil_data_DAG is a chained sequence of tasks that fetches a geopackage soil data from a provided FTP site,
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

from src.utility import clip_soil_data, get_most_recent_dag_run, get_latest_file

####################
## DAG definition ##
####################
default_args = {
    "owner": "thünen_institute",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 6, tz='UTC'),
    "retries": 4,
    "retry_delay": timedelta(minutes=6)
}

dag = DAG(
    "fetch_soil_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "soil"]
)

with dag:
    dag_sensor = ExternalTaskSensor(
        task_id = 'sensor',
        external_dag_id = 'primary_DAG',
        external_task_id = 'ingest',
        mode = 'reschedule',
        execution_date_fn = lambda dt: get_most_recent_dag_run("primary_DAG"),
        poke_interval = 5
    )

    clip = PythonOperator(
        task_id='clip',
        python_callable=clip_soil_data,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )
    
    output = LocalFilesystemToS3Operator(
        task_id='output',
        filename=get_latest_file('output/soil/'),
        dest_key=get_latest_file('output/soil/').split("/output/")[-1], 
        dest_bucket='BreedFidesETL-OBS',
        aws_conn_id='aws_breedfides_obs',
        replace=True
    )
    
    
    dag_sensor >> clip >> output
