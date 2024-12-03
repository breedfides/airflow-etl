#################################################################################################
## Description: Fetches soilgrids data from soilgrids REST API via lat/lon. Output data
##              will be written in S3 object store.
##############################################################################################
## Project: Breedfides
## Date: 22.11.2024
## Status: prod/dev
##############################################################################################
## Comments:
##############################################################################################

from airflow.decorators import dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


import pendulum
from datetime import datetime, timedelta

from src.utility import fetch_soilgrids, get_latest_files, write_to_s3, get_most_recent_dag_run
from airflow.sensors.external_task_sensor import ExternalTaskSensor

####################
## DAG definition ##
####################
default_args = {
    "owner": "thünen_institute",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 6, tz='UTC'),
    "retries": 3,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    "fetch_soilgrids",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "soil", "soilgrids"]
)

with dag:
    dag_sensor = ExternalTaskSensor(
        task_id = 'sensor',
        external_dag_id = 'fetch_cdc_air_temp',
        external_task_id = 'output',
        mode = 'reschedule',
        execution_date_fn = lambda dt: get_most_recent_dag_run("fetch_cdc_air_temp"),
        poke_interval = 5
    )
        
    input = PythonOperator(
        task_id='input',
        python_callable=fetch_soilgrids,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )
        
    output = PythonOperator(
        task_id='output',
        python_callable=write_to_s3,
        provide_context=True,
        op_kwargs={'local_files': get_latest_files(directory='output/soilgrids/')}
    )
    
    dag_sensor >> input >> output