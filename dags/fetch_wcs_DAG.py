#################################################################################################
## Description: The fetch_wcs_DAG is a chained sequence of tasks that fetches geospatial coverage data from a provided url,
##              transforms and enriches it as a TIFF format and loads the output to a datalake on S3
##############################################################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
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

from src.utility import download_geodata, get_latest_files, write_wcs_to_s3

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
    "fetch_wcs",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "OGC"]
)

with dag:
    input = PythonOperator(
        task_id='input',
        python_callable=download_geodata,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )
    
    load = LocalFilesystemToS3Operator(
            task_id='write_wcs_output', 
            filename=get_latest_files(directory='wcs/')[0],
            dest_key='wcs',
            dest_bucket='BreedFidesETL-OBS',
            aws_conn_id='aws_breedfides_obs',
            replace=True
            )
    
    output = PythonOperator(
        task_id='output',
        python_callable=write_wcs_to_s3,
        provide_context=True,
        op_kwargs={'local_files': get_latest_files(directory='wcs/')}
    )
    
    input >> output
    # input >> load

