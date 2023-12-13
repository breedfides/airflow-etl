##########################################################
## Description: The primary_DAG is an orchestrator DAG that receives inputs made on the frontend via POST requests made to Airflow's API,
##              it then triggers other DAGS using attributes found on the response payload
##########################################################
## Project: BMI Thünen Institute Breedfides
## Date: 28.11.2023
## Status: prod/dev
##########################################################
## Comments:
##########################################################

import json 
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from src.utility import fetch_payload

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
    "primary_DAG",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "OGC"]
)

dag_ids = ['fetch_cdc_radiation', 'fetch_cdc_air_temp'] ## DAGs to be triggered using the INPUTs from the API payloads
with dag:
    ingest = PythonOperator(
        task_id = 'ingest',
        python_callable = fetch_payload,
        provide_context = True,
        execution_timeout = timedelta(seconds=3600)
    )
    
    # List to store TriggerDagRunOperators
    trigger_downstreams = []
    
    for dag_id in dag_ids:
        trigger_downstream = TriggerDagRunOperator(
            task_id = dag_id,
            trigger_dag_id = dag_id,
            conf = {
                'input_attributes': "{{ task_instance.xcom_pull(task_ids='ingest', key='payload_key') }}"
            } 
        )
        
        trigger_downstreams.append(trigger_downstream)
        
    
    ingest >> trigger_downstreams
    