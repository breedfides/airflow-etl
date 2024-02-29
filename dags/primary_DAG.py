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
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from src.utility import fetch_payload, get_most_recent_dag_run

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

dag_ids = {'fetch_soil_data':['primary_DAG', 'ingest'], 'fetch_cdc_radiation':['fetch_soil_data', 'output'], 'fetch_cdc_air_temp':['fetch_cdc_radiation', 'output']} ## DAGs to be triggered using the INPUTs from the API payloads

with dag:
    ingest = PythonOperator(
        task_id = 'ingest',
        python_callable = fetch_payload,
        provide_context = True,
        execution_timeout = timedelta(seconds=3600)
    )
    
    # List to store TriggerDagRunOperators
    trigger_downstreams = []
    
    for dag_id, external_dag in dag_ids.items():
        dag_sensor = ExternalTaskSensor(
            task_id = f'{dag_id}_sensor',
            external_dag_id = external_dag[0],
            external_task_id = external_dag[1],
            mode = 'reschedule',
            execution_date_fn = lambda dt: get_most_recent_dag_run(external_dag[0]),
            poke_interval = 5
        )
        
        trigger_downstream = TriggerDagRunOperator(
            task_id = dag_id,
            trigger_dag_id = dag_id,
            conf = {
                'input_attributes': "{{ task_instance.xcom_pull(task_ids='ingest', key='payload_key') }}"
            }
        )
        
        trigger_downstreams += [dag_sensor, trigger_downstream]

    ingest >> trigger_downstreams
