##########################################################
## Description: The location_specific_air_temp_DAG is meant to work tied to fetch_cdc_air_temp_DAG and orchestrate some temperature related data manipulation
##              functions some of which can work independent from eachother which are seperated with branch operator. input position is included if user wants
##              to test functionality of dag, since they will need datasets for other positions to work, but then they will need to trigger dag_sensor manually.
##########################################################
## Project: BMI Thünen Institute Breedfides
## Date: 25.03.2024
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
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import BranchPythonOperator

from src.utility import download_geodata, get_location_specific_temps, get_daily_temp_extremes, add_address_info, get_most_recent_dag_run
from airflow.models import DagRun

####################
## DAG definition ##
####################
default_args = {
    "owner": "thünen_institute",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 6, tz='UTC'),
    "retries": 4,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "location_specific_air_temp",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["BreedFides", "DWD", "CDC", "QGIS", "air_temperature_mean"]
)

with dag:
    dag_sensor = ExternalTaskSensor(
        task_id='location_specific_air_temp_DAG_sensor',
        external_dag_id='fetch_cdc_air_temp',
        external_task_id='clip',
        mode='reschedule',
        execution_date_fn=lambda dt: get_most_recent_dag_run("fetch_cdc_air_temp"),
        poke_interval=5
    )

    input = PythonOperator(
        task_id='input',
        python_callable=download_geodata,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600)
    )

    extract_branch = BranchPythonOperator(
        task_id="extract_branch",
        python_callable=lambda: ["extract_daily_temps", "extract_extremes"],
        dag=dag
    )

    extract_daily_temps = PythonOperator(
        task_id='extract_daily_temps',
        python_callable=get_location_specific_temps,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600),
        trigger_rule="none_failed_or_skipped"
    )

    extract_extremes = PythonOperator(
        task_id='extract_extremes',
        python_callable=get_daily_temp_extremes,
        provide_context=True,
        execution_timeout=timedelta(seconds=3600),
        trigger_rule="none_failed_or_skipped"
    )

    """
    Warning: In this case 'add_address_info' function porcesses at most 4750 lines of data for each csv file, if whole range of TT_daytime dataset is available.
             Which then makes ca. 4750 x 2 = 9500 Requests in total at most. It can take very long time for process to finish.
             To avoid server side ban, one request is made at a time and not in bulks.
    """
    address = PythonOperator(
        task_id='address',
        python_callable=add_address_info,
        provide_context=True,
        execution_timeout=timedelta(seconds=10800),
        trigger_rule="none_failed_or_skipped"
    )

    # Downstream of input
    dag_sensor >> input >> extract_branch

    # Conditional branching
    extract_branch >> extract_daily_temps
    extract_branch >> extract_extremes >> address
    address.execution_date_fn = lambda dt: dt + timedelta(seconds=5)
