#!/bin/bash
set -e
printenv
airflow db init
airflow users create --username airflow --firstname airflow --lastname airflow --role Admin --email airflow@airflow.com --password $AIRFLOW_USER_PASS
airflow scheduler & airflow webserver -p 8080