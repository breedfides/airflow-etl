requirements:
	pip3 install -r requirements.txt

geospatial:
	sudo apt install gdal-bin python3-gdal libgdal-dev

python-dep:
	sudo apt-get install python3-dev

apt_update:
	sudo apt-get update

build-essential:
	sudo apt-get install --reinstall build-essential

postgres:
	sudo apt install postgresql

create_database:
	sudo -u postgres psql -l | grep -w airflow_db || sudo -u postgres createdb airflow_db

create_user:
	sudo -u postgres psql -c "CREATE USER airflow_user WITH PASSWORD 'airflow_pass';"

grant_privileges:
	sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;"

use_db_and_grant_all:
	sudo -u postgres psql -c "GRANT ALL ON SCHEMA public TO airflow_user;"

init_airflow_db:
	airflow db init

airflow_create_user:
	airflow users create --username airflow --firstname airflow --lastname airflow --role Admin --email airflow@airflow.com --password airflow

install-dependencies:
	make python-dep && make apt_update && make geospatial && make apt_update && make build-essential && make requirements && make postgres && make create_database && make create_user && make grant_privileges && make use_db_and_grant_all && make init_airflow_db && make airflow_create_user

run-airflow:
	airflow scheduler & airflow webserver -p 8080
