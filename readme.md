# BreedFides-ETL

## Usage
* Clone the repository to the local filesystem

* Navigate to the repository directory using the Terminal

* To install the project's dependencies, execute the command ```make install-dependencies``` - This command not only installs the packages listed in the requirements.txt file but also configures PostgreSQL and initializes the metadata database used by Airflow.

* Rename the ```airflow.cfg-dev-test.txt``` to ```airflow.cfg``` as this config file has all the configurations needed to execute a DAG-RUN on a dev environment (NOTE: Before doing this rename the current ```airflow.cfg``` to ```airflow-prod.cfg.txt```) 

* Start the Airflow webserver and its scheduler by running the command ```make run-airflow```

* Utilize one of the example headers available in ```post_example.json``` to trigger the Airflow DAGs.

* Data results generated from the ```wcs``` or ```wfs``` data-sources will be stored in a directory named after the respective data sources.