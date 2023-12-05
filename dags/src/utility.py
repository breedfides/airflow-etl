##########################################################
###### UTILITY MODULE FOR AIRFLOW DAGs. ##################
## Description: This script contains functions utilized by the Airflow DAGs as python callables.
##
## Functions:
##     - fetch_payload: Function to retrieve data from an API source, sets the retrieved parameter as an input variable
##     - download_geodata: Function downloads the geo specific data related to the downstream DAG 
##     - clip: Function clips the downloaded geo-data using the inputs generated from the upstream DAG per requirements.
##     - load: Function to load the transformed data into a destination S3 bucket
#################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
## Status: prod/dev
#################################################
## Comments:
##################################################

import json
import ast
import owslib.fes
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
import rasterio
from rasterio import plot, MemoryFile
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def fetch_payload(**kwargs):
    """
    Description: The `fetch_payload` function listens to POST requests made on airflow's API endpoint, retrieves the payload and 
                 sets it as input parameters to be used by other downstream DAGs 
    """
    params = kwargs['params']
    
    try:      
        kwargs['ti'].xcom_push(key='payload_key', value=params) ## Returns INPUT attributes for downstream tasks
        
    except Exception as e:
        logger.error(f"An error occured while extracting the JSON payload: {e}")
        raise

    
def download_geodata(**kwargs):
    """
    Description: The `download_geodata` function fetches the related geo/climate related data from independent sources (API or FTP), the retrieved data will
                 be clipped subsequently using the input attrbutes
    """
    input_var = ast.literal_eval(kwargs['params']['input_attributes'])
    dag_id = kwargs['dag'].dag_id ## Extracts DAG-ID from context object
    
    try:
        if dag_id == 'fetch_wcs':
            wcs_url, cov_id = input_var['wcs_url'], input_var['cov_id']
            
            logger.info(f"Fetching Geo data from {wcs_url}")
            wcs = WebCoverageService(wcs_url, version='2.0.1')

            response = wcs.getCoverage(
                identifier = [cov_id], 
                crs = input_var['srsname'],
                subsets = input_var['bbox'], 
                format = 'image/tiff')
        
            ## get rid of non ascii characters for filename:
            filename = ''.join(e for e in cov_id if e.isalnum())

            ## write response to hdd as json file:
            logger.info(f"Writing coverage file to breedfides-airflow/wcs/{filename}.tif")
            with open(f'wcs/{filename}.tif', 'wb') as file:
                file.write(response.read())
            
        elif dag_id == 'fetch_wfs':
            typename = input_var['typename']
            filename = typename.replace(':', '_')
            
            wfs_url = input_var['wfs_url']
            wfs = WebFeatureService(url=wfs_url, version='1.1.0')
        
            logger.info(f"Fetching Geospatial feature data from {wfs_url}")
            response = wfs.getfeature(
                typename = typename,
                bbox = input_var['bbox'],
                srsname = input_var['srsname'],
                outputFormat = 'json').read().decode('utf-8')
            
            response = json.loads(response) ### CONVERT TO JSON OBJECT
        
            logger.info(f"Writing feature data to breedfides-airflow/wfs/{filename}.json")
            with open(f'wfs/{filename}.json', 'w') as json_file:
                json.dump(response, json_file)
        
    except Exception as e:
        logger.error(f"An error occured while extracting the GeoNetwork data: {e}")
        raise 
    
    
## def transform_wcs(**kwargs):
    
    