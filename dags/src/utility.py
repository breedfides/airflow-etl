##########################################################
###### UTILITY MODULE FOR AIRFLOW DAGs. ##################
## Description: This script contains functions utilized by the Airflow DAGs as python callables.
##
## Functions:
##     - fetch_api: Function to retrieve data from an API source
##     - transform_wcs/wfs: Function to transform the fetched data as per requirements.
##     - load: Function to load the transformed data into a destination S3 bucket
#################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
## Status: prod/dev
#################################################
## Comments:
##################################################

import json
import owslib.fes
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
import rasterio
from rasterio import plot, MemoryFile
import logging
from datetime import datetime
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def fetch_api(**kwargs):
    """
    Description: The `fetch_api` function retrieves data from independent external data sources (wfs, wcs & ftp) and saves the output to a designated local directory 
    """
    params = kwargs['params']
    dag_id = kwargs['dag'].dag_id ## Extracts DAG-ID from context object
    srsname, bbox = params['srsname'], params['bbox']
    
    try:
        if dag_id == 'fetch_wcs':
            wcs_url, cov_id = params['wcs_url'], params['cov_id']
            
            logger.info(f"Fetching Geo data from {wcs_url}")
            wcs = WebCoverageService(wcs_url, version='2.0.1')

            response = wcs.getCoverage(
                identifier = [cov_id], 
                crs = srsname,
                subsets = bbox, 
                format = 'image/tiff')
        
            ## get rid of non ascii characters for filename:
            filename = ''.join(e for e in cov_id if e.isalnum())

            ## write response to hdd as json file:
            logger.info(f"Writing coverage file to breedfides-airflow/wcs/{filename}.tif")
            with open(f'wcs/{filename}.tif', 'wb') as file:
                file.write(response.read())
            
        elif dag_id == 'fetch_wfs':
            wfs_url, typename = params['wfs_url'], params['typename']
            wfs = WebFeatureService(url=wfs_url, version='1.1.0')
        
            logger.info(f"Fetching Geospatial feature data from {wfs_url}")
            response = wfs.getfeature(
                typename = typename,
                bbox = bbox,
                srsname = srsname,
                outputFormat = 'json')
        
            logger.info(f"Writing feature data to breedfides-airflow/wfs/{filename}.json")
            with open(f'wfs/{filename}.json', 'w') as json_file:
                json_file.write(response)
        
    except Exception as e:
        logger.error(f"An error occured while extracting the GeoNetwork data: {e}")
        raise 
    
    
## def transform_wcs(**kwargs):
    
    