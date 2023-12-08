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
import ftplib
import os
import gzip
import io
import xarray as xr
import dask
import glob
from osgeo import ogr, osr
import pyproj
import dask.dataframe as dd
import owslib.fes
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
import rasterio
from rasterio import plot, MemoryFile
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
current_dir = os.getcwd()

# Get today's date and time
date_now = datetime.now().strftime("%Y%m%d_%H%M")

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
            with open(f'{current_dir}/wcs/{filename}.tif', 'wb') as file:
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
            with open(f'{current_dir}/wfs/{filename}.json', 'w') as json_file:
                json.dump(response, json_file)
                
        elif 'fetch_cdc' in dag_id:
            ftp_dir = kwargs['dag'].tags[-1]
            ftp = ftplib.FTP('opendata.dwd.de', timeout=3600)
            local_file_path = f'{current_dir}/{ftp_dir}'
            
            ftp.login() ### Login to FTP site
            
            remote_directory = f"/climate_environment/CDC/grids_germany/daily/Project_TRY/{ftp_dir}"
            ftp.cwd(remote_directory)
            
            ftp_files = set(obj[:-3] for obj in ftp.nlst() if '_20' in obj) ### List the files in the remote FTP directory (data from year 2000)
            net_cdf_files = set(os.path.basename(fp) for fp in glob.glob(f'{local_file_path}/*.nc')) ### List the files in the local directory
            
            file_exists, files_to_download = verify_file_exists(net_cdf_files, ftp_files)
            
            if file_exists:
                pass
            
            else: 
                ## Download each file
                for file in files_to_download:
                    logger.info(f"Downloading file : {file}")
                    remote_file = io.BytesIO() ## Open remote file 
                    ftp.retrbinary('RETR ' + file, remote_file.write)
                    remote_file.seek(0)
                    
                    ## Decompress GZIP file
                    with gzip.GzipFile(fileobj=remote_file, mode='rb') as gzipped_file:
                        decompressed_content = gzipped_file.read() ## Read the decompressed content
                
                    logger.info(f"Writing netCDF data to {local_file_path}")
                
                    with open(f"{os.path.join(f'{local_file_path}/', os.path.splitext(file)[0])}", 'wb') as local_file:
                        local_file.write(decompressed_content)
                        
            ## Close the FTP connection
            ftp.quit()
            
    except Exception as e:
        logger.error(f"An error occured while extracting the GeoNetwork data: {e}")
        raise 
    
   
def clip_data(**kwargs):
    """
    Description: The `clip_data` function utilizes the input attributes (the lattitude and longitude), creates a buffer-extent using both coordinates and a buffer,
                 it then converts the buffer-extent to min/max lat-long coordinates which in turn uses these attributes to clip the downloaded netCDF4 geodata
    """
    input_var, geo_tag = ast.literal_eval(kwargs['params']['input_attributes']), kwargs['dag'].tags[-1]
    directory = f"{current_dir}/{geo_tag}"
    
    try:
        latitude, longitude, buffer_in_metres = input_var['lat'], input_var['long'], 3000
        
        buffer_extent = compute_buffer_extent(longitude, latitude, buffer_in_metres)
        lat_min, lat_max, long_min, long_max = convert_buffer_extent(buffer_extent)
        
        clipped_data = []
        file_list = [f'{directory}/{file}' for file in os.listdir(f'{directory}/')[1:]]
        
        for geo_data in file_list:
            dataset = xr.open_dataset(geo_data)
            ddf = dataset.to_dask_dataframe() ### Fit all netCDF files to a Dask dataframe
            
            # Clip the Dask DataFrame based on the converted buffer extent
            ddf_clipped = ddf[(ddf['lon'] >= long_min) & (ddf['lon'] <= long_max) & (ddf['lat'] >= lat_min) & (ddf['lat'] <= lat_max)]
            
            if len(ddf_clipped) > 0:
                clipped_data.append(ddf_clipped)
        
        ### Concatenate and save clipped output as parquet
        if clipped_data:
            concat_ddf = dd.concat(clipped_data, axis=0)  
              
            output_path = f'{current_dir}/output/{geo_tag}/{geo_tag}_{date_now}.parquet'
            logger.info(f"Writing clipped data to {output_path}")
            concat_ddf.to_parquet(output_path, engine='pyarrow') 
        
        else:
            logger.warning("No data to save. Clipped data is empty.")
        
    except Exception as e:
        logger.error(f"An error occured while clipping the GeoNetwork data: {e}")
        raise 
    

def verify_file_exists(local, ftp):
    """
    Description: The `verify_file_exists` function compares two sets of filenames, one representing files locally and the other obtained from an FTP server. 
    
    Output: It returns a tuple indicating whether the sets are identical (True OR False) and a set of differing items 
            which will then be downloaded to the local filesystem 
    """
    differing_items = set(item + '.gz' for item in local.symmetric_difference(ftp))
    
    return local == ftp, differing_items

    
def compute_buffer_extent(long, lat, buffer_in_metres):
    """
    Description: The `compute_buffer_extent` function calculates the bounding box (extent) of a buffer 
                 around a specified point defined by user's input (i.e. latitude and longitude). The buffer is created with a user-defined 
                 radius in meters (3000 metres). 
    
    Output: The resulting extent is provided as a tuple representing the minimum and maximum coordinates in the UTM Zone 32 North coordinate reference system.
    """    
    try:
        ### creation of point geometry with user / front end lat-long values
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(float(long), float(lat))
        
        # spatial reference required to convert/transform WGS84 values to Projected CRS
        srs_latlon = osr.SpatialReference()
        srs_latlon.ImportFromEPSG(4326)
        srs_utm = osr.SpatialReference()
        srs_utm.ImportFromEPSG(32632)

        proj_point =osr.CoordinateTransformation(srs_latlon,srs_utm)
        point.Transform(proj_point)

        # buffer creation and buffer extent as output
        buffer_geom = point.Buffer(buffer_in_metres)

        buffer_extent = buffer_geom.GetEnvelope()
        
        return buffer_extent
        
    except Exception as e:
        logger.error(f"An error occured while computing the buffer extent: {e}")
    
        
def convert_buffer_extent(buffer_extent):
    """
    Description: The `convert_buffer_extent` function transforms coordinates from a given buffer extent in the UTM Zone 32 North coordinate reference system 
                 to latitude and longitude positions in the WGS84 standard. 
    
    Output: The resulting coordinates represent the minimum and maximum latitude and longitude values.
    """
    try:
        # Define the original and target coordinate reference systems
        original_crs = pyproj.CRS.from_epsg(32632)  # UTM Zone 32 North
        target_crs = pyproj.CRS.from_epsg(4326)    # WGS84, the standard for latitude and longitude
        
        # Create a transformer
        transformer = pyproj.Transformer.from_crs(original_crs, target_crs, always_xy=True)
        
        # Initialize x/y coordinates from Buffer-Extent
        ymin, ymax, xmin, xmax = buffer_extent
        
        # Perform the transformation
        long_min, lat_min = transformer.transform(xmin, ymin)
        long_max, lat_max = transformer.transform(xmax, ymax)
        
        return lat_min, lat_max, long_min, long_max
    
    except Exception as e:
        logger.error(f"An error occured while converting the buffer extent to lat/long positions: {e}")
    