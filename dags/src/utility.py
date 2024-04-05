##########################################################
###### UTILITY MODULE FOR AIRFLOW DAGs. ##################
## Description: This script contains functions utilized by the Airflow DAGs as python callables.
##
## Functions:
##     - fetch_payload: Function to retrieve data from an API source, sets the retrieved parameter as an input variable
##     - download_geodata: Function downloads the geo specific data related to the downstream DAG 
##     - clip: Function clips the downloaded geo-data using the inputs generated from the upstream DAG per requirements.
##     - load: Function to load the transformed data into a destination S3 bucket
##     - get_coordinates: Function retrieves latitude and longitude values using free Nominatim API
##     - generate_location_folder: Function generates subfolders and specific subfolders within
##     - get_location_specific_temps: Function extracts daily temperatures of a specified location within dataset
##     - get_daily_temp_extremes: Function extracts daily highest and lowest temperatures and their geolocation within dataset
##     - add_address_info: Function reverse-geocodes a csv file and appends new address details based on latitude and longitude
#################################################
## Project: BMI Thünen Institute Breedfides
## Date: 26.10.2023
## Status: prod/dev
#################################################
## Comments: This version of source code is not to expand Breedfides Airflow ETL infrastructure explicitly but a brief demonstration of its expandability in some aspects
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
import dask.dataframe as dd
import owslib.fes
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
import rasterio
from rasterio import plot, MemoryFile
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely.geometry import box, MultiPolygon
from dotenv import load_dotenv
from airflow.models import DagRun
import netCDF4
from netCDF4 import Dataset
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

### GLOBAL VARS
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
## current_dir = f"{os.getcwd()}/" ## Uncomment this and comment the next line if testing is on local-env (not prod server)
current_dir = os.path.expanduser(os.path.join("~", 'airflow-etl/'))

# Specify the path to the .env file
dotenv_path = os.path.join(current_dir, 'dags', 'src', '.env')

# Load environment variables from the .env file
load_dotenv(dotenv_path)


# Get today's date and time
date_now = datetime.now().strftime("%Y%m%d_%H%M")

### Initialize Spatial reference for User's Lat/Long coordinates (WGS84 coordinate system)
SRS_LATLON = osr.SpatialReference()
SRS_LATLON.ImportFromEPSG(4326)
SRS_UTM = osr.SpatialReference() ## Initialize spatial reference for a projected coordinate system UTM (Universal Transverse Mercator) Zone 32 North
SRS_UTM.ImportFromEPSG(32632)


def get_coordinates(kwargs):
    """
    Description: The `get_coordinates` function retrieves values of keys inside POST requests parameters. This function is to reduce redundancy as multiple downstream tasks
                 depend on user inputs. If latitude and longitude are provided inside POST requests, it calculates a rough address using reverse-search function of Nominatim API.
                 If input was a location, it will be geocoded in Nominatim backend and latitude and longitude values of that location are returned.

    Output: It returns either directly user-provided latitude and longitude inside POST requests plus reverse-searched address using these values
            or returns geocoded latitude and longitude values of a user given location plus its detailed address.
    """
    # Parse the input using ast.literal_eval()
    input_var = ast.literal_eval(kwargs['params']['input_attributes'])
    # Define geolocator with custom user agent
    geolocator = Nominatim(user_agent="breedfides_agent")

    if 'lat' in input_var and 'long' in input_var:
        # User-provided latitude and longitude
        latitude = float(input_var['lat'])
        longitude = float(input_var['long'])
        address = str(geolocator.reverse((latitude, longitude)))
    elif 'loc' in input_var:
        # User-provided location name
        loc = input_var['loc']
        # Use the Nominatim API to convert the location name to latitude and longitude
        location = geolocator.geocode(loc)
        latitude = location.latitude
        longitude = location.longitude
        address = location.address
    else:
        raise ValueError("Either 'lat' and 'long' or 'loc' should be provided.")

    return latitude, longitude, address


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


def get_most_recent_dag_run(dag_id):
    """
    Description:
        The `get_most_recent_dag_run` function returns the execution time of the previous DAG, this function is called by the ExternalDAGSensor operator

    Args:
        dag_id (_type_): Name of External DAG

    Returns:
        _type_: DateTime / None
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)

    if dag_runs:
        return dag_runs[0].execution_date
    else:
        return None

    
def download_geodata(**kwargs):
    """
    Description: The `download_geodata` function fetches the related geo/climate related data from independent sources (API or FTP), the retrieved data will
                 be clipped subsequently using the input attrbutes
    """
    input_var = ast.literal_eval(kwargs['params']['input_attributes'])
    dag_id = kwargs['dag'].dag_id ## Extracts DAG-ID from context object
    ftp_dir = kwargs['dag'].tags[-1] ## Extract Tags
    local_file_path = current_dir + ftp_dir
    
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
            output_dir_path = os.path.join(current_dir, 'wcs', f'{filename}.tif')
            with open(output_dir_path, 'wb') as file:
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
            output_dir_path = os.path.join(current_dir, 'wfs', f'{filename}.json')
            with open(output_dir_path, 'w') as json_file:
                json.dump(response, json_file)
                
        elif 'fetch_cdc' in dag_id:
            ftp = ftplib.FTP('opendata.dwd.de', timeout=3600)
            
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
    directory = current_dir + geo_tag
    
    try:
        latitude, longitude, buffer_in_metres = get_coordinates(kwargs)[0], get_coordinates(kwargs)[1], 3000 if 'buffer_in_metres' not in input_var else int(input_var['buffer_in_metres'])
        
        buffer_extent = compute_buffer_extent(longitude, latitude, buffer_in_metres)
        lat_min, lat_max, long_min, long_max = convert_buffer_extent(buffer_extent)
        
        dataset = xr.open_mfdataset(f'{directory}/*.nc', decode_times=True, 
                                    chunks={'time': 1000000, 'latitude':1, 'longitude':1}, engine='netcdf4', 
                                    data_vars='minimal', coords='minimal', compat='override', parallel=True)
        
        # Clip the Array based on the converted buffer extent and set a boolean mask
        mask_lon = (dataset.lon >= long_min) & (dataset.lon <= long_max)
        mask_lat = (dataset.lat >= lat_min) & (dataset.lat <= lat_max)

        mask_lon = mask_lon.compute()
        mask_lat = mask_lat.compute()
        
        # Apply the masks to subset the dataset
        subset_ds = dataset.where(mask_lon & mask_lat, drop=True)
          
        # Write clipped output as netCDF
        output_path = os.path.join(current_dir, 'output', geo_tag, f'{geo_tag}_{date_now}.nc')
        logger.info(f"Writing clipped data to {output_path}")
        subset_ds.to_netcdf(output_path, format='netcdf4', compute=False)
        
    except Exception as e:
        logger.error(f"An error occured while clipping the GeoNetwork data: {e}")
        raise 



def clip_soil_data(**kwargs):
    """
    Description: The `clip_soil_data` function utilizes the input attributes (the lattitude and longitude), creates a buffer-extent using both coordinates and a buffer,
                 it then converts the buffer-extent to min/max lat-long coordinates which in turn uses these attributes to clip the downloaded soil geopackage data
    """
    input_var, geo_tag = ast.literal_eval(kwargs['params']['input_attributes']), kwargs['dag'].tags[-1]
    directory = os.path.join(current_dir, geo_tag, 'BUEK_data.gpkg')
    
    try:
        latitude, longitude, buffer_in_metres = get_coordinates(kwargs)[0], get_coordinates(kwargs)[1], 3000 if 'buffer_in_metres' not in input_var else int(input_var['buffer_in_metres'])

        gdf = gpd.read_file(directory, engine='pyogrio', use_arrow=True)
        buffer_extent = compute_buffer_extent(longitude, latitude, buffer_in_metres)
        miny, maxy, minx, maxx = convert_buffer_extent(buffer_extent)

        ## Transform crs from EPSG:25832 (default CRS) to EPSG:4326 (Lat/Long coordinate system)
        gdf = gdf.to_crs('EPSG:4326')

        ## Assign buffer-extents to bounding-box and initialize bbox as MultiPolgons
        bbox = box(minx, miny, maxx, maxy)
        multi_bbox = MultiPolygon([bbox])

        # Create a GeoDataFrame for the bounding box, to be used later for clipping
        bbox_gdf = gpd.GeoDataFrame(geometry=[multi_bbox], crs=gdf.crs)

        ## Clip the soil geo file based on the bounding box buffer extent and set the crs back to its default value EPSG:25832")
        clipped_df = gpd.clip(gdf, bbox_gdf)
        clipped_df = clipped_df.to_crs('EPSG:25832')

        # Write clipped output as .gpkg
        output_path = os.path.join(current_dir, 'output', geo_tag, f'{geo_tag}_{date_now}.gpkg')
        logger.info(f"Writing clipped data to {output_path}")
        clipped_df.to_file(output_path, driver='GPKG')
        
    except Exception as e:
        logger.error(f"An error occured while clipping the GeoPackage data: {e}")
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

        ## Transform point from the WGS84 (latitude and longitude) coordinate system (SRS_LATLON) to the UTM Zone 32 North coordinate system (SRS_UTM)
        proj_point = osr.CoordinateTransformation(SRS_LATLON, SRS_UTM)
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
        # Convert UTM coordinates back to latitude and longitude
        proj_extent_to_latlon = osr.CoordinateTransformation(SRS_UTM, SRS_LATLON)
        
        # Perform the transformation
        long_min, lat_min, _ = proj_extent_to_latlon.TransformPoint(buffer_extent[0], buffer_extent[2])
        long_max, lat_max, _ = proj_extent_to_latlon.TransformPoint(buffer_extent[1], buffer_extent[3])
        
        return lat_min, lat_max, long_min, long_max
    
    except Exception as e:
        logger.error(f"An error occured while converting the buffer extent to lat/long positions: {e}")


def generate_location_folder(subfolder_name, location_name):
    """
    Description: The `generate_location_folder` function generates a subfolder in 'output' folder and another subfolder within if second parameter is provided.
                 Its main purpose is seperating content that being calculated from other functions. Since it is a help function of a location specific function
                 it is mostly used to generate a folder with the name of that specific location.

    Output: It returns the path to (location specific) generated folder
    """
    # Create a folder for the location if it doesn't exist
    subfolder = subfolder_name.replace(" ", "_")  # Replace spaces with underscores
    if location_name is None:
        location_directory = os.path.join("output", subfolder)
    else:
        location_name = location_name.replace(" ", "_")
        location_directory = os.path.join("output", subfolder, location_name)
    if not os.path.exists(location_directory):
        os.makedirs(location_directory)
    return location_directory


def get_location_specific_temps(**kwargs):
    """
    Description: The `get_location_specific_temps` function calculates daily average temperatures of a given location within a provided dataset (TT_daymean datasets of DWD in this case). 
                 For that, it first iterates through availabe NetCDF files and calculates how many days, months, years there are to adjust the size of the dataframe.
                 !Specific to these datasets:!
                 Since latitude and longitude arrays are 2D. There are 938 x 720 = 675360 possible assignable temperature values, which not all of them are covered by the grid.
                 For an easy approach to extract temp values, 2D arrays are converted to 1D arrays, in which latitude and longitude values distributed evenly on 938 x 720 sized grid.
                 To get best results, user given latitude and longitude values are approximated to closest points possible on grid. Then index values of arrays are being found.
                 These indexes determinates the temperature value, hence closest temp values to the user-given location. All collected dataframe then written into csv files and a plot
                 of that dataframe, that named after their specific locations, are then being saved in ther corresponding folders.
                 There are some commented print lines for debugging purposes.

    Output: A csv file that contains daily temps and their corresponding date and a plot of that data in png format that named after a specific location 
    """
    geo_tag = kwargs['dag'].tags[-1]
    directory = current_dir + geo_tag

    try:
        latitude, longitude, address = get_coordinates(kwargs)
        # Iterate through NetCDF files and record all the years of the netCDF files into a Python list
        all_years = []
        nc_files = glob.glob(os.path.join(directory, "*.nc"))
        for file in nc_files:
            data = Dataset(file, 'r')
            time = data.variables['time']
            year = time.units[12:16]
            all_years.append(year)

        # Creating an empty Pandas DataFrame covering the whole range of data 
        year_start = min(all_years) 
        end_year = max(all_years)
        date_range = pd.date_range(start = str(year_start) + '-01-01', 
                                end = str(end_year) + '-12-31', 
                                freq = 'D')

        df = pd.DataFrame(0.0, columns=['Temperature'], index=date_range)
        df.index.name = 'Date'

        # Defining the lat, lon for the location of your interest
        lat_location = latitude
        lon_location = longitude

        #print('Getting Temperature Data of: '+ address)

        location_name = address.split(',', 1)[0]

        # Sorting the all_years python list
        all_years.sort()

        for year in all_years:
            for month in range(1, 13):
                filename = f"TT_{year}{str(month).zfill(2)}_daymean.nc"
                if os.path.join(directory, filename) in nc_files:
                    # Reading-in the data 
                    data = Dataset(os.path.join(directory, filename), 'r')
                    
                    # Storing the lat and lon data of the netCDF file into variables 
                    lat = data.variables['lat'][:]
                    lon = data.variables['lon'][:]

                    # Convert 2D Arrays to 1D Arrays
                    flat_lat_array = lat.reshape(-1)            
                    lat_indices = np.linspace(0, len(flat_lat_array) - 1, len(lat), dtype=int)
                    lat_subset = np.array([flat_lat_array[i] for i in lat_indices])
                    lon_subset = lon[len(lon)-1]

                    # Identify the index of the min value for lat and lon
                    min_index_lat = np.abs(lat_subset - lat_location).argmin()
                    min_index_lon = np.abs(lon_subset - lon_location).argmin()
                    
                    # Accessing the average temperature data
                    temp = data.variables['temperature']
                    
                    # Creating the date range for each month during each iteration
                    start = f"{year}-{str(month).zfill(2)}-01"
                    end = f"{year}-{str(month).zfill(2)}-{pd.Period(start).days_in_month}"
                    d_range = pd.date_range(start=start, end=end, freq='D')
                    
                    for t_index, date in enumerate(d_range):
                        #print('Recording the value for: ' + str(d_range[t_index]))
                        df.loc[date]['Temperature'] = float("{:.1f}".format(temp[t_index, min_index_lat, min_index_lon]))

        # Write data to a CSV file inside the location folder
        df.to_csv(os.path.join(generate_location_folder("air_temperature_mean_location_specific", location_name), "{}_data.csv".format(location_name.replace(" ", "_"))))
        df.plot()
        plt.savefig(os.path.join(generate_location_folder("air_temperature_mean_location_specific", location_name), "{}_plot.png".format(location_name.replace(" ", "_"))))

    except Exception as e:
        logger.error(f"An error occured while getting data at location: {e}")
        raise 


def get_daily_temp_extremes(**kwargs):
    """
    Description: The `get_daily_temp_extremes` function extracts the latitude and longitude of points, where temps are highest and lowest daily within available
                 dataset. This function is written specifically for DWD TT_daymean datasets as it iterates through datasets and collects data from them.
                 Collected datas are then written in corresponding csv files within a specified folder in 'output' folder

    Output: Two csv files for daily highest and lowest temperatures and their geolocation
    """
    geo_tag = kwargs['dag'].tags[-1]
    directory = current_dir + geo_tag
    
    # Define the CSV file name
    csv_max_file = 'daily_temp_extremes_max.csv'
    csv_min_file = 'daily_temp_extremes_min.csv'

    # Initialize lists to store the data
    data_max = []
    data_min = []

    # Loop through each netCDF file in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith('_daymean.nc'):
            # Parse the file name to get the year and month
            year = int(file_name[3:7])
            month = int(file_name[7:9])

            # Open the netCDF file
            file_path = os.path.join(directory, file_name)
            file = netCDF4.Dataset(file_path)

            # Get the temperature variable
            temperature = file.variables['temperature']

            # Get the latitude and longitude variables
            lat = file.variables['lat']
            lon = file.variables['lon']

            # Get the time variable
            time = file.variables['time']

            # Loop through each day in the dataset
            for i in range(time.size):
                # Extract the temperature data for the current day
                day_temperature = temperature[i,:,:]

                # Find the maximum and minimum temperature values and their indices
                max_temp = np.nanmax(day_temperature)
                max_temp_index = np.unravel_index(np.argmax(day_temperature), day_temperature.shape)
                min_temp = np.nanmin(day_temperature)
                min_temp_index = np.unravel_index(np.argmin(day_temperature), day_temperature.shape)

                # Get the latitude and longitude values corresponding to the temperature values
                max_temp_lat = lat[max_temp_index[0], max_temp_index[1]]
                max_temp_lon = lon[max_temp_index[0], max_temp_index[1]]
                min_temp_lat = lat[min_temp_index[0], min_temp_index[1]]
                min_temp_lon = lon[min_temp_index[0], min_temp_index[1]]
                
                day_time = i + 1

                #print(f'day: {day_time}, maximum temperature value: {max_temp:.2f} degC, latitude: {max_temp_lat:.2f} degN, longitude: {max_temp_lon:.2f} degE')
                #print(f'day: {day_time}, minimum temperature value: {min_temp:.2f} degC, latitude: {min_temp_lat:.2f} degN, longitude: {min_temp_lon:.2f} degE')

                # Add the data to the lists
                data_max.append((year, month, day_time, float("{:.1f}".format(max_temp)), max_temp_lat, max_temp_lon))
                data_min.append((year, month, day_time, float("{:.1f}".format(min_temp)), min_temp_lat, min_temp_lon))

    # Convert the data lists to pandas DataFrames
    df_max = pd.DataFrame(data_max, columns=['year', 'month', 'day', 'max_temperature', 'latitude', 'longitude'])
    df_min = pd.DataFrame(data_min, columns=['year', 'month', 'day', 'min_temperature', 'latitude', 'longitude'])

    # Sort the data by year, month, and day
    df_max['day'] = pd.to_numeric(df_max['day'])
    df_min['day'] = pd.to_numeric(df_min['day'])
    df_max.sort_values(['year', 'month', 'day'], inplace=True)
    df_min.sort_values(['year', 'month', 'day'], inplace=True)

    subfolder_name = 'air_temperature_mean_daily_extremes'
    # Save the DataFrame to a CSV file
    df_max.to_csv(os.path.join(generate_location_folder(subfolder_name, None), csv_max_file), index=False)
    df_min.to_csv(os.path.join(generate_location_folder(subfolder_name, None), csv_min_file), index=False)


def add_address_info():
    """
    Description: The `add_address_info` iterates through all csv files in a folder and looks specifically for 'latitude' and 'longitude' columns in those csv files.
                 For each row, it reverse geocodes each latitude longitude couples and appends new columns with address details and places them into each row on
                 their corresponding columns. It overwrites the current csv file with new address information.

    Output: A csv file with new address details that overwrites pre-processed csv file if it has latitude and longitude information
    """
    directory = os.path.join(current_dir, 'output', 'air_temperature_mean_daily_extremes')

    # Initialize geocoder
    geolocator = Nominatim(user_agent="reverse_geocode")

    # Loop through each CSV file in the directory
    for file_name in os.listdir(directory):
        if file_name.endswith('.csv'):
            csv_file = os.path.join(directory, file_name)
            # Read CSV file
            df = pd.read_csv(csv_file)
            print(f'Reading {csv_file}')
            
            # Initialize new columns
            df['address'] = ''
            df['state'] = ''
            df['postcode'] = ''
            df['country'] = ''

            # Perform reverse geocoding for each row
            for index, row in df.iterrows():
                latitude = row['latitude']  # Modify 'latitude' to match the column name in your CSV
                longitude = row['longitude']  # Modify 'longitude' to match the column name in your CSV
                try:
                    print(f'Processing Row {index + 2} Getting address at location {latitude} {longitude}')
                    location = geolocator.reverse((latitude, longitude), timeout=10)
                    address = location.address
                    address_parts = location.raw.get('address', {})
                    state = address_parts.get('state', '')
                    postcode = address_parts.get('postcode', '')
                    country = address_parts.get('country', '')
                    df.at[index, 'address'] = address
                    df.at[index, 'state'] = state
                    df.at[index, 'postcode'] = postcode
                    df.at[index, 'country'] = country
                except GeocoderTimedOut:
                    logger.error("Geocoding service timed out for coordinates:", latitude, longitude)
                    raise

            # Save the updated DataFrame back to the CSV file
            df.to_csv(csv_file, index=False)
            