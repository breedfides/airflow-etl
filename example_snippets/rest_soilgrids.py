import requests
import json


def fetch_soilgrids(lat, lon):
    # params = kwargs['params'].get('input_attributes', kwargs['params'])
    # input_var, geo_tag = ast.literal_eval(params) if type(params) == str else params, kwargs['dag'].tags[-1] 
    # input_var['lat'], input_var['long']
    url = 'https://rest.isric.org/soilgrids/v2.0/properties/query?'
    payload = {'lon': lon, 'lat': lat}
    r = requests.get(url, params=payload)
    if r.status_code == 200:
        print(r.json())
    else:
        print(r.status_code)

