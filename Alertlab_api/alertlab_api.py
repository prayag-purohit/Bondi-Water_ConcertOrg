import os
import sys
import boto3
import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import urllib.parse as urlparse
import io
import time
import streamlit as st
import logging
import pandas as pd
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Alertlab_api.aws_utils import get_s3_client_and_bucket_name, upload_log_to_s3, get_logger_and_log_stream

load_dotenv()  # Still needed for local development


TOKEN_KEY = 'token.txt'
logger, log_stream = get_logger_and_log_stream()

def secrets_file_exists():
    """Check if secrets.toml exists locally (to avoid Streamlit error)."""
    return Path('.streamlit/secrets.toml').is_file() or Path.home().joinpath('.streamlit/secrets.toml').is_file()

def get_secret(key):
    """Unified secrets loader for Streamlit Cloud or local .env."""
    if secrets_file_exists():
        try:
            value = st.secrets[key]
            logger.info(f"Loaded {key} from Streamlit secrets.")
            return value
        except KeyError:
            logger.warning(f"{key} not found in secrets.toml.")
    else:
        logger.info(f"Running locally. Fetching {key} from .env")
    
    return os.getenv(key)

#########################################################################################################################
# AUTHORIZATION FUNCTIONS 
HIDDEN_LOGIN_API = "https://www.alertaq.com/api/v4/login"
TOKEN_API = 'https://www.alertaq.com/api/v4/public/login'

def _get_credentials():
    """Fetch credentials from Streamlit secrets or .env fallback."""
    return {
        "user": get_secret("ALERTLABS_USER"),
        "password": get_secret("ALERTLABS_PASSWORD"),
        "client_secret": get_secret("ALERTLABS_CLIENT_SECRET"),
        "user_id": get_secret("ALERTLABS_USERID"),
    }

def _read_token_from_file():
    """
    Read the token and its creation date from token.txt stored in S3.
    Returns:
        tuple: token (str), date_updated (datetime)
    """
    try:
        s3, BUCKET_NAME = get_s3_client_and_bucket_name()
        response = s3.get_object(Bucket=BUCKET_NAME, Key=TOKEN_KEY)
        content = response['Body'].read().decode('utf-8').strip()
        token_part, date_part = content.split("date: ")
        token = token_part.split("token: ")[1].strip()
        date_str = date_part.strip()
        logger.info('Token read from S3 successfully.')
        return token, datetime.strptime(date_str, "%m/%d/%Y")
    except Exception as e:
        logger.error(f"Failed to read token from S3: {e}")
        return None, None
    
def _write_token_to_file(token):
    """
    Write the token and current date to token.txt and upload to S3.
    """
    try:
        s3, BUCKET_NAME = get_s3_client_and_bucket_name()
        today_str = datetime.now().strftime("%m/%d/%Y")
        content = f"token: {token}\ndate: {today_str}"
        s3.put_object(Bucket=BUCKET_NAME, Key=TOKEN_KEY, Body=content)
        logger.info(f"Token written to S3 at {datetime.now()}.")
    except Exception as e:
        logger.error(f"Failed to write token to S3: {e}")

#Token lifetime is 30 days.
def _generate_new_token():
    """Generate a new token using OAuth."""
    credentials = _get_credentials()
    
    body={
        'user': credentials["user"],
        'password': credentials["password"], 
        'tokenLifetime' : 2592000
    }

    try:
        response = requests.post(TOKEN_API, json=body, headers={"Content-Type": "application/json"})
        response.raise_for_status()  # Raise exception for HTTP errors
        if response.status_code == 201:
            token_data = response.json()
            logger.info("New Token generated successfully.")
            return token_data.get("token")
        else:
            logger.error("Failed to retrieve token")
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error generating new token: {e}")
        return None

def _generate_new_hidden_token():
    "special token obtained through fuzzed API"
    credentials = _get_credentials()
    

    body={
        'user': credentials["user"],
        'password': credentials["password"], 
    }
    try:
        response = requests.post(HIDDEN_LOGIN_API, data=body)
        if response.status_code == 201:
            token_data = response.json()
            hidden_token = token_data.get('access_token')
            return hidden_token
        else: 
            logger.error(f"Problem with hidden API response code")
    except requests.exceptions.RequestException as e: 
        logger.error(f"Error generating new hidden token: {e}")
        return None

def get_token(query_type="default"):
    """
    Get the current token, refreshing it if necessary.
    params = 'hidden_api'
    """
    if query_type == 'hidden_api':
        hidden_token = _generate_new_hidden_token()
        logger.info("Hidden token generated successfully.")
        return hidden_token

    if query_type == 'default':
        token, token_date = _read_token_from_file()
        if token and token_date:
            today = datetime.now()
            if (today - token_date).days <= 29:
                return token
    
    new_token = _generate_new_token()
    _write_token_to_file(new_token)
    return new_token
################################################################################################################################################################################################
#DATA EXTRACTION FUNCTIONS 

#Gets all the sensors 
def get_all_sensors(token):

    url = f"https://api.alertaq.com/api/v4/public/sensors"
    headers = {"token": token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch sensors: {response.text}")
    return response.json().get('dataModel', [])
 
# Works well. 
def get_locations(token):
    """Fetch all locations from the API."""
    url = "https://www.alertaq.com/api/v4/public/locations"
    headers = {"token": token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch locations: {response.text}")
    data = response.json()
    return data.get("dataModel", [])

#This is used to fetch sensorids but sends a bunch of interesting information. 
def get_sensoreventsatlocation(location_id, token):
    """Fetch all sensor events at a location."""
    #token = get_token()
    url = f"https://www.alertlabsdashboard.com/api/v3/dataModel/read/allSensorEventsAtLocation?locationID={location_id}"
    headers = {"token": token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch sensor events: {response.text}")
    return response.json()

#Change to v4 and add help comments for rate, and series. 
def _get_timeseries(sensor_id, start_date, end_date, rate="h", series="W", token=None):
    """Fetch timeseries data for a sensor."""
    if not token:
        token = get_token()
    if not sensor_id or not start_date or not end_date:
        raise ValueError("sensor_id, start_date, and end_date are required")
    url = f"https://www.alertaq.com/api/v4/public/timeseries?sensorID={sensor_id}&from={start_date}&to={end_date}&rate={rate}&series={series}"
    headers = {"token": token}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch timeseries: {response.text}")
    response_json = response.json()
    if response_json.get("error") != None:
        logger.info(f"Error in timeseries data: {response_json['error']}")
        return None
    values = response_json['dataModel'][sensor_id]
    df = pd.DataFrame(values, columns=['time', 'series'])
    df['Datetime'] = pd.to_datetime(df['time'], unit='ms') - timedelta(hours=4)
    return df

def get_list_timeseries(sensor_list, start_date="1720119038", end_date="1720205438", rate="h", series="water", token=None):
    time_series_list = []
    for sensor in sensor_list:
        time_series_data = _get_timeseries(sensor_id=sensor,
                                             start_date=start_date,
                                             end_date=end_date,
                                             rate=rate, 
                                             token=token
                                            )
        if time_series_data is not None:
            time_series_list.append(time_series_data)
    return time_series_list

#v2 would be deprecated soon. Make a copy of this function's output for future use. 
def get_property_detailsv2(location_id, authorization_header):
    location_ids_json = json.dumps(location_id)
    property_details_url = f"https://www.alertlabsdashboard.com/api/v2/locations/{location_id}/details"
    headers = {
        "authorization": authorization_header,
    }
    # Pass the locationIDs as a JSON string in the params
    params = {
        "locationIDs": location_ids_json,
    }
    response = requests.get(property_details_url, headers=headers, params=params)
    return response.json()

#v4 version of the property details function
def get_property_detailsv4(location_id):
    """
    Returns property details for a single property. Use it after the query to filter for queried location_id.
    """
    hidden_token = get_token('hidden_api')
    time.sleep(3)
    propertydetails_endpoint = "https://www.alertaq.com/api/v4/dataModel/read"
    headers = {
        "authorization": hidden_token,
        "Content-Type": "application/json; charset=utf-8"
    }

    body = {
        "locationsV2": {
            "fields": [
                "_id", "owner", "name", "addressType", "address", "ancestors", "street", "city",
                "province", "country", "timezone", "latitude", "longitude", "size", "age",
                "numOccupants", "occupantType", "postalCode", "waterBillingStartDate",
                "waterBillingEveryXMonths", "numberSuites", "unoccupiedSuites", "parentIDs",
                "users", "groupType", "nodeType", "commercialPropertyType", "buildingType",
                "numberFloors", "ancestors", "vacationMode", "notes", "hasNotes", "flags"
            ],
            "where": {
                "_id": {
                    "$eq": location_id  # Dynamically inserted
                }
            },
            "children": {
                "sensors": {
                    "fields": ["location_id", "_id"]
                }
            }
        }
    }

    body = json.dumps(body)

    response = requests.post(propertydetails_endpoint, headers=headers, data=body)
    return response.json()

# An alternative to this has to be found. Very important. This function is the basis to get the parent name which is later used in the dashboard. 
#This function is deprecated, we would only use self joins in v4 version
def get_only_parent_id(parent_id, authorization_header):
    location_ids_json = json.dumps(parent_id)
    url = f"https://www.alertlabsdashboard.com/api/v2/locations/{parent_id}"
    headers = {
        "authorization": authorization_header,
    }
    params = {
        "locationIDs": location_ids_json,
    }
    response = requests.get(url, headers=headers, params=params)
    if "friendlyName" in response.json().keys():
        return response.json()["friendlyName"]
    else:
        return "Error"
    
#Unsued
def get_water_costs(location_id, token=None):
    """
    Fetch water costs for a location.
    API Returns
    {
    "error": null,
    "dataModel": {
        "present": {
        "usage": 0,
        "cost": 0
    },
    "future": {
      "usage": 0,
      "cost": 0
    },
    "past": {
      "usage": 0,
      "cost": 0
    }
    """
    if not token:
        token = get_token()
    if not location_id:
        raise ValueError("location_id is required")
    url = f"https://www.alertaq.com/api/v4/public/locations/{location_id}/bills/water"
    headers = {"authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch water costs: {response.text}")
    return response.json()




#Testing
"""
def main():
    token = get_token()
    print("Token:", token)
    locations = get_locations(token)
    print("Locations:", locations)
    location_id = locations[0]["_id"]
    print("Location ID:", location_id)
    sensor_events = get_sensoreventsatlocation(location_id, token)
    print("Sensor events:", sensor_events)
    sensor_id = sensor_events["dataModel"][0]["sensors"][0]["_id"]
    print("Sensor ID:", sensor_id)
    start_date = "1720119038"
    end_date = "1720205438"
    timeseries = get_timeseries(sensor_id, start_date, end_date, token=token)
    print("Timeseries:", timeseries)
"""
def tests3_token_functions():
    """Test token retrieval, writing, and reading with logging."""
    #logger.info("Starting token function tests...")

    
    logger, log_stream = get_logger_and_log_stream()
    token = _read_token_from_file()
    print(token)
    logger.info("Logs successfully uploaded to S3 after test.")
    upload_log_to_s3(logger, log_stream)

#token = get_token('hidden_api')
#print(token)
# Run the test
#tests3_token_functions()
#test_property_details = get_property_detailsv4('1523554162884-1322')
#print(test_property_details)



#main()