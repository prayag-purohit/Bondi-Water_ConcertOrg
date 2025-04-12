import pandas as pd
import os
from datetime import datetime, timedelta
import requests
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from Alertlab_api.alertlab_api import get_token, get_locations, get_property_detailsv4, get_sensoreventsatlocation, get_only_parent_id, get_all_sensors
from Alertlab_api.aws_utils import get_logger_and_log_stream

logger, log_stream = get_logger_and_log_stream()


def _clean_tombstone(tombstone_df):
    """
    Drop second location ID
    Drop if node_type_child == 'folder'
    if name_child = null then drop 
    if node_type_child = null but sensors not null then node_type_child = 'building'
    if node_type_child and sensors both null then drop the row
    if location_info & sensors both present name_parent = 'Other'
    if name_parent = 'Bob L's org then name_parent = 'Other' 
    """
    # Make copy to avoid SettingWithCopyWarning
    df = tombstone_df.copy() 
    
    # 2. Remove folder-type children
    df = df[(df['nodeType_child'] != 'folder') & (df['nodeType_child'] != 'org')] # works
    
    # 3. Remove rows with missing child names
    df = df.dropna(subset=['name_child'])
    
    # 4. Set node type to building if sensors exist
    df.loc[df['nodeType_child'].isna() & df['serialNumber'].notna(), 'nodeType_child'] = 'building' 
    
    # If sensors don't exist for a building drop rows
    df = df.dropna(subset=['_id', 'serialNumber'], how='all')
    
    # 6. Clean parent names
    df['name_parent'] = df['name_parent'].replace(
        "Bob Langlois's Org", 'Other', regex=False
    )
    # Set to Other when child location _id is present, at least one sensor is present, and the there is a missing parent name
    df.loc[df['_id_child'].notna() & df['serialNumber'].notna() & df['name_parent'].isna(), 'name_parent'] = 'Other' 
    renamed_columns = {'_id': 'sensor_ids', 
           'name' : 'sensor_names',
           'serialNumber': 'sensor_serialNumbers', 
           'friendlyType': 'sensor_friendlyType'}

    df.rename(columns=renamed_columns, inplace=True)
    return df.reset_index(drop=True)

def get_property_metadata(property_id):
    """
    Get property metadata from settings using hidden APIs from alertAQ platform.
    Returns number of suites, number of floors, property type, property age, and number of users

    Example:
    property_id = '5f5d6b4b4b0b6e001b7f7e9b'
    number_of_suites, number_of_floors, CommercialPropertyType, property_age, number_of_users = get_property_metadata(property_id)

    """
    property_details = get_property_detailsv4(property_id)
    try:
        number_of_suites = property_details['dataModel'][0]['numberSuites']
        if number_of_suites is None: 
            number_of_suites = 1
    except KeyError as e:
        number_of_suites = 1
        logger.error(f"KeyError: {e} - number_of_suites not found in property details")
        pass
    try:
        number_of_floors = property_details['dataModel'][0]['numberFloors']
    except KeyError as e:
        number_of_floors = 1
        logger.error(f"KeyError: {e} - number_of_floors not found in property details")
        pass
    try:
        CommercialPropertyType = property_details['dataModel'][0]['commercialPropertyType']
    except KeyError as e:
        CommercialPropertyType = 'Unknown'
        logger.error(f"KeyError: {e} - commercialPropertyType not found in property details")
        pass
    try:
        property_age = property_details['dataModel'][0]['age']
    except KeyError as e:
        property_age = 'Unknown'
        logger.error(f"KeyError: {e} - age not found in property details")
        pass
    try:
        number_of_users = len(property_details['dataModel'][0]['users'])
    except KeyError as e:
        number_of_users = 0
        logger.error(f"KeyError: {e} - users not found in property details")
        pass
    return number_of_suites, number_of_floors, CommercialPropertyType, property_age, number_of_users


def populate_client_data():
    token = get_token()
    locations = get_locations(token)
    location_df = pd.DataFrame(locations)

    sensors = get_all_sensors(token)
    sensors_df = pd.DataFrame(sensors)
    sensors_df = sensors_df[(sensors_df['friendlyType'] == 'Flowie-O') | (sensors_df['friendlyType'] == 'Flowie')]
    sensors_df= sensors_df.groupby(by='location_id', as_index=False)[['_id', 'name', 'serialNumber', 'friendlyType']].agg(list)

    # Get parent properties with self-join
    location_df = pd.merge(location_df, location_df, left_on='parentID', right_on='_id', suffixes=('_child', '_parent'), how='left')
    
    tombstone_df = pd.merge(location_df,sensors_df,left_on='_id_child', right_on='location_id', how='outer')
    tombstone_df = _clean_tombstone(tombstone_df)  
    return tombstone_df
    


#if __name__ == '__main__':
    #df = pd.read_csv('Client_data_processing/new_tombstone.csv')
    #test_property = df['_id_child'].iloc[1]
    #print(get_property_metadata(test_property))
    # _clean_tombstone()
    # _enrich_tombstone_with_property_details()
    # _get_parentproperties_and_sensors()