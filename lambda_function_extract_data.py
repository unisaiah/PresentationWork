import json
import os
import requests
import igdb
from igdb.wrapper import IGDBWrapper
import boto3
from datetime import datetime



def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    r = requests.post("https://id.twitch.tv/oauth2/token?client_id={}&client_secret={}&grant_type=client_credentials".format(client_id,client_secret))
    access_token = json.loads(r._content)['access_token']
    wrapper = IGDBWrapper(client_id, access_token)
    
    # JSON API request - Can only extract 500 rows at a time so must re-run lambda function with changed offset (NOTE: some offsets have no data)
    byte_array = wrapper.api_request('games','fields follows, genres.name, hypes, name, platforms.name, total_rating, total_rating_count, url; where follows > 0 & hypes > 0 & total_rating > 0 & total_rating_count > 0; offset 0; limit 500;')
    
    #print(byte_array)
    
    # Decode UTF-8 bytes to Unicode, and convert single quotes 
    # to double quotes to make it valid JSON
    my_json = byte_array.decode('utf-8')
    #my_json = byte_array.decode('utf8')
    #print(my_json)
    #print('- ' * 20)

    data = json.loads(my_json)
    # s = json.dumps(data, indent=4, sort_keys=True)
    # print(s)
    
    client = boto3.client('s3')
    filename = "igdb_raw_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket="igdbproject-itc",
        Key="raw/toprocess/" + filename,
        Body=json.dumps(data)
        )