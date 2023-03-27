import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 

def game(data):
    game_list = []
    for row in data:
        
        game_id = row['id']
        game_follows = row['follows']
        game_hypes = row['hypes']
        game_name = row['name']
        game_total_rating = row['total_rating']
        game_total_rating_count = row['total_rating_count']
        game_url = row['url']
        
        game_element = {'id':game_id, 'follows':game_follows,  'hypes':game_hypes, 'name':game_name, 'rating':game_total_rating, 'rating_count':game_total_rating_count, 'url':game_url}
        game_list.append(game_element)
    return game_list



def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    Bucket = "igdbproject-itc"
    Key = "raw/toprocess/"
    
    
    igdb_data = []
    igdb_keys = []
    
    
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            igdb_data.append(jsonObject)
            igdb_keys.append(file_key)    
            
            
    for data in igdb_data:
        game_list = game(data)
    
    
        # GAME Transformations
        
        game_df = pd.DataFrame.from_dict(game_list)
        game_df = game_df.drop_duplicates(subset=['id'])
        game_key = "transformed/game/game_transformed_" + str(datetime.now()) + ".csv"
        game_buffer=StringIO()
        game_df.to_csv(game_buffer, index=False)
        game_content = game_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=game_key, Body=game_content)
    
        
    s3_resource = boto3.resource('s3')
    for key in igdb_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw/processed/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()
        
