import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd


def album(data):

    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        ablum_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,
                         'album_name':album_name,
                         'ablum_release_date':ablum_release_date,
                         'album_total_tracks':album_total_tracks,
                         'album_url':album_url
                        }
        album_list.append(album_element)
    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, value in row.items():
            if key == 'track':
                for artist in (value['artists']):
                    artist_dict = {'artist_id':artist['id'],
                                   'artist_name': artist['name'],
                                   'external_url':artist['href']}
                    artist_list.append(artist_dict)
    return artist_list
    
    
def song(data):
    
   
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
        
    return song_list
     
     
        
def lambda_handler(event, context):
    # Create S3 object to interact with S3
    s3 = boto3.client('s3')    
    # Project's bucket name
    Bucket="sj-spotify-etl-project"  
    # Folder name
    Key="raw_data/to_be_processed/"        
    #List the objects in the bucket(files, metadata, config data etc)
    print(s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents'])  
    
    spotify_data = []
    spotify_keys = [] 
    
    # Store the data from JSON files into list - spotify_data 
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:   # Each file in the S3 folder
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            # Create JSON object of the contents of the file
            jsonObject = json.loads(content.read())
            # Append the JSON data to the spotify_data list
            spotify_data.append(jsonObject)
            # Append the file name(file_key) to the list of file names(spotify_keys)
            spotify_keys.append(file_key)
            

            
    for data in spotify_data:
        
        # Create lists using above functions
        album_list = album(data)
        artist_list = artist(data)
        song_list = song(data)
        
        # Convert lists to pandas dataframes for use in data transformation
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame.from_dict(artist_list)
        song_df = pd.DataFrame.from_dict(song_list)
        
        # Data transformation - remove unique id duplicates
        album_df = album_df.drop_duplicates(subset=['album_id'])
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        song_df = song_df.drop_duplicates(subset=['song_id'])
        
        # Data transformation - standardize dates information to datetime format
        album_df['release_date'] = pd.to_datetime(album_df['ablum_release_date'])
        song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
        
        # Convert all dataframes back to csvs and load them in S3 bucket's target folders
        
        song_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer,index = False)
        song_content=song_buffer.getvalue()
        # Put the data into the song_key named file into the S3
        s3.put_object(Bucket=Bucket,Key=song_key, Body=song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=album_key, Body=album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index = False)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key=artist_key, Body=artist_content)
        
        # Move the source files from source folder to a different folder, and empty the source folder
    s3_resource = boto3.resource('s3')
    # print("------------------------")
    # print(spotify_keys)
    for key1 in spotify_keys:
        # print(key1)
        copy_source = {
            'Bucket': Bucket,
            'Key': key1
        }
        
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key1.split("/")[-1])
        # Delete the file from the source folder - to_be_processed
        s3_resource.Object(Bucket,key1).delete()
    # print(spotify_keys)   
    # print("------------------------")