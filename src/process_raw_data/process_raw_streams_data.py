import os
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import awswrangler as wr
import json
import time

######################## SUMMARY ########################
'''
    Converts raw streams json data to a CSV and uploads
    it to the processed layer S3 bucket.
'''
#########################################################


# Gets current date id based of date when script is executed
def get_day_date_id(s3_client):
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_day_dates_data/raw_day_dates_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for date dimension. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    else:
        print(f"Unsuccessful S3 get_object response for date dimension. Status - {status}")
        print(response)
        exit()
    current_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
   
    return str(day_date_id)


# Gets time of day id based off of current time of script execution
def get_time_of_day_id(s3_client):
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_time_of_day_data/raw_time_of_day_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for time of day dimension. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False, dtype={"time_of_day_id": str})
    else:
        print(f"Unsuccessful S3 get_object response for time of day dimension. Status - {status}")
        print(response)
        exit()
    cur_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    minimum_diff = 1000
    time_of_day_id = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(cur_date.year, cur_date.month, cur_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((cur_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_of_day_id = row[1]["time_of_day_id"]

    return time_of_day_id


# Gets the S3 object paths to most recently collected stream data
def get_stream_data_paths(s3_client, day_date_id, time_of_day_id):
    data_paths = []
    response = s3_client.list_objects_v2(Bucket="twitch-project-raw-layer", Delimiter='/', Prefix=f"raw_streams_data/{day_date_id}/{time_of_day_id}/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".json"):
                data_paths.append(obj["Key"])

    return data_paths


# Converts the raw stream data in JSON format to a dataframe
# Removes some data since it wouldn't fit in tabular format
def process_raw_stream_data(raw_stream_data, processed_stream_data_dict):
    for stream in raw_stream_data["data"]:
        processed_stream_data_dict["id"].append(stream["id"])
        processed_stream_data_dict["user_id"].append(stream["user_id"])
        processed_stream_data_dict["user_login"].append(stream["user_login"])
        processed_stream_data_dict["user_name"].append(stream["user_name"])
        processed_stream_data_dict["game_id"].append(stream["game_id"])
        processed_stream_data_dict["game_name"].append(stream["game_name"])
        processed_stream_data_dict["title"].append(stream["title"])
        processed_stream_data_dict["viewer_count"].append(stream["viewer_count"])
        processed_stream_data_dict["started_at"].append(stream["started_at"])
        processed_stream_data_dict["language"].append(stream["language"])
        processed_stream_data_dict["thumbnail_url"].append(stream["thumbnail_url"])
        processed_stream_data_dict["is_mature"].append(stream["is_mature"])



def lambda_handler(event, context):
    start = time.time()
    s3_client = boto3.client("s3")
    day_date_id = get_day_date_id(s3_client)
    time_of_day_id = get_time_of_day_id(s3_client)

    processed_stream_data_dict = {
            "id": [],
            "user_id": [],
            "user_login": [],
            "user_name": [],
            "game_id": [],
            "game_name": [],
            "title": [],
            "viewer_count": [],
            "started_at": [],
            "language": [],
            "thumbnail_url": [],
            "is_mature": []
        }
    
    # Get paths of JSON files of most recently collected stream data
    stream_data_paths = get_stream_data_paths(s3_client, day_date_id, time_of_day_id)

    # Process raw stream data
    if len(stream_data_paths) != 0:
        for path in stream_data_paths:
            response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key=path)
            status = response["ResponseMetadata"]["HTTPStatusCode"]
            if status == 200:
                raw_stream_data = json.loads(response["Body"].read().decode("utf-8"))
                process_raw_stream_data(raw_stream_data, processed_stream_data_dict)    
            else:
                print(f"Error: {status}")
                print("Unable to retrieve one of the stream files. Ending program.")
                exit()
        
        # Drop duplicate streams
        processed_stream_df = pd.DataFrame(processed_stream_data_dict).drop_duplicates(subset=["id"], keep="first")

        # Upload CSV to processed layer
        processed_stream_file_path = f"s3://twitch-project-processed-layer/processed_streams_data/{day_date_id}/processed_streams_data_{day_date_id}_{time_of_day_id}.csv"
        wr.s3.to_csv(
            df=processed_stream_df,
            path=processed_stream_file_path,
            index=False
        )

        end = time.time()
        print("Duration: " + str(end - start))

        return {
            'statusCode': 200,
            'body': json.dumps('Successful program end!')
        }

    else:
        return {
            'statusCode': 200,
            'body': json.dumps('No new stream data to process. Ending program.')
        }
