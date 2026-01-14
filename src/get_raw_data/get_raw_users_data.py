import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import boto3
import json
import time
import ast

################################ SUMMARY ################################
'''
    This script calls the "Get Users" Twitch endpoint to return
    information on Twitch broadcasters and users. The users to collect
    data for is based on recently collected curated stream data. The 
    output will be a JSON file containing user data. The goal is to get 
    information on all users that Twitch has available. Data is not
    returned for users that are banned.
'''
#########################################################################



# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    s3_client = boto3.client('s3')
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers, s3_client


# Gets user ids that we will potentially call the API to get data on
def get_potential_new_users(s3_client, bucket_name, day_date_id, time_of_day_id):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=f"curated_streams_data/{day_date_id}/curated_streams_data_{day_date_id}_{time_of_day_id}.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated streams data. Status - {status}")
            stream_df = pd.read_csv(response.get("Body"), keep_default_na = False)
            user_list = list(set(stream_df["user_id"].tolist()))
        else:
            print(f"Unsuccessful S3 get_object response for the curated streams data. Status - {status}")  
            exit()  
    except Exception as e: # if curated user data does not exist yet, error will be returned which we will catch
        print(e)
        exit()

    return user_list


# Reads the CSV file containing current users we already have data for
# This is located in the miscellaneous bucket
def get_current_users(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-miscellaneous", Key="current_data/current_users.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the current users data. Status - {status}")
            current_user_df = pd.read_csv(response.get("Body"), index_col=False, dtype={"user_id": "string"})
            current_users = list(set(current_user_df["user_id"].tolist()))
        else:
            print(f"Unsuccessful S3 get_object response for the current users data. Status - {status}")  
            exit()  
    except Exception as e: # if current user data does not exist yet, error will be returned which we will catch
        current_users = []
    
    return current_users


# Calls Twitch's "Get Users" endpoint to get data on users
def get_data_from_API(user_list, raw_user_data, headers):
    # API endpoint for getting users accepts max 100 users at a time
    for i in range(0, len(user_list), 100):
        user_list_tmp = user_list[i:i + 100]
        params = {
            "id": user_list_tmp,
            "first": 100
        }
        response = requests.get("https://api.twitch.tv/helix/users", params=params, headers=headers)
        output = response.json()

        if response.status_code == 200:
            raw_user_data["data"].extend(output["data"])
        elif response.status_code == 429:
            print("Rate limit exceeded. Retrying in 20 seconds")
            time.sleep(20)
        else:
            print(f"Error: {response.status_code}")
            print(output)
            exit()   



def lambda_handler(event, context):
    start = time.time()
    event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
    curated_streams_bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
    curated_streams_key = event_notification["Records"][0]["s3"]["object"]["key"]
    day_date_id = curated_streams_key.split("/")[1]
    time_of_day_id = curated_streams_key.split("/")[2].split("_")[4][:4]

    headers, s3_client = get_credentials()

    # Gets user IDs from recently collected stream data
    stream_user_list = get_potential_new_users(s3_client, curated_streams_bucket_name, day_date_id, time_of_day_id)

    # Gets user IDs we have already collected data for
    current_user_list = get_current_users(s3_client)   
    
    # Gets only users that we have not collected data of yet
    set1 = set(stream_user_list)
    set2 = set(current_user_list)
    need_data_users_list = list(set1.difference(set2))

    raw_user_data = {
        "day_date_id": day_date_id,
        "time_of_day_id": time_of_day_id,
        "data": []
    }

    # Calls Twitch's "Get Users" endpoint to get data on users
    get_data_from_API(need_data_users_list, raw_user_data, headers)

    # Upload data as JSON to S3
    s3_client.put_object(
            Bucket="twitch-project-raw-layer",
            Key=f"raw_users_data/{day_date_id}/raw_users_data_{day_date_id}_{time_of_day_id}.json",
            Body=json.dumps(raw_user_data, indent=4),
            ContentType='application/json'
        )

    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }
