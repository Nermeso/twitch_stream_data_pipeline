import os
import requests
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import json
import time

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


# Gets user ids that we will potentially call the API to get data on
def get_potential_new_users(s3_client, day_date_id, time_of_day_id):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key=f"curated_streams_data/{day_date_id}/curated_streams_data_{day_date_id}_{time_of_day_id}.csv")
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


# Reads current user dimension data to get users we have data for already
def get_current_user_dim(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key=f"curated_users_data/curated_users_data.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated users data. Status - {status}")
            current_user_dim_df = pd.read_csv(response.get("Body"), index_col=False)
            current_users = list(set(current_user_dim_df["user_id"].tolist()))
        else:
            print(f"Unsuccessful S3 get_object response for the curated streams data. Status - {status}")  
            exit()  
    except Exception as e: # if curated user data does not exist yet, error will be returned which we will catch
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

    headers, s3_client = get_credentials()
    day_date_id = get_day_date_id(s3_client)
    time_of_day_id = get_time_of_day_id(s3_client)

    day_date_id = "20260105" # test value
    time_of_day_id = "1130" # test value

    # Gets user IDs from recently collected stream data
    stream_user_list = get_potential_new_users(s3_client, day_date_id, time_of_day_id)

    # Gets user IDs from user dimension
    current_user_list = get_current_user_dim(s3_client)
    
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
