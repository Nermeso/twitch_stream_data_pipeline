import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
import boto3
import awswrangler as wr
import json


###################################### SUMMARY ######################################
'''
    This script collects information on twitch broadcasters by calling the Twitch API.
    It runs every 15 minutes and does so 7 minutes after the most recent collection
    of stream data.
'''
#####################################################################################


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Gets date id associated with most recently collected stream data
def get_date_id(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/date_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for the date dimension. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    current_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    current_date = current_date - timedelta(minutes=7) # Subtract 7 minutes from current time since that time will be associated with most recently collected stream data, this script runs 7 minutes after recent stream data collection
    date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
   
    return str(date_id)


# Gets time key associated with most recently collected stream data
def get_time_key(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/time_of_day_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for the time dimension. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    cur_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    cur_date = cur_date - timedelta(minutes=7) # Subtract 7 minutes from current time since that time will be associated with most recently collected stream data, this script runs 7 minutes after recent stream data collection
    minimum_diff = 1000
    time_key = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(cur_date.year, cur_date.month, cur_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((cur_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_key = row[1]["time_of_day_id"]

    return time_key


# Get the users who we will get more info on through the API
def get_users(s3_client, date_id, time_key):
    users_list = []
    folder_path = f"raw/fact_table/{str(date_id)}_{str(time_key)}/"  
    response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix=folder_path)
    
    # Folder for most recently collected stream data exists, get user data from it
    # If nothing is there, return empty users list
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                data_path = obj["Key"]
                response2 = s3_client.get_object(Bucket="twitchdatapipelineproject", Key=data_path)
                status = response2["ResponseMetadata"]["HTTPStatusCode"]
                if status == 200:
                    df_tmp = pd.read_csv(response2.get("Body"))
                    new_users = df_tmp["user_id"].tolist()
                    users_list.extend(new_users)
                else:
                    print(f"Unsuccessful S3 get_object response. Status - {status}")

    return list(set(users_list))


# Gets current user dimension
# If does not exist, we create it
def get_current_user_dim(s3_client):
    user_dim_exist = False
    user_dim_path = ""
    response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix="raw/dimension_table/")

    # Check to see if user dimension exists currently
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith("user_dimension.csv"):
                user_dim_exist = True
                user_dim_path = obj["Key"]
    
    if user_dim_exist is False: # create user dimension if does not exist
        current_user_dim_df = pd.DataFrame(columns=["user_id", "user_name", "login_name", "broadcaster_type"])
        wr.s3.to_csv(current_user_dim_df, "s3://twitchdatapipelineproject/raw/dimension_table/user_dimension.csv", index=False)
        current_users = []
    else: # if exists, read it and get the current users
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key=user_dim_path)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the user dimension. Status - {status}")
            current_user_dim_df = pd.read_csv(response.get("Body"))
            current_users = current_user_dim_df["user_id"].tolist()

    return current_users, current_user_dim_df


# Update user list to only include users we do not have info on already
# in user dimension
def get_only_new_users(user_list, current_users):
    set1 = set(user_list)
    set2 = set(current_users)
    new_users = list(set1.difference(set2))

    return new_users


# Calls Twitch API to get additional info on users
def get_user_data(user_list):
    user_data_dict = {
        "user_id": [],
        "user_name": [],
        "login_name": [],
        "broadcaster_type": []
    }
    headers = get_credentials()

    # Iteratively loops over user list when calling API
    # API endpoint for getting users accepts max 100 users at a time
    for i in range(0, len(user_list), 100):
        user_list_tmp = user_list[i:i + 100]
        while True: # If rate limit reached, retry after 20 seconds
            params = {
                "id": user_list_tmp,
                "first": 100
            }
            response = requests.get("https://api.twitch.tv/helix/users", params=params, headers=headers)
            output = response.json()
            if response.status_code == 200:
                for user_info in output["data"]:
                    user_data_dict["user_id"].append(str(user_info["id"]))
                    user_data_dict["user_name"].append(user_info["display_name"])
                    user_data_dict["login_name"].append(user_info["login"])
                    if user_info["broadcaster_type"] == "":
                        user_data_dict["broadcaster_type"].append("normal") # make broadcaster type normal instead of empty string
                    else:
                        user_data_dict["broadcaster_type"].append(user_info["broadcaster_type"])
                break
            elif response.status_code == 429:
                print("Rate limit exceeded. Retrying in 20 seconds")
                time.sleep(20)
                continue
            else:
                print(f"Error: {response.status_code}")
                print(output)
                exit()
    
    return user_data_dict


def lambda_handler(event, context):
    start = time.time()

    s3_client = boto3.client('s3')
    date_id = get_date_id(s3_client)
    time_key = get_time_key(s3_client)
    user_list = get_users(s3_client, date_id, time_key) # get all users present in recently collected stream data

    # End program early if no new users to get data on
    if len(user_list) == 0:
        print("No new users to get data on")
        return {
            'statusCode': 200,
            'body': "No new users to get data on."
        }

    current_users, current_user_dim_df = get_current_user_dim(s3_client) # get current users we have data for
    new_users = get_only_new_users(user_list, current_users) # Returns user ids of users not in user dimension yet
    user_data_dict = get_user_data(new_users) # call api to get data for new users
    user_dim_df = pd.DataFrame(user_data_dict) # convert user data to dataframe
    new_df = pd.concat([current_user_dim_df, user_dim_df]).drop_duplicates()
    new_df["user_id"] = new_df["user_id"].astype(int) # prevents user_id from becoming float

    # Create temporary CSV of new user data to be uploaded to Postgres
    new_user_data_df = pd.DataFrame(user_data_dict)
    new_user_data_path = "s3://twitchdatapipelineproject/raw/other/new_data_temp/new_user_data.csv"
    wr.s3.to_csv(new_user_data_df, new_user_data_path, index=False) # Upload user data CSV to S3

    # Upload user data CSV to S3
    wr.s3.to_csv(new_df, "s3://twitchdatapipelineproject/raw/dimension_table/user_dimension.csv", index=False)

    event_payload = {
                        "table_name": "users",
                        "new_data_path": new_user_data_path
                    }

    # Invokes another lambda to upload data to postgres db
    lambdaClient = boto3.client('lambda')
    response = lambdaClient.invoke(
        FunctionName='arn:aws:lambda:us-west-1:484743883065:function:insertDatatoDB',
        InvocationType='Event',
        Payload=json.dumps(event_payload)
    )

    end = time.time()
    print("Duration: " + str(end - start))

    return {
        'statusCode': 200,
        'body': "Success"
    }



