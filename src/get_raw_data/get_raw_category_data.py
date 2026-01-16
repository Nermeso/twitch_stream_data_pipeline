import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import boto3
import json
import time

################################ SUMMARY ################################
'''
    This script calls the "Get Top Games" Twitch endpoint to return
    currently streamed categories at the time of script execution. The
    output will be a JSON file containing category data. The goal is to get
    information on all categories that Twitch has available.
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
def get_day_date_id(s3_client, current_date):
    current_date = current_date.astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_day_dates_data/raw_day_dates_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for date dimension. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    else:
        print(f"Unsuccessful S3 get_object response for date dimension. Status - {status}")
        print(response)
        exit()
        
    # If later than 23:52, the day will be considered the next day
    if current_date.hour == 23 and current_date.minute > 52:
        current_date += timedelta(days=1)
        day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
    else:
        day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
   
    return str(day_date_id)


# Gets time of day id based off of current time of script execution
def get_time_of_day_id(s3_client, current_date):
    current_date = current_date.astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_time_of_day_data/raw_time_of_day_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for time of day dimension. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False, dtype={"time_of_day_id": str})
    else:
        print(f"Unsuccessful S3 get_object response for time of day dimension. Status - {status}")
        print(response)
        exit()

    # If later than 23:52, next nearest time will be 00:00
    if current_date.hour == 23 and current_date.minute > 52:
        return "0000"
    
    minimum_diff = 1000
    time_of_day_id = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(current_date.year, current_date.month, current_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((current_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_of_day_id = row[1]["time_of_day_id"]

    return str(time_of_day_id)


# Calls the "Get Top Games" Twitch endpoint to get data on currently streamed categories
# Returns raw API JSON output of categories 
def call_get_top_games_endpoint(headers, raw_category_data):
    # Twitch uses cursor-based pagination to show API results
    # Will need cursor value in one API call to get next set of results in other API call
    cursor = ""
    while cursor != "done":
        params = {
            "first": 100, # Can get max of 100 items in each API call
            "after": cursor
        }
        response = requests.get("https://api.twitch.tv/helix/games/top", headers=headers, params=params)
        output = response.json()
        
        if response.status_code == 200:
            raw_category_data["data"].extend(output["data"])
        elif response.status_code == 429:
            print("Rate limit exceeded. Retrying in 20 seconds")
            time.sleep(20)
            continue
        else:
            print(f"Error: {response.status_code}")
            print(output)
            exit()

        # Ends pagination of pages when done
        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "done"
        else:
            cursor = output["pagination"]["cursor"]


def lambda_handler(event, context):
    headers, s3_client = get_credentials() # gets access token and client id needed to call Twitch API and s3 client
    todays_date = datetime.today()
    day_date_id = get_day_date_id(s3_client, todays_date)
    time_of_day_id = get_time_of_day_id(s3_client, todays_date)

    # Calls Twitch's "Get Top Games" endpoint to get currently streamed categories and category data we have not collected yet
    raw_category_data = {
        "day_date_id": day_date_id,
        "time_of_day_id": time_of_day_id,
        "data": []
    }

    # Do API calling twice to make sure no category is skipped over
    for _ in range(2):
        # Calls API to get category data
        call_get_top_games_endpoint(headers, raw_category_data)
       
    # Upload data as JSON to S3
    s3_client.put_object(
            Bucket="twitch-project-raw-layer",
            Key=f"raw_categories_data/{day_date_id}/raw_categories_data_{day_date_id}_{time_of_day_id}.json",
            Body=json.dumps(raw_category_data, indent=4),
            ContentType='application/json'
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }



