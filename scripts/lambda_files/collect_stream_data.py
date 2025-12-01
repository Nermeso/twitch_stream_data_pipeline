import os
import requests
import ast
import boto3
import time
import awswrangler as wr
from requests.exceptions import ConnectionError
from datetime import datetime
import pandas as pd


###################################### SUMMARY #####################################
'''
    This script collects stream data for specified categories.
'''
####################################################################################


# Gets current date id based of date when script is executed
def get_date_id(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/date_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    current_date = datetime.today()
    date_id = date_df[date_df["OurDate"] == str(current_date.date())].iloc[0, 0]
   
    return str(date_id)


# Gets time key based off of current time of script execution
def get_time_key(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/time_of_day_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    cur_date = datetime.today()
    minimum_diff = 1000
    time_key = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(cur_date.year, cur_date.month, cur_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((cur_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_key = row[1]["time_key"]

    return time_key


# Calls Get Stream Twitch API to get stream data for specified categories
def get_data_from_API(stream_data_dict, category_set, headers):
    cursor = ""
    while cursor != "end":
        params = {
            "game_id": list(category_set),
            "first": 100,
            "after": cursor
        }
        # Calls API to get data for 100 categories
        try:
            response = requests.get("https://api.twitch.tv/helix/streams", headers=headers, params=params)
        except Exception as e:
            print("An exception has occurred in the get_streams function!")
            print(e)
            print()
            exit()
        output = response.json()
        for stream in output["data"]:
            # Adds stream data to stream data dictionary
            if stream["id"] not in stream_data_dict["stream_id"]:
                stream_data_dict["stream_id"].append(stream["id"])
                stream_data_dict["user_id"].append(stream["user_id"])
                stream_data_dict["category_id"].append(stream["game_id"])
                stream_data_dict["category_name"].append(stream["game_name"])
                stream_data_dict["viewer_count"].append(stream["viewer_count"])
                stream_data_dict["language_id"].append(stream["language"])
                stream_data_dict["user_name"].append(stream["user_name"])

        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "end"
        else:    
            cursor = output["pagination"]["cursor"]


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }
    s3_client = boto3.client('s3')

    return headers, s3_client


# Gets all categories from messages and puts it in one list
def get_categories(event):
    categories_to_process = []
    for message in event["Records"]:
                category_list = ast.literal_eval(message["body"])
                categories_to_process.extend(category_list)

    return set(categories_to_process)


# Adds date and time values to data dictionary
def add_date_time_data(s3_client, stream_data_dict):
    cur_date_id= get_date_id(s3_client) # get date id using date dimension
    cur_time_key = get_time_key(s3_client) # gets time key
    num_of_streams = len(stream_data_dict["stream_id"])
    stream_data_dict["date_day_id"].extend(num_of_streams * [cur_date_id])
    stream_data_dict["time_of_day_id"].extend(num_of_streams * [cur_time_key])

    return cur_date_id, cur_time_key


def lambda_handler(event, context):
    if event:
        start = time.time()
        func_ID = context.aws_request_id
        headers, s3_client = get_credentials()
        categories_to_process = get_categories(event)
        stream_data_dict = {
            "stream_id": [],
            "date_day_id": [],
            "time_of_day_id": [],
            "user_id": [],
            "category_id": [],
            "category_name": [],
            "viewer_count": [],
            "language_id": [],
            "user_name": []
        }

        # Delete messages so even if lambda function fails, it will not be processed
        sqs_client = boto3.client("sqs")
        for message in event["Records"]:
            sqs_client.delete_message(
                    QueueUrl="https://sqs.us-west-1.amazonaws.com/484743883065/categoryGroupWeights",
                    ReceiptHandle=message["receiptHandle"]
                )

        # Calls Twitch's Get Streams API for every 100 categories since 100 is max
        # Counts as one API request, minimizing API request number to better adhere to rate limits    
        category_set = set()   
        for i, category_id in enumerate(categories_to_process):
            ith_category = i + 1
            category_set.add(category_id)
            # Process categories in batches of 100 while including the last non-100 batch
            if (ith_category % 100 == 0) or len(categories_to_process) == ith_category:
                get_data_from_API(stream_data_dict, category_set, headers)
                category_set = set()

        # Add date and time values to data
        date_id, time_key = add_date_time_data(s3_client, stream_data_dict)

        # Convert stream dict to dataframe
        stream_df = pd.DataFrame(stream_data_dict).drop_duplicates()

        # Group categories to get number of streamers for each one
        # Group categories to get number of streamers for each one
        category_popularity_df = stream_df.groupby(["category_id"], as_index=False).agg(
                                        category_id=('category_id', 'first'),
                                        category_name=('category_name', 'first'),
                                        num_of_streamers=('stream_id', 'count')
                                   ).sort_values(by="num_of_streamers", ascending=False)
        
        # Convert dataframes to CSVs and upload to S3
        wr.s3.to_csv(stream_df, f"s3://twitchdatapipelineproject/raw/fact_table/{date_id}_{time_key}/stream_data_{date_id}_{time_key}_{func_ID}.csv", index=False)
        wr.s3.to_csv(category_popularity_df, f"s3://twitchdatapipelineproject/raw/recent_category_popularity_data/category_popularity_data_{date_id}_{time_key}_{func_ID}.csv", index=False)

      
        end = time.time()
        print("Duration: " + str(end - start))
       
