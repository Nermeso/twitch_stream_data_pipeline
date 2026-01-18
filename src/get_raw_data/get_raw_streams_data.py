import os
import requests
import ast
import boto3
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd


###################################### SUMMARY #####################################
'''
    This script collects stream data for specified categories.
'''
####################################################################################

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


# Deletes messages in SQS queue so if lambda function fails, it will not be processed
def delete_SQS_messages(event):
    sqs_client = boto3.client("sqs")
    for message in event["Records"]:
        sqs_client.delete_message(
                QueueUrl="https://sqs.us-west-2.amazonaws.com/484743883065/category_groups",
                ReceiptHandle=message["receiptHandle"]
            )


# Calls Get Stream Twitch API to get stream data for specified categories
def get_data_from_API(raw_stream_data, category_set, headers):
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
            output = response.json()
            if response.status_code == 200:
                raw_stream_data["data"].extend(output["data"])
            elif response.status_code == 429:
                print("Rate limit exceeded. Retrying in 20 seconds")
                time.sleep(20)
                continue
            else:
                print(f"Error: {response.status_code}")
                print(output)
                exit()
        except Exception as e:
            print("An exception has occurred in the get_streams function!: " + str(e))
            exit()
        
        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "end"
        else:    
            cursor = output["pagination"]["cursor"]


def lambda_handler(event, context):
    if event:
        start = time.time()
        func_ID = context.aws_request_id
        headers, s3_client = get_credentials()
        categories_to_process = get_categories(event)
        day_date_id = event["Records"][0]["messageAttributes"]["day_date_id"]["stringValue"]
        time_of_day_id = event["Records"][0]["messageAttributes"]["time_of_day_id"]["stringValue"]
        delete_SQS_messages(event) # deletes messages from SQS queue

        raw_stream_data = {
            "day_date_id": day_date_id,
            "time_of_day_id": time_of_day_id,
            "data": []
        }

        # Calls Twitch's Get Streams API for every 100 categories since 100 is max
        # Counts as one API request, minimizing API request number to better adhere to rate limits    
        category_set = set()   
        for i, category_id in enumerate(categories_to_process):
            ith_category = i + 1
            category_set.add(category_id)
            # Process categories in batches of 100 while including the last non-100 batch
            if (ith_category % 100 == 0) or len(categories_to_process) == ith_category:
                get_data_from_API(raw_stream_data, category_set, headers)
                category_set = set()

        # Upload data as JSON to S3
        s3_client.put_object(
                Bucket="twitch-project-raw-layer",
                Key=f"raw_streams_data/{day_date_id}/{time_of_day_id}/raw_streams_data_{day_date_id}_{time_of_day_id}_{func_ID}.json",
                Body=json.dumps(raw_stream_data, indent=4),
                ContentType='application/json'
            )

        end = time.time()
        print("Duration: " + str(end - start))
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successful program end!')
        }
    
    return {
       'statusCode': 404,
       'body': json.dumps("No data found in event input.")
    }