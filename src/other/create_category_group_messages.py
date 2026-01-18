import pandas as pd
import time
import numpy as np
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo
import botocore
import json
import ast


######################### SUMMARY #########################
'''
    Outputs to SQS messages that contains a group of
    category IDs. Each group should be approximately 
    equal to each other in terms of the number of 
    associated channels streaming. The size of each
    group and the associated categories is either based
    off of default weights or the most recently collected
    stream data.
'''
###########################################################


# Gets most recently made processed categories df to be used as current streamed categories
def get_processed_categories(s3_client, bucket_name, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the processed category data. Status - {status}")
            processed_categories_df = pd.read_csv(response.get("Body"), keep_default_na = False)           
    except Exception as e: # if processed category data does not exist, error will be returned which we will catch
        print(e)
        print("Unsuccessful S3 get_object response for the processed category data.")
        print(response)
        exit()
    
    return processed_categories_df


# Gets the default category popularity data that contains default weights for each category
def get_default_popularity_df(s3_client):
    try:
        object_key = f"category_popularity_data/default_category_weights.csv"
        response = s3_client.get_object(Bucket="twitch-project-miscellaneous", Key=object_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the default popularity category data. Status - {status}")
            default_pop_df = pd.read_csv(response.get("Body"), keep_default_na = False)           
    except Exception as e: # if processed category data does not exist, error will be returned which we will catch
        print(e)
        print("Unsuccessful S3 get_object response for the default popularity category data.")
        print(response)
        exit()
    
    return default_pop_df


# Split categories into equal groups in terms of their number of channels/streamers using greedy algorithm
def split_categories_into_groups(weighted_category_df): 
    category_groups = [[] for _ in range(25)] # Max is 25 messages which means max of 25 lambda functions running at the same time
    weight_value_groups = [0 for _ in range(25)]
    # Go through each category, then assign it to a group
    for _, row in weighted_category_df.iterrows():
        num_of_streamers = row['num_of_streamers']
        category_id = row["category_id"]
        min_sum = 999999999
        min_idx = -1
        # Iterate through each weight value group to see which one is suitable for category
        for wvg_idx, group_weight_sum in enumerate(weight_value_groups):
            if group_weight_sum + num_of_streamers <= 7000:  # If end group weight sum is 7000 or less, add it first
                min_idx = wvg_idx
                break
            elif group_weight_sum == 0:   # weight group has no category yet, automatically add it
                min_idx = wvg_idx
                break
            elif group_weight_sum <= min_sum:  # if all groups don't have a group weight sum of 0 or would be more than 7000, start adding to smallest weight value groups
                min_sum = group_weight_sum
                min_idx = wvg_idx
        weight_value_groups[min_idx] += num_of_streamers
        category_groups[min_idx].append(category_id)

    return category_groups, weight_value_groups


# Sends each category group as a message
def send_SQS_messages(category_groups, day_date_id, time_of_day_id):
    date_time_info = {
        "day_date_id": {
            "StringValue": day_date_id,
            "DataType": "String"
        },
        "time_of_day_id": {
            "StringValue": time_of_day_id,
            "DataType": "String"
        }
    }

    sqs_client = boto3.client("sqs")
    queue_url = "https://sqs.us-west-2.amazonaws.com/484743883065/category_groups"
    batch_entries = []
    # Each category group will be in one message
    # Loop sends ten messages at a time in a batch
    for i, group in enumerate(category_groups):
        message = {'Id': 'msg' + str(i+1), 'MessageBody': str(group), 'MessageAttributes': date_time_info}
        batch_entries.append(message)
        if (i+1) % 10 == 0 or len(category_groups) == i+1: # every 10th group, we send message batch
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=batch_entries
            )
            batch_entries = []


def lambda_handler(event, context):
    event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
    processed_categories_bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
    processed_categories_key = event_notification["Records"][0]["s3"]["object"]["key"]
    day_date_id = processed_categories_key.split("/")[1]
    time_of_day_id = processed_categories_key.split("/")[2].split("_")[4][:4]

    s3_client = boto3.client("s3")

    # Get current streamed categories based off of processed_categories file
    curr_streamed_categories_df = get_processed_categories(s3_client, processed_categories_bucket_name, processed_categories_key)
    
    # Check if popularity data exists or not
    popularity_data_exists = False
    category_popularity_df = ""
    key = "category_popularity_data/category_popularity_data.csv"
    try:
        s3_client.head_object(Bucket="twitch-project-miscellaneous", Key=key)
        response = s3_client.get_object(Bucket="twitch-project-miscellaneous", Key=key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if  status == 200:
            print(f"Successful S3 get_object response for the category popularity data. Status - {status}")
            category_popularity_df = pd.read_csv(response.get("Body"), keep_default_na = False)
            popularity_data_exists = True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Key: '{key}' does not exist!")
        else:
            print("Something else went wrong")
            exit()

    # Produce category groups
    if popularity_data_exists: # use recent popularity data since it exists
        merged_df = pd.merge(curr_streamed_categories_df, category_popularity_df, on="category_id", how='left')
        merged_df['num_of_streamers'] = merged_df['num_of_streamers'].replace(np.nan, 1)
        category_groups, wvg = split_categories_into_groups(merged_df)
        s3_client.delete_object(Bucket="twitch-project-miscellaneous", Key=key) # delete popularity data
    else: # if no recent category popularity data found, use default popularity data
        default_pop_df = get_default_popularity_df(s3_client)
        category_pop_df = pd.concat([curr_streamed_categories_df, default_pop_df], axis=1)
        category_pop_df = category_pop_df[["category_id", "category_name", "num_of_streamers"]].fillna(1)
        category_groups, wvg = split_categories_into_groups(category_pop_df)

    # Sends groups of categories as messages to categoryGroupWeights SQS queue
    final_category_groups = [group for group in category_groups if len(group) != 0]
    send_SQS_messages(final_category_groups, day_date_id, time_of_day_id) 


    for group in category_groups:
        print(group)
    
    print("WVG: " + str(wvg))
