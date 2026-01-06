import pandas as pd
import time
import numpy as np
import boto3
from datetime import datetime
from zoneinfo import ZoneInfo
import botocore



# This script triggers once processed_categories is uploaded
# First searches for category popularity data in miscellaneous bucket
# if that does not exist, refer to default weights and use that when creating
# category group messages


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


# Gets most recently made processed categories df to be used as current streamed categories
def get_processed_categories(s3_client, day_date_id, time_of_day_id):
    try:
        object_key = f"processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv"
        response = s3_client.get_object(Bucket="twitch-project-processed-layer", Key=object_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated category data. Status - {status}")
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
def send_SQS_messages(category_groups):
    sqs_client = boto3.client("sqs")
    queue_url = "https://sqs.us-west-2.amazonaws.com/484743883065/category_groups"
    batch_entries = []
    # Each category group will be in one message
    # Loop sends ten messages at a time in a batch
    for i, group in enumerate(category_groups):
        message = {'Id': 'msg' + str(i+1), 'MessageBody': str(group)}
        batch_entries.append(message)
        if (i+1) % 10 == 0 or len(category_groups) == i+1: # every 10th group, we send message batch
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=batch_entries
            )
            batch_entries = []


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    day_date_id = get_day_date_id(s3_client)
    time_of_day_id = get_time_of_day_id(s3_client)

    day_date_id = "20260102" # test value
    time_of_day_id = "1430" # test value

    # Get current streamed categories based off of processed_categories file
    curr_streamed_categories_df = get_processed_categories(s3_client, day_date_id, time_of_day_id)
    
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
        s3_client.delete_object(Bucket="twitch-project-miscellaneous", Key=key)
    else: # if no recent category popularity data found, use default popularity data
        default_pop_df = get_default_popularity_df(s3_client)
        category_pop_df = pd.concat([curr_streamed_categories_df, default_pop_df], axis=1)
        category_pop_df = category_pop_df[["category_id", "category_name", "num_of_streamers"]].fillna(1)
        category_groups, wvg = split_categories_into_groups(category_pop_df)

    # Sends groups of categories as messages to categoryGroupWeights SQS queue
    final_category_groups = [group for group in category_groups if len(group) != 0]
    # send_SQS_messages(final_category_groups) 


    for group in category_groups:
        print(group)
    
    print("WVG: " + str(wvg))
