import os
import requests
import boto3
import pandas as pd
from pathlib import Path
import time
import numpy as np

############################################## SUMMARY ##############################################
'''
    This script creates a file that assigns a weight to currently streamed categories indicating
    the number of channels currently streaming it. This is important input to serve the lambda
    functions collecting stream data.
'''
#####################################################################################################


# Get locations of most recently collected category popularity data from collectStreamData function
def get_category_popularity_data_paths(s3_client):
    data_paths = []
    response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix="raw/fact_table/recent_category_popularity_data/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                data_paths.append(obj["Key"])

    return data_paths


# Combine different CSVs of category popularity data into one dataframe
def combine_category_popularity(category_popularity_paths, s3_client):
    popularity_df = pd.DataFrame(columns=["category_id", "num_of_streamers"])
    for path in category_popularity_paths:
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key=path)
        df_tmp = pd.read_csv(response.get("Body"), keep_default_na=False)
        popularity_df = pd.concat([popularity_df, df_tmp])
    popularity_df = popularity_df.drop_duplicates(subset=['category_id'], keep='first').sort_values(by="num_of_streamers", ascending=False)
    
    return popularity_df


# Returns dataframe of currently streamed categories
def get_curr_streamed_categories(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/other/current_streamed_categories.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        curr_streamed_categories = pd.read_csv(response.get("Body"), keep_default_na=False)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    
    return curr_streamed_categories

    
# Produce dataframe containing currently streamed categories with default num_of_streams supplanted from default category popularity file
# Categories with no associated default value will get a value of 1 streamer
def produce_category_weights(popularity_df, curr_streamed_categories):
    output_df = pd.concat([curr_streamed_categories, popularity_df], axis=1)
    output_df = output_df[["category_id", "category_name", "num_of_streamers"]].fillna(1)
    
    return output_df


# Split categories into equal groups in terms of their number of channels/streamers using greedy algorithm
def split_categories_into_groups(weighted_category_df):
    category_groups = [[] for _ in range(20)]
    weight_value_groups = [0 for _ in range(20)]
    # Go through each category, then assign it to a group
    for cat_idx, row in weighted_category_df.iterrows():
        num_of_streamers = row['num_of_streamers']
        category_id = row["category_id"]
        min_sum = 999999999
        min_idx = -1
        # Iterate through each weight value group to see which one is suitable for category
        for wvg_idx, group_weight_sum in enumerate(weight_value_groups):
            # weight group has no category yet, automatically add it
            if group_weight_sum == 0:
                min_sum = group_weight_sum
                min_idx = wvg_idx
                break
            # aim for category to be assigned to group with the lowest total summed weight
            elif group_weight_sum <= min_sum:
                min_sum = group_weight_sum
                min_idx = wvg_idx
        weight_value_groups[min_idx] += num_of_streamers
        category_groups[min_idx].append(category_id)
        
    return category_groups, weight_value_groups


# Reads file that contains data default num_of_streamers we will use if recent stream popularity data is not present
def get_default_category_df(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/other/default_category_weights.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"), keep_default_na=False)

    return df


# Merges current streamed categories with most recently streamed categories with streamer number data
# Categories with no associated value gets a default number of channels of 1
def merge_current_categories(popularity_df, curr_streamed_categories):
    merged_df = pd.merge(curr_streamed_categories, popularity_df, on="category_id", how='left')
    merged_df['num_of_streamers'] = merged_df['num_of_streamers'].replace(np.nan, 1)

    return merged_df


# Sends each category group as a message
def send_SQS_messages(category_groups):
    sqs_client = boto3.client("sqs")
    queue_url = "https://sqs.us-west-1.amazonaws.com/484743883065/categoryGroupWeights"
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
    s3_client = boto3.client('s3')
    category_popularity_paths = get_category_popularity_data_paths(s3_client)
    curr_streamed_categories = get_curr_streamed_categories(s3_client)

    # Category group weights will be based off of most recently collected stream data
    if len(category_popularity_paths) != 0:
        popularity_df = combine_category_popularity(category_popularity_paths, s3_client)
        weighted_category_df = merge_current_categories(popularity_df, curr_streamed_categories)
        category_groups, wvg = split_categories_into_groups(weighted_category_df)
    else: # if no recent stream data collected, weights will be based off of file containing category popularity
        default_category_df = get_default_category_df(s3_client)
        weighted_category_df = produce_category_weights(default_category_df, curr_streamed_categories)
        category_groups, wvg = split_categories_into_groups(weighted_category_df)

    # Delete recent popularity data files
    for path in category_popularity_paths:
        s3_client.delete_object(Bucket="twitchdatapipelineproject", Key=path)

    # Sends groups of categories as messages to categoryGroupWeights SQS queue
    send_SQS_messages(category_groups)

    print("Weight Value Groups: ")
    print(wvg)
    print()
    print("Category Groups: ")
    for group in category_groups:
        print(group)

