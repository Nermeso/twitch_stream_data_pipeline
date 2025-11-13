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


# Get locations of most recently collected stream data
def get_stream_data_paths(s3_client):
    data_paths = []
    response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix="raw/fact_table/recent_data/")
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".csv"):
                data_paths.append(obj["Key"])

    return data_paths


# Returns dataframe of currently streamed categories
def get_curr_streamed_categories(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/other/current_streamed_categories.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        curr_streamed_categories = pd.read_csv(response.get("Body"), keep_default_na=False)
    # else: maybe something
    
    return curr_streamed_categories


# Gets category weights
# Category group weights will be based off of most recently collected stream data
def get_category_weights(s3_client, fact_table_data_paths, curr_streamed_categories):
    if len(fact_table_data_paths) != 0:
        grouped_df = combine_fact_table_data(s3_client, fact_table_data_paths)
        weighted_category_df = produce_category_weights(grouped_df, curr_streamed_categories)
        category_groups, wvg = split_categories_into_groups(weighted_category_df)
    else: # if no recent stream data collected, weights will be based off of file containing pre-set weight values for categories
        default_category_df = get_default_category_df(s3_client)
        weighted_category_df = produce_category_weights(default_category_df, curr_streamed_categories)
        category_groups, wvg = split_categories_into_groups(weighted_category_df)

    return category_groups


# Combines recently created fact table CSVs into one aggregated by category to get total # of streams
def combine_fact_table_data(s3_client, fact_table_data_paths):
    master_df = pd.DataFrame(columns=["stream_id", "date_day_id", "time_of_day_id", "user_id", "category_id", "viewer_count", "language_id", "user_name"])
    for data_path in fact_table_data_paths:
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key=data_path)
        df = pd.read_csv(response.get("Body"), keep_default_na=False)
        master_df = pd.concat([master_df, df])
    master_df = master_df.drop_duplicates(subset=['stream_id'].reset_index())
    grouped_df = master_df.groupby('category_id').size().reset_index(name='num_of_streams').sort_values(by='num_of_streams', ascending=False).reset_index(drop=True)
    
    return grouped_df

    

# Produce dataframe containing currently streamed categories with their number of streams associated with it
# Categories with no num_of_streams value is given a default value of 1
def produce_category_weights(grouped_df, curr_streamed_categories):
    merged_df = pd.merge(curr_streamed_categories, grouped_df, on='category_id', how='left')
    merged_df['num_of_streams'] = merged_df['num_of_streams'].replace(np.nan, 1)
    
    return merged_df


# Split categories into equal groups in terms of their weights (num_of_streams) using greedy algorithm
def split_categories_into_groups(weighted_category_df):
    category_groups = [[] for _ in range(20)]
    weight_value_groups = [0 for _ in range(20)]
    # Go through each category, then assign it to a group
    for cat_idx, row in weighted_category_df.iterrows():
        num_of_streams = row['num_of_streams']
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
        weight_value_groups[min_idx] += num_of_streams
        category_groups[min_idx].append(category_id)
        
    return category_groups, weight_value_groups


# Reads file that contains data on typical popular categories and their number of streams
# Serves as default category weights if recent stream data is not present to make weights for
def get_default_category_df(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/other/default_category_weights.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        df = pd.read_csv(response.get("Body"), keep_default_na=False)

    return df


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
    fact_table_data_paths = get_stream_data_paths(s3_client)
    curr_streamed_categories = get_curr_streamed_categories(s3_client)
    category_groups = get_category_weights(s3_client, fact_table_data_paths, curr_streamed_categories)
    send_SQS_messages(category_groups)


