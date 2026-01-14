import pandas as pd
from datetime import datetime
import json
import time
import boto3
import awswrangler as wr
import ast

################################# SUMMARY #################################
'''
    This script produces a CSV file that contains the popularity of each
    category based off of the most recently collected stream data.
'''
###########################################################################

def get_curated_stream_data(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the curated stream data. Status - {status}")
        curated_stream_df = pd.read_csv(response.get("Body"), keep_default_na = False)
    else:
        print(f"Unsuccessful S3 get_object response for the stream data. Status - {status}")
        print(response)
        exit()
    
    return curated_stream_df


def lambda_handler(event, context):
    s3_client = boto3.client("s3")

    event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
    curated_streams_bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
    curated_streams_key = event_notification["Records"][0]["s3"]["object"]["key"]
    day_date_id = curated_streams_key.split("/")[1]
    time_of_day_id = curated_streams_key.split("/")[2].split("_")[4][:4]

    curated_stream_df = get_curated_stream_data(s3_client, curated_streams_bucket_name, curated_streams_key)

    # Transforms it to get number of streamers per category
    category_popularity_df = curated_stream_df.groupby(["category_id"], as_index=False).agg(
                                        category_id=('category_id', 'first'),
                                        num_of_streamers=('stream_id', 'count')
                                   ).sort_values(by="num_of_streamers", ascending=False).reset_index(drop=True)
    
    # Upload file as CSV to miscellaneous bucket for next create_category_groups function invocation to use
    wr.s3.to_csv(
                df=category_popularity_df,
                path=f"s3://twitch-project-miscellaneous/category_popularity_data/category_popularity_data.csv",
                index=False
            )
