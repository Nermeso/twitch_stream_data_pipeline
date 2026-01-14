import pandas as pd
import json
import boto3
import awswrangler as wr
import time

################################# SUMMARY #################################
'''
    This script processes the raw user data by converting it into a 
    CSV file. Slight modifications will also be made.
'''
###########################################################################

def lambda_handler(event, context):
    start = time.time()

    # Get file location details
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    s3_client = boto3.client("s3")

    # Load in JSON data
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        raw_user_data = json.loads(response["Body"].read().decode("utf-8"))
    else:
        print(f"Error: {status}")
        exit()

    # Convert to dataframe and remove useless columns
    user_df = pd.DataFrame(raw_user_data["data"]).drop_duplicates()
    user_df = user_df.drop(columns=["view_count"]) # view count column is deprecated

    # Replace empty strings with relevant value
    user_df["type"] = user_df["type"].replace("", "normal")
    user_df["broadcaster_type"] = user_df["broadcaster_type"].replace("", "normal")

    # Get day_date_id and time_of_day_id
    day_date_id = raw_user_data["day_date_id"]
    time_of_day_id = raw_user_data["time_of_day_id"]

    # Upload CSV to processed layer in S3
    wr.s3.to_csv(
        df=user_df,
        path=f"s3://twitch-project-processed-layer/processed_users_data/{day_date_id}/processed_users_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }