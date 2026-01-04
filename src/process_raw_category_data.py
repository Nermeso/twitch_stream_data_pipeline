import pandas as pd
import json
import boto3
import awswrangler as wr

################################# SUMMARY #################################
'''
    This script processes the raw category data by converting it into a 
    CSV file. Slight modifications will also be made such as converting 
    empty IGDB values to "NA".
'''
###########################################################################


def lambda_handler(event, context):
    # Get file location details
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    s3_client = boto3.client("s3")

    # Load in JSON data
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        raw_category_data = json.loads(response["Body"].read().decode("utf-8"))
    else:
        print(f"Error: {status}")
        exit()

    # Convert to dataframe and rename columns
    category_df = pd.DataFrame(raw_category_data["data"]).drop_duplicates()
    category_df = category_df.rename(columns = {"id": "category_id", "name": "category_name"})

    # Get day_date_id and time_of_day_id
    day_date_id = raw_category_data["day_date_id"]
    time_of_day_id = raw_category_data["time_of_day_id"]

    # Replace empty strings with "NA"
    category_df["igdb_id"] = category_df["igdb_id"].replace("", "NA")
    category_df["box_art_url"] = category_df["box_art_url"].replace("", "NA")

    # Upload CSV to processed layer in S3
    wr.s3.to_csv(
        df=category_df,
        path=f"s3://twitch-project-processed-layer/processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }
