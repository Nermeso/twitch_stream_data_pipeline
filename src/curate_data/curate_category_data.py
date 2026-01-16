import pandas as pd
import boto3
import awswrangler as wr
import json
import ast

########################### SUMMARY ###########################
'''
    Updates the current category dimension CSV file. Looks
    through most recently collected current category data
    and adds categories not already present in the data.
'''
###############################################################


# Gets recent processed category data
def get_processed_category_data(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the processed category data. Status - {status}")
        processed_category_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        processed_category_df = processed_category_df[["category_id", "category_name", "igdb_id"]] 
    else:
        print(f"Unsuccessful S3 get_object response for the category dimension. Status - {status}")
        print(response)
        exit()

    return processed_category_df


# Gets categories we already have data for
def get_current_categories(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-miscellaneous", Key="current_data/current_categories.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the current category data. Status - {status}")
            current_category_df = pd.read_csv(response.get("Body"), keep_default_na = False)           
    except Exception as e: # if  category data does not exist yet, error will be returned which we will catch
        if "NoSuchKey" in str(e): # if error is due to category data not existing, create new category data
            current_category_df = pd.DataFrame(columns=["category_id", "category_name", "igdb_id"])
        else:
            print("Unsuccessful S3 get_object response for the category data.")
            print(response)
            exit()

    return current_category_df


# Adds new category data from processed category data to the current category data
# Also returns dataframe filled with new categories not seen before to the curated layer to be uploaded to postgres
def add_new_category_data(processed_category_df, current_category_df):
    new_current_category_df = pd.concat([current_category_df, processed_category_df]).drop_duplicates(subset=["category_id"], keep="first").reset_index()
    new_current_category_df = new_current_category_df[["category_id", "category_name", "igdb_id"]]

    # New additional categories are curated categories
    additional_categories_df = pd.concat([new_current_category_df.drop_duplicates(), current_category_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_current_category_df, additional_categories_df


def lambda_handler(event, context):
    event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
    processed_categories_bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
    processed_categories_key = event_notification["Records"][0]["s3"]["object"]["key"]
    day_date_id = processed_categories_key.split("/")[1]
    time_of_day_id = processed_categories_key.split("/")[2].split("_")[4][:4]
    
    s3_client = boto3.client("s3")

    # Gets recent processed category data
    processed_category_df = get_processed_category_data(s3_client, processed_categories_bucket_name, processed_categories_key)

    # Gets categories we currently already have data for
    current_category_df = get_current_categories(s3_client)

    # Curated category data contains new category data to be uploaded to postgres
    # Current categories is updated
    current_categories_df, curated_category_dim_df = add_new_category_data(processed_category_df, current_category_df)

    if curated_category_dim_df.empty:
        print("No new categories added to category dimension data.")
        return {
            'statusCode': 200,
            'body': "No new categories added to category dimension data."
        }

    # Upload new additional categories CSV to curated layer in S3
    wr.s3.to_csv(
        df=curated_category_dim_df,
        path=f"s3://twitch-project-curated-layer/curated_categories_data/{day_date_id}/curated_categories_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    # Updates current categories we have data for
    wr.s3.to_csv(
        df=current_categories_df,
        path=f"s3://twitch-project-miscellaneous/current_data/current_categories.csv",
        index=False
    )
   
    return {
        'statusCode': 200,
        'body': "Success"
    }

