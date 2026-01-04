import pandas as pd
import boto3
import awswrangler as wr

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
        processed_category_df[["category_id", "category_name", "igdb_id"]] 
    else:
        print(f"Unsuccessful S3 get_object response for the category dimension. Status - {status}")
        print(response)
        exit()

    return processed_category_df


# Gets the curated category dimension data
def get_category_dim_info(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key="curated_categories_data/curated_categories_data.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated category data. Status - {status}")
            category_dim_df = pd.read_csv(response.get("Body"), keep_default_na = False)           
    except Exception as e: # if curated category data does not exist yet, error will be returned which we will catch
        print(e)
        if "NoSuchKey" in str(e): # if error is due to category data not existing, create new category data
            category_dim_df = pd.DataFrame(columns=["category_id", "category_name", "igdb_id"])
        else:
            print("Unsuccessful S3 get_object response for the curated category data.")
            print(response)
            exit()

    return category_dim_df


# Adds new category data from processed category data to the curated dimension data
# Also returns dataframe filled with new categories not seen before in original curated category dimension data
def add_new_category_data(processed_category_df, category_dim_df):
    curated_category_dim_df = pd.concat([category_dim_df, processed_category_df]).drop_duplicates(subset=["category_id"], keep="first").reset_index()
    curated_category_dim_df = curated_category_dim_df[["category_id", "category_name", "igdb_id"]]

    # New categories added to dimension data
    additional_categories_df = pd.concat([curated_category_dim_df.drop_duplicates(), category_dim_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return curated_category_dim_df, additional_categories_df


def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    s3_client = boto3.client("s3")

    # Gets recent processed category data
    processed_category_df = get_processed_category_data(s3_client, bucket_name, file_key)

    # Gets current category dimension data
    category_dim_df = get_category_dim_info(s3_client)

    # adds new category data to curated category dimension file
    curated_category_dim_df, additional_categories_df = add_new_category_data(processed_category_df, category_dim_df)

    if additional_categories_df.empty:
        print("No new categories added to category dimension data.")
        return {
            'statusCode': 200,
            'body': "No new categories added to category dimension data."
        }

    # Upload CSV to curated layer in S3
    wr.s3.to_csv(
        df=curated_category_dim_df,
        path=f"s3://twitch-project-curated-layer/curated_categories_data/curated_categories_data.csv",
        index=False
    )

    # Converts new additional category data to CSV and uploads to temp file which will be uploaded to postgres
    wr.s3.to_csv(
        df=additional_categories_df,
        path=f"s3://twitch-project-miscellaneous/temp_table_data/new_categories_data.csv",
        index=False
    )
   
    return {
        'statusCode': 200,
        'body': "Success"
    }

