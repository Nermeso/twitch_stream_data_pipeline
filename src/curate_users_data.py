import pandas as pd
import boto3
import awswrangler as wr

########################### SUMMARY ###########################
'''
    Updates the current user dimension CSV file. Looks
    through most recently collected current user data
    and adds users not already present in the data.
'''
###############################################################

# Gets recent processed user data
def get_processed_user_data(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the processed user data. Status - {status}")
        processed_user_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        processed_user_df = processed_user_df[["id", "display_name", "login", "broadcaster_type"]] 
    else:
        print(f"Unsuccessful S3 get_object response for the user dimension. Status - {status}")
        print(response)
        exit()

    return processed_user_df


# Gets curated user dimension data
def get_user_dim_info(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key="curated_users_data/curated_users_data.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the user dimension. Status - {status}")
            user_dim_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        else:
            print(f"Unsuccessful S3 get_object response for the user dimension. Status - {status}")
            print(response)
            exit()
    except Exception as e: # if curated user dimension data does not exist yet, error will be thrown
        print(e)
        if "NoSuchKey" in str(e):
            user_dim_df = pd.DataFrame(columns=["user_id", "user_name", "login_name", "broadcaster_type"])
        else:
            print("Unsuccessful S3 get_object response for the curated user data.")
            print(response)
            exit()

    return user_dim_df


# Adds new user data from processed user data to the curated dimension data
# Also returns dataframe filled with new users not seen before in original curated user dimension data
def add_new_user_data(processed_user_df, user_dim_df):
    curated_user_dim_df = pd.concat([user_dim_df, processed_user_df]).drop_duplicates(subset=["user_id"]).reset_index(drop=True)
    curated_user_dim_df["user_id"] = curated_user_dim_df["user_id"].astype(int)
    curated_user_dim_df = curated_user_dim_df[["user_id", "user_name", "login_name", "broadcaster_type"]]

    # New users added to dimension data
    additional_users_df = pd.concat([curated_user_dim_df.drop_duplicates(), user_dim_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)
  
    return curated_user_dim_df, additional_users_df



def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]

    s3_client = boto3.client("s3")

    # Gets recent processed user data
    processed_user_df = get_processed_user_data(s3_client, bucket_name, file_key)

    # Change column names
    processed_user_df = processed_user_df.rename(columns={
        "id": "user_id",
        "display_name": "user_name",
        "login": "login_name"
    })

    user_dim_df = get_user_dim_info(s3_client) # gets current user dimension data

    # Adds new user data to curated user dimension file
    # Additional users should be equivalent to processed_user_df if previous part of data pipeline was correct
    curated_user_dim_df, additional_users = add_new_user_data(processed_user_df, user_dim_df) 

    # Upload CSV to curated layer in S3
    wr.s3.to_csv(
        df=curated_user_dim_df,
        path=f"s3://twitch-project-curated-layer/curated_users_data/curated_users_data.csv",
        index=False
    )

    # Converts new additional user data to CSV and uploads to temp file which will be uploaded to postgres
    wr.s3.to_csv(
        df=additional_users,
        path=f"s3://twitch-project-miscellaneous/temp_table_data/new_users_data.csv",
        index=False
    )

    return {
        'statusCode': 200,
        'body': "Success"
    }
