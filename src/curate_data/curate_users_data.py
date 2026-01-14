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


# Gets current users we have info for already
def get_current_users(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-miscellaneous", Key="current_data/current_users.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the current users data. Status - {status}")
            current_user_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        else:
            print(f"Unsuccessful S3 get_object response for the current users data. Status - {status}")
            print(response)
            exit()
    except Exception as e: # if current user data does not exist yet, error will be thrown
        if "NoSuchKey" in str(e):
            current_user_df = pd.DataFrame(columns=["user_id", "user_name", "login_name", "broadcaster_type"])
        else:
            print("Unsuccessful S3 get_object response for the current user data.")
            print(response)
            exit()

    return current_user_df


# Adds new user data from processed user data to the current users data
# Also returns dataframe filled with new users not seen before in to the curated layer to be uploaded to the postgres DB
def add_new_user_data(processed_user_df, current_user_df):
    new_current_user_df = pd.concat([current_user_df, processed_user_df]).drop_duplicates(subset=["user_id"], keep="first").reset_index()
    new_current_user_df = new_current_user_df[["user_id", "user_name", "login_name", "broadcaster_type"]]

    # New additional users not seen before will be uploaded as curated data to curated layer
    additional_users_df = pd.concat([new_current_user_df.drop_duplicates(), current_user_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_current_user_df, additional_users_df



def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    day_date_id = file_key.split("/")[1]
    time_of_day_id = file_key.split("/")[2].split("_")[4][:4]

    s3_client = boto3.client("s3")

    # Gets recent processed user data
    processed_user_df = get_processed_user_data(s3_client, bucket_name, file_key)

    # Change column names
    processed_user_df = processed_user_df.rename(columns={
        "id": "user_id",
        "display_name": "user_name",
        "login": "login_name"
    })

    current_users_df = get_current_users(s3_client)

    # Curated user data contains new user data to be uploaded to postgres
    # Current users is updated
    current_users_df, curated_users_df = add_new_user_data(processed_user_df, current_users_df) 

    if curated_users_df.empty:
        return {
            'statusCode': 200,
            'body': "No new user data to be added"
        }


    # Converts new additional user data to CSV and uploads to curated layer which will be uploaded to postgres
    wr.s3.to_csv(
        df=curated_users_df,
        path=f"s3://twitch-project-curated-layer/curated_users_data/{day_date_id}/curated_users_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    # Updates the current users we have data for already
    wr.s3.to_csv(
        df=current_users_df,
        path=f"s3://twitch-project-miscellaneous/current_data/current_users.csv",
        index=False
    )

    return {
        'statusCode': 200,
        'body': "Success"
    }
