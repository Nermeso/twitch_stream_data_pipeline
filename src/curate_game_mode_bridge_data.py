import pandas as pd
import boto3
import awswrangler as wr
import time

########################### SUMMARY ###########################
'''
    Updates the current game_mode bridge dimension CSV file. Looks
    through most recently collected current category game_modes
    and inserts them into current dimension file.
'''
###############################################################


# Gets recent processed game_mode bridge dimension data and limits it to relevant columns
def get_processed_game_mode_bridge_data(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the processed game_mode bridge data. Status - {status}")
        processed_game_mode_bridge_df = pd.read_csv(response.get("Body"), keep_default_na = False)
    else:
        print(f"Unsuccessful S3 get_object response for the game_mode bridge dimension. Status - {status}")
        print(response)
        exit()

    return processed_game_mode_bridge_df


# Gets current game_mode bridge dim
def get_game_mode_bridge_dim(s3_client):
    bucket_name = "twitch-project-curated-layer"
    file_key = "curated_game_mode_bridge_data/curated_game_mode_bridge_data.csv"
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated game_mode bridge data. Status - {status}")
            game_mode_bridge_dim_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        else:
            print(f"Unsuccessful S3 get_object response for the game_mode bridge dimension. Status - {status}")
            print(response)
            exit()
    except Exception as e:
        print(e)
        game_mode_bridge_dim_df = pd.DataFrame(columns=["category_id", "game_mode_id"])

    return game_mode_bridge_dim_df


# Adds new game_mode data from processed game_mode bridge data to the curated dimension data
# Also returns dataframe filled with new cateogory game_modes not seen before in original curated game_mode bridge dimension data
def add_new_game_mode_data(processed_game_mode_bridge_df, curated_game_mode_bridge_df):
    new_curated_game_mode_bridge_df = pd.concat([curated_game_mode_bridge_df, processed_game_mode_bridge_df]).drop_duplicates(subset=["category_id", "game_mode_id"]).reset_index()
    new_curated_game_mode_bridge_df = new_curated_game_mode_bridge_df[["category_id", "game_mode_id"]]

    # New category game_modes to be added
    additional_category_game_modes = pd.concat([new_curated_game_mode_bridge_df.drop_duplicates(), curated_game_mode_bridge_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_curated_game_mode_bridge_df, additional_category_game_modes



def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    day_date_id = file_key.split("/")[1]
    time_of_day_id = file_key.split("/")[2].split("_")[6][:4]

    s3_client = boto3.client("s3")

    # Get processed and curated game_mode bridge data
    processed_game_mode_bridge_df = get_processed_game_mode_bridge_data(s3_client, bucket_name, file_key)

    # Curate processed data to only include relevant data
    curated_game_mode_bridge_df = processed_game_mode_bridge_df[["category_id", "game_mode_id"]]
    curated_game_mode_bridge_df = curated_game_mode_bridge_df.drop_duplicates(subset=["category_id", "game_mode_id"]).reset_index(drop=True)


    # Upload CSV to curated layer in S3
    wr.s3.to_csv(
        df=curated_game_mode_bridge_df,
        path=f"s3://twitch-project-curated-layer/curated_game_mode_bridge_data/{day_date_id}/curated_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.csv", 
        index=False
    )


    return {
        'statusCode': 200,
        'body': "Success"
    }