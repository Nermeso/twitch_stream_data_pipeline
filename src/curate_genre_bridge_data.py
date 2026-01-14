import pandas as pd
import boto3
import awswrangler as wr
import time

########################### SUMMARY ###########################
'''
    Updates the current genre bridge dimension CSV file. Looks
    through most recently collected current category genres
    and inserts them into current dimension file.
'''
###############################################################


# Gets recent processed genre bridge dimension data and limits it to relevant columns
def get_processed_genre_bridge_data(s3_client, bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the processed genre bridge data. Status - {status}")
        processed_genre_bridge_df = pd.read_csv(response.get("Body"), keep_default_na = False)
    else:
        print(f"Unsuccessful S3 get_object response for the genre bridge dimension. Status - {status}")
        print(response)
        exit()

    return processed_genre_bridge_df


# Gets current genre bridge dim
def get_genre_bridge_dim(s3_client):
    bucket_name = "twitch-project-curated-layer"
    file_key = "curated_genre_bridge_data/curated_genre_bridge_data.csv"
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated genre bridge data. Status - {status}")
            genre_bridge_dim_df = pd.read_csv(response.get("Body"), keep_default_na = False)
        else:
            print(f"Unsuccessful S3 get_object response for the genre bridge dimension. Status - {status}")
            print(response)
            exit()
    except Exception as e:
        print(e)
        genre_bridge_dim_df = pd.DataFrame(columns=["category_id", "genre_id"])
    
    return genre_bridge_dim_df


# Adds new genre data from processed genre bridge data to the curated dimension data
# Also returns dataframe filled with new cateogory genres not seen before in original curated genre bridge dimension data
def add_new_genre_data(processed_genre_bridge_df, curated_genre_bridge_df):
    new_curated_genre_bridge_df = pd.concat([curated_genre_bridge_df, processed_genre_bridge_df]).drop_duplicates(subset=["category_id", "genre_id"]).reset_index()
    new_curated_genre_bridge_df = new_curated_genre_bridge_df[["category_id", "genre_id"]]
    
    # New category genres to be added
    additional_category_genres = pd.concat([new_curated_genre_bridge_df.drop_duplicates(), curated_genre_bridge_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_curated_genre_bridge_df, additional_category_genres



def lambda_handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    day_date_id = file_key.split("/")[1]
    time_of_day_id = file_key.split("/")[2].split("_")[5][:4]

    s3_client = boto3.client("s3")

    # Get processed and curated genre bridge data
    processed_genre_bridge_df = get_processed_genre_bridge_data(s3_client, bucket_name, file_key)

    # Curate processed data to only include relevant data
    curated_genre_bridge_df = processed_genre_bridge_df[["category_id", "genre_id"]]
    curated_genre_bridge_df = curated_genre_bridge_df.drop_duplicates(subset=["category_id", "genre_id"]).reset_index(drop=True)


    # Upload CSV to curated layer in S3
    wr.s3.to_csv(
        df=curated_genre_bridge_df,
        path=f"s3://twitch-project-curated-layer/curated_genre_bridge_data/{day_date_id}/curated_genre_bridge_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )


    return {
        'statusCode': 200,
        'body': "Success"
    }

