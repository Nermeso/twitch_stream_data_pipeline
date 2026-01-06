import pandas as pd
import json
import boto3
import awswrangler as wr
import time

############################ SUMMARY ############################
'''
    Processes the raw genre bridge data. Converts it into
    tabular format and adds the appropriate category IDs.
'''
#################################################################

# Gets category id associated with IGDB ID
def get_associated_category_id(category_df, igdb_id):
    category_row = category_df[category_df["igdb_id"] == str(igdb_id)]
    category_id = str(category_row["category_id"].iloc[0].item())
    
    return category_id


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
        raw_genre_bridge_data = json.loads(response["Body"].read().decode("utf-8"))
    else:
        print(f"Error: {status}")
        exit()

    # Load in curated category data
    response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key="curated_categories_data/curated_categories_data.csv")
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        category_df = pd.read_csv(response["Body"], keep_default_na=False)
    else:
        print(f"Error: {status}")
        exit()

    processed_genre_bridge_data_dict = {
        "igdb_id": [],
        "category_id": [],
        "game_name": [],
        "genre_id": []
    }

    # Add genre data to processed genre bridge data dict
    for game_info in raw_genre_bridge_data["data"]: # some games have no associated genres
        if "genres" in game_info.keys():
            category_id = get_associated_category_id(category_df, game_info["id"])
            for genre_id in game_info["genres"]:
                processed_genre_bridge_data_dict["igdb_id"].append(game_info["id"])
                processed_genre_bridge_data_dict["category_id"].append(category_id)
                processed_genre_bridge_data_dict["game_name"].append(game_info["name"])
                processed_genre_bridge_data_dict["genre_id"].append(genre_id)

    # Convert data to dataframe
    processed_genre_bridge_df = pd.DataFrame(processed_genre_bridge_data_dict)

    # Get day_date_id and time_of_day_id
    day_date_id = raw_genre_bridge_data["day_date_id"]
    time_of_day_id = raw_genre_bridge_data["time_of_day_id"]

    # Upload CSV to processed layer in S3
    wr.s3.to_csv(
        df=processed_genre_bridge_df,
        path=f"s3://twitch-project-processed-layer/processed_genre_bridge_data/{day_date_id}/processed_genre_bridge_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }