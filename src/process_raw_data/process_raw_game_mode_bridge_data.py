import pandas as pd
import json
import boto3
import awswrangler as wr
import time

############################ SUMMARY ############################
'''
    Processes the raw game_mode bridge data. Converts it into
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
        raw_game_mode_bridge_data = json.loads(response["Body"].read().decode("utf-8"))
    else:
        print(f"Error: {status}")
        exit()

    # Get day_date_id and time_of_day_id
    day_date_id = raw_game_mode_bridge_data["day_date_id"]
    time_of_day_id = raw_game_mode_bridge_data["time_of_day_id"]

    # Load in curated category data
    response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key=f"curated_categories_data/{day_date_id}/curated_categories_data_{day_date_id}_{time_of_day_id}.csv")
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        category_df = pd.read_csv(response["Body"], keep_default_na=False)
    else:
        print(f"Error: {status}")
        exit()

    processed_game_mode_bridge_data_dict = {
        "igdb_id": [],
        "category_id": [],
        "game_name": [],
        "game_mode_id": []
    }

    # Add game_mode data to processed game_mode bridge data dict
    for game_info in raw_game_mode_bridge_data["data"]: # some games have no associated game_modes
        if "game_modes" in game_info.keys():
            category_id = get_associated_category_id(category_df, game_info["id"])
            for game_mode_id in game_info["game_modes"]:
                processed_game_mode_bridge_data_dict["igdb_id"].append(game_info["id"])
                processed_game_mode_bridge_data_dict["category_id"].append(category_id)
                processed_game_mode_bridge_data_dict["game_name"].append(game_info["name"])
                processed_game_mode_bridge_data_dict["game_mode_id"].append(game_mode_id)

    # Convert data to dataframe
    processed_game_mode_bridge_df = pd.DataFrame(processed_game_mode_bridge_data_dict)

    # Upload CSV to processed layer in S3
    wr.s3.to_csv(
        df=processed_game_mode_bridge_df,
        path=f"s3://twitch-project-processed-layer/processed_game_mode_bridge_data/{day_date_id}/processed_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.csv",
        index=False
    )

    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))

    return {
        'statusCode': 200,
        'body': json.dumps('Success!')
    }