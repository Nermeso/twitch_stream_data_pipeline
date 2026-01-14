import os
from igdb.wrapper import IGDBWrapper
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import json
import time
import ast

############################## SUMMARY ##############################
'''
    This script calls the IGDB API to get raw game_mode data for
    for categories in the category dimension.
'''
#####################################################################


# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    s3_client = boto3.client('s3')
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers, s3_client


# Gets the curated game_mode bridge data
def get_curated_category_data(s3_client, bucket_name, obj_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=obj_key)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated category data. Status - {status}")
            curated_category_df = pd.read_csv(response.get("Body"), keep_default_na=False)
        else:
            print(f"Unsuccessful S3 get_object response for the curated category data. Status - {status}")
            exit()
    except Exception as e:
        print(e)
        exit()

    return curated_category_df



# Get raw game_mode data for new categories
def get_raw_category_game_mode_data(wrapper, curated_categories_df, raw_category_game_mode_data_dict):
    # One API call accepts max 100 IGDB ids
    # To minimize num of calls made, we make one API call per 100 games
    igdb_category_id_temp = {} # temporarily store what category ids are associated with igdb ids
    for i, row in curated_categories_df.iterrows():
        i += 1
        igdb_id = str(row["igdb_id"])
        category_id = str(row["category_id"])

        if igdb_id != "NA": # Only consider categories that have an associated IGDB ID
            igdb_category_id_temp[int(float(igdb_id))] = int(float(category_id))
        if i % 100 == 0 or i == len(curated_categories_df): # every 100 igdb games or if last one, make api call
            igdb_ids_tuple = tuple(igdb_category_id_temp.keys())
            igdb_ids_arg = str(igdb_ids_tuple)
            if len(igdb_ids_tuple) == 1:
                igdb_ids_arg = igdb_ids_arg.replace(',', '')
            raw_game_mode_data = get_igdb_game_mode(wrapper, igdb_ids_arg) # Calls api to get data
            raw_category_game_mode_data_dict["data"].extend(raw_game_mode_data)
            igdb_category_id_temp.clear()




# Calls IGDB API to get data on up to 100 games' game_modes
def get_igdb_game_mode(wrapper, igdb_ids_arg):
    while True:
        try:
            byte_array = wrapper.api_request(
                            "games",
                            f"f name, game_modes; where id = {igdb_ids_arg}; limit 100;"
                    )
            break
        except Exception as e: # If rate limit reached, reset
            time.sleep(5)
            continue

    my_json = byte_array.decode('utf8').replace("'", '"')
    game_mode_data = json.loads(my_json)

    return game_mode_data



def lambda_handler(event, context):
    start = time.time()
    event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
    curated_categories_bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
    curated_categories_key = event_notification["Records"][0]["s3"]["object"]["key"]
    day_date_id = curated_categories_key.split("/")[1]
    time_of_day_id = curated_categories_key.split("/")[2].split("_")[4][:4]


    s3_client = boto3.client("s3")
    wrapper = make_wrapper()

    # Get curated category data
    curated_categories_df = get_curated_category_data(s3_client, curated_categories_bucket_name, curated_categories_key)

    raw_category_game_mode_data_dict = {
            "day_date_id": day_date_id,
            "time_of_day_id": time_of_day_id,
            "data": []
        }

    # Get new game_mode data
    get_raw_category_game_mode_data(wrapper, curated_categories_df, raw_category_game_mode_data_dict)

    # Write the raw category game_mode data to json file
    s3_client.put_object(
            Bucket="twitch-project-raw-layer",
            Key=f"raw_game_mode_bridge_data/{day_date_id}/raw_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.json",
            Body=json.dumps(raw_category_game_mode_data_dict, indent=4),
            ContentType='application/json'
        )

    end = time.time()
    duration = end - start
    print("Duration: " + str(duration))

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }