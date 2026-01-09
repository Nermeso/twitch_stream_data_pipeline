import os
from igdb.wrapper import IGDBWrapper
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import json
import time

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


# Gets current date id based of date when script is executed
def get_day_date_id(s3_client):
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_day_dates_data/raw_day_dates_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for date dimension. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    else:
        print(f"Unsuccessful S3 get_object response for date dimension. Status - {status}")
        print(response)
        exit()
    current_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]

    return str(day_date_id)


# Gets time of day id based off of current time of script execution
def get_time_of_day_id(s3_client):
    response = s3_client.get_object(Bucket="twitch-project-raw-layer", Key="raw_time_of_day_data/raw_time_of_day_data.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for time of day dimension. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False, dtype={"time_of_day_id": str})
    else:
        print(f"Unsuccessful S3 get_object response for time of day dimension. Status - {status}")
        print(response)
        exit()
    cur_date = datetime.today().astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    minimum_diff = 1000
    time_of_day_id = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(cur_date.year, cur_date.month, cur_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((cur_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_of_day_id = row[1]["time_of_day_id"]

    return time_of_day_id



# Gets the curated game_mode bridge dimension data
def get_curated_game_mode_bridge_data(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key=f"curated_game_mode_bridge_data/curated_game_mode_bridge_data.csv")
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the curated game_mode bridge data. Status - {status}")
            curated_game_mode_bridge_df = pd.read_csv(response.get("Body"), keep_default_na=False)
        else:
            print(f"Unsuccessful S3 get_object response for the curated streams data. Status - {status}")
            exit()
    except Exception as e: # if curated game_mode bridge data does not exist yet, error will be returned which we will catch
        print(e)
        curated_game_mode_bridge_df = pd.DataFrame(columns=["category_id", "igdb_id", "game_mode_id"])

    return curated_game_mode_bridge_df


# Gets the curated game_mode bridge data
def get_curated_category_data(s3_client):
    try:
        response = s3_client.get_object(Bucket="twitch-project-curated-layer", Key=f"curated_categories_data/curated_categories_data.csv")
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
def get_raw_game_mode_data(wrapper, category_df, game_mode_bridge_df, raw_category_game_mode_data_dict):
    exclude_list = game_mode_bridge_df["category_id"].tolist()
    new_category_df = category_df[~category_df["category_id"].isin(exclude_list)].reset_index() # only include categories that we did not get game_mode data on yet

    # One API call accepts max 100 IGDB ids
    # To minimize num of calls made, we make one API call per 100 games
    igdb_category_id_temp = {} # temporarily store what category ids are associated with igdb ids
    for i, row in new_category_df.iterrows():
        i += 1
        igdb_id = str(row["igdb_id"])
        category_id = str(row["category_id"])

        if igdb_id != "NA": # Only consider categories that have an associated IGDB ID
            igdb_category_id_temp[int(float(igdb_id))] = int(float(category_id))
        if i % 100 == 0 or i == len(new_category_df): # every 100 igdb games or if last one, make api call
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
        except Exception as e:
            print(e)
            time.sleep(5)
            continue

    my_json = byte_array.decode('utf8').replace("'", '"')
    game_mode_data = json.loads(my_json)

    return game_mode_data



def lambda_handler(event, context):
    start = time.time()

    s3_client = boto3.client("s3")
    wrapper = make_wrapper()
    day_date_id = get_day_date_id(s3_client)
    time_of_day_id = get_time_of_day_id(s3_client)

    # Get curated game_mode bridge data
    curated_game_mode_bridge_df = get_curated_game_mode_bridge_data(s3_client)

    # Get curated category data
    curated_category_df = get_curated_category_data(s3_client)

    raw_category_game_mode_data_dict = {
            "day_date_id": day_date_id,
            "time_of_day_id": time_of_day_id,
            "data": []
        }

    # Get new game_mode data
    get_raw_game_mode_data(wrapper, curated_category_df, curated_game_mode_bridge_df, raw_category_game_mode_data_dict)

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