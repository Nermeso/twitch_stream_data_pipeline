import os
import requests
import pandas as pd
import time
import boto3
import botocore.exceptions
import awswrangler as wr


###################### SUMMARY #####################
'''
    This script calls the Twitch API to get
    the names and IGDB ids of all Twitch categories
    that have at least 1 streamer at the moment
    of script execution.
'''
####################################################



# Creates dataframe of current category dimension and creates list of category ids in that dim
def get_category_dim_info(s3_client):
    try:
        s3_client.head_object(Bucket='twitchdatapipelineproject', Key="raw/dimension_table/category_dimension.csv")
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/category_dimension.csv")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            current_category_dim_df = pd.read_csv(response.get("Body"), keep_default_na=False)
            current_ids = current_category_dim_df["category_id"].tolist()
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            exit()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404": # key does not exist, so we make new csv
            current_category_dim_df = pd.DataFrame(columns=["category_id", "igdb_id", "category_name"])
            wr.s3.to_csv(current_category_dim_df, "s3://twitchdatapipelineproject/raw/dimension_table/category_dimension.csv", index=False)
            current_ids = []
        elif e.response['Error']['Code'] == 403:
            print("Unauthorized access. Ending program.")
            exit()
        else:
            print("Something else went wrong. Ending program.")
            exit()
    
    return current_category_dim_df, current_ids


# Iteratively call get top games api to get current games that have at least one streamer to do two things:
# Gets all categories that are currently being streamed
# Get all categories that are not present in the category dimension and to add to it
def api_call_loop(url, headers, data, already_exist_ids):
    current_streamed_categories_dict = {"category_id": [], "category_name": []}
    cursor = ""
    while cursor != "done":
        params = {
            "first": 100,
            "after": cursor
        }
        response = requests.get(url, headers=headers, params=params)
        output = response.json()
        for category in output["data"]:
            category_id = category["id"]
            category_name = category["name"]
            igdb_id = category["igdb_id"]

            # Adds category to dict containing current streamed categories
            if int(category_id) not in current_streamed_categories_dict["category_id"]:
                current_streamed_categories_dict["category_id"].append(int(category_id))
                current_streamed_categories_dict["category_name"].append(category_name)  

            # Adds category to new category dict if not seen before in category dimension
            if int(category_id) in data["category_id"] or int(category_id) in already_exist_ids: # if category exists already, go to next category
                continue
            data["category_id"].append(int(category_id))
            data["category_name"].append(category_name)
            if igdb_id == "": # IGDB Id may be empty, fill it with NA if the case
                data["igdb_id"].append("NA")
            else:
                data["igdb_id"].append(int(igdb_id))

        # Ends pagination of pages when done
        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "done"
        else:    
            cursor = output["pagination"]["cursor"]

    return current_streamed_categories_dict


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }
    s3_client = boto3.client(
        's3'
    )

    return headers, s3_client


# Call Twitch top games API preiodically to collect game info
def collect_twitch_category_data(headers, s3_client):
    url = "https://api.twitch.tv/helix/games/top"
    data = {
        "category_id": [],
        "igdb_id": [],
        "category_name": []
    }

    current_category_dim_df, current_ids = get_category_dim_info(s3_client)

    while True:
        try:
            current_streamed_categories_dict = api_call_loop(url, headers, data, current_ids)
            break
        except ConnectionError as e:
            data = {"category_id": [], "igdb_id": [],"category_name": []}
            continue        

    # current_category_dim_df = top_games_api_call_loop(url, headers, data, s3_client)

    return data, current_category_dim_df, current_streamed_categories_dict


# Converts dictionary of category data to csv
def data_to_csv(data_dict, current_dim_df):
    data = pd.DataFrame(data_dict) # converts dictionary of data to dataframe
    final_df = pd.concat([current_dim_df, data]).drop_duplicates() # combines current category data with new
    wr.s3.to_csv(final_df, 's3://twitchdatapipelineproject/raw/dimension_table/category_dimension.csv', index=False)

    return


# Converts dictionary of currently streamed categories to CSV
def curr_streamed_categories_to_csv(current_streamed_categories_dict):
    df = pd.DataFrame(current_streamed_categories_dict)
    wr.s3.to_csv(df, 's3://twitchdatapipelineproject/raw/other/current_streamed_categories.csv', index=False)



def lambda_handler(event, context):
    headers, s3_client = get_credentials()
    new_category_data_dict, current_dim_df, current_streamed_categories_dict = collect_twitch_category_data(headers, s3_client)
    data_to_csv(new_category_data_dict, current_dim_df)
    curr_streamed_categories_to_csv(current_streamed_categories_dict)

