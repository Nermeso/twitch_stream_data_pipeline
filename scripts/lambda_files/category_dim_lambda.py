import os
import requests
import pandas as pd
import time
import boto3
import awswrangler as wr
import json


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
    category_dim_exist = False
    category_dim_path = ""
    response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix="raw/dimension_table/")

    # Check to see if category dimension exists currently
    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith("category_dimension.csv"):
                category_dim_exist = True
                category_dim_path = obj["Key"]

    if category_dim_exist is False: # create category dimension if does not exist
        current_category_dim_df = pd.DataFrame(columns=["category_id", "igdb_id", "category_name"])
        wr.s3.to_csv(current_category_dim_df, "s3://twitchdatapipelineproject/raw/dimension_table/category_dimension.csv", index=False)
        curr_categories = []
    else: # if exists, read it and get the current categories we have info on already
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key=category_dim_path)
        status = response["ResponseMetadata"]["HTTPStatusCode"]
        if status == 200:
            print(f"Successful S3 get_object response for the category dimension. Status - {status}")
            current_category_dim_df = pd.read_csv(response.get("Body"), keep_default_na = False)
            curr_categories = current_category_dim_df["category_id"].tolist()

    return curr_categories, current_category_dim_df


# Iteratively call get top games api to get current games that have at least one streamer to do two things:
# Gets all categories that are currently being streamed
# Get all categories that are not present in the category dimension and to add to it
def call_API_get_category_data(headers, new_category_data_dict, curr_streamed_categories_dict, already_exist_ids):
    url = "https://api.twitch.tv/helix/games/top"
    cursor = ""
    while cursor != "done":
        params = {
            "first": 100,
            "after": cursor
        }
        response = requests.get(url, headers=headers, params=params)
        API_output = response.json()

        if response.status_code == 200:
            process_API_data(API_output, curr_streamed_categories_dict, already_exist_ids, new_category_data_dict)      
        elif response.status_code == 429:
            print("Rate limit exceeded. Retrying in 20 seconds")
            time.sleep(20)
            continue
        else:
            print(f"Error: {response.status_code}")
            print(API_output)
            exit()

        # Ends pagination of pages when done
        if len(API_output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "done"
        else:    
            cursor = API_output["pagination"]["cursor"]


# Gets data on currently streamed categories and adds categories not seen before to dict to be later processed
def process_API_data(API_output, curr_streamed_categories_dict, already_exist_ids, new_category_data_dict):
    for category in API_output["data"]:
        category_id = category["id"]
        category_name = category["name"]
        igdb_id = category["igdb_id"]

        # Adds category to dict containing current streamed categories
        if int(category_id) not in curr_streamed_categories_dict["category_id"]:
            curr_streamed_categories_dict["category_id"].append(int(category_id))
            curr_streamed_categories_dict["category_name"].append(category_name)  

        # Adds category to new category dict if not seen before in category dimension
        if int(category_id) in new_category_data_dict["category_id"] or int(category_id) in already_exist_ids: # if category exists already, go to next category
            continue
        else: # category not seen before, so we add it
            new_category_data_dict["category_id"].append(int(category_id))
            new_category_data_dict["category_name"].append(category_name)
            if igdb_id == "": # IGDB Id may be empty, fill it with NA if the case
                new_category_data_dict["igdb_id"].append("NA")
            else:
                new_category_data_dict["igdb_id"].append(int(igdb_id))


# Returns S3 Client and Twitch API credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    s3_client = boto3.client('s3')
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers, s3_client


# Converts new category data to dataframe, adds it on to current category dimension, then uploads to s3
def add_new_category_data_to_csv(new_category_data_dict, current_category_dim_df):
    new_category_df = pd.DataFrame(new_category_data_dict) # converts dictionary of new category data to dataframe
    final_df = pd.concat([current_category_dim_df, new_category_df]).drop_duplicates() # combines current category data with new
    wr.s3.to_csv(final_df, 's3://twitchdatapipelineproject/raw/dimension_table/category_dimension.csv', index=False)


# Converts dictionary of currently streamed categories to CSV
def curr_streamed_categories_to_csv(curr_streamed_categories_dict):
    df = pd.DataFrame(curr_streamed_categories_dict)
    wr.s3.to_csv(df, 's3://twitchdatapipelineproject/raw/other/current_streamed_categories.csv', index=False)


def lambda_handler(event, context):
    start = time.time()
    headers, s3_client = get_credentials()

    # Reads current category dimension and gets current categories we have info on already
    curr_categories, current_category_dim_df = get_category_dim_info(s3_client)

    new_category_data_dict = {
        "category_id": [],
        "igdb_id": [],
        "category_name": []
    }

    curr_streamed_categories_dict = {
        "category_id": [],
        "category_name": []
    }

    # Calls Get Top Games API to get data on currently streamed categories
    # and potential new categories we have not recorded info on yet
    call_API_get_category_data(headers, new_category_data_dict, curr_streamed_categories_dict, curr_categories)

    # Adds new category data to current dimension then uploads it to S3
    add_new_category_data_to_csv(new_category_data_dict, current_category_dim_df)

    # Convert current streamed categories dict to CSV then uploads to S3
    curr_streamed_categories_to_csv(curr_streamed_categories_dict)

    event_payload = {
                        "table_name": "categories",
                        "data": new_category_data_dict
                    }

    # Invokes another lambda to upload data to postgres db
    lambdaClient = boto3.client('lambda')
    response = lambdaClient.invoke(
        FunctionName='arn:aws:lambda:us-west-1:484743883065:function:insertDatatoDB',
        InvocationType='Event',
        Payload=json.dumps(event_payload)
    )

    end = time.time()
    print("Duration: " + str(end - start))

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }


    


