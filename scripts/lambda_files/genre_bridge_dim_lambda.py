import os
import time
from igdb.wrapper import IGDBWrapper
import json
import pandas as pd
import boto3
import botocore.exceptions
import awswrangler as wr


############################## SUMMARY ##############################
'''
    This script creates data for the genre bridge dimension.
    This table contains the category_id and genre_id attribute
    This script requires the category_dimension.csv file. If the 
    bridge dimension data already exists, it updates it with new
    categories not seen before and their associated genres.
'''
#####################################################################

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)

    return output


# Calls IGDB API to get data on up to 100 games' genres
def get_igdb_genre(wrapper, igdb_ids_arg):
    byte_array = wrapper.api_request(
                    "games",
                    f"f name, genres; where id = {igdb_ids_arg}; limit 100;"
            )
    genre_data = byte_to_json(byte_array)

    return genre_data


# Parses through API call output for genre data to add to the genre bridge data dictionary
# that will later be converted to a dataframe for the genre bridge dimension
def add_data_to_genre_bridge(genre_data, genre_bridge_dict, igdb_category_id_temp):
    # Iterates through each igdb id in the IGDB api output, then adds the genre data
    # associated with each one to genre_bridge_dict
    for genre_info in genre_data:
        igdb_id = genre_info["id"]
        category_id = int(igdb_category_id_temp[igdb_id])
        if "genres" not in genre_info: # if igdb game has no associated genres, add "NA"
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")
        else:
            for genre_id in genre_info["genres"]:
                genre_bridge_dict["category_id"].append(category_id)
                genre_bridge_dict["genre_id"].append(genre_id)

    # Searches through all IGDB ids that did not have any output from the API
    # Then gives them a Not Available genre in the final output
    for igdb_id, category_id in igdb_category_id_temp.items():
        if category_id not in genre_bridge_dict["category_id"]:
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")


# Makes IGDB wrapper and gets s3 client
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)
    s3_client = boto3.client('s3')

    return wrapper, s3_client


# Accesses the genre bridge dimension
def access_genre_bridge_dimension(s3_client):
    try:
        s3_client.head_object(Bucket='twitchdatapipelineproject', Key="raw/dimension_table/genre_bridge_dimension.csv")
        response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/genre_bridge_dimension.csv")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            current_genre_bridge_df = pd.read_csv(response.get("Body"), keep_default_na=False)
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
            exit()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404": # key does not exist, so we make new csv
            current_genre_bridge_df = pd.DataFrame(columns=["category_id", "genre_id"])
            wr.s3.to_csv(current_genre_bridge_df, "s3://twitchdatapipelineproject/raw/dimension_table/genre_bridge_dimension.csv", index=False)###################################
        elif e.response['Error']['Code'] == 403:
            print("Unauthorized access. Ending program.")
            exit()
        else:
            print("Something else went wrong. Ending program.")
            exit()

    return current_genre_bridge_df


# Accesses the category dimension
def access_category_dimension(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/category_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        category_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
        exit()
    
    return category_df


# Adds genre data for categories not seen before which would be updated
# in the category dimension
def add_new_genre_data(wrapper, category_df, genre_bridge_dim):
    # dict holding category and genre data not found in category_df already
    genre_bridge_dict = {
        "category_id": [],
        "genre_id": []
    }

    exclude_list = list(set(genre_bridge_dim["category_id"].tolist()))
    new_category_df = category_df[~category_df["category_id"].isin(exclude_list)].reset_index()

    # One API call accepts max 100 IGDB ids
    # To minimize num of calls made, we make one API call per 100 games
    igdb_category_id_temp = {} # temporarily store what category ids are associated with igdb ids

    for i, row in new_category_df.iterrows():
        i += 1
        igdb_id = str(row["igdb_id"])
        category_id = str(row["category_id"])

        if igdb_id != "NA":
            igdb_category_id_temp[int(float(igdb_id))] = int(float(category_id))
        else:
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")
        if i % 100 == 0 or i == len(new_category_df): # every 100 igdb games or if last one, make api call
            igdb_ids_tuple = tuple(igdb_category_id_temp.keys())
            igdb_ids_arg = str(igdb_ids_tuple)
            if len(igdb_ids_tuple) == 1:
                igdb_ids_arg = igdb_ids_arg.replace(',', '')
            genre_data = get_igdb_genre(wrapper, igdb_ids_arg)
            add_data_to_genre_bridge(genre_data, genre_bridge_dict, igdb_category_id_temp)
            igdb_category_id_temp.clear()

    return genre_bridge_dict


# Creates CSV file from the genre bridge dimension dictionary
# Updates the CSV file if it already exists with new categories and their genres
def process_dim_csv_file(genre_bridge_dim, new_genre_data_dim):
    new_genre_data_dim = pd.DataFrame(new_genre_data_dim)
    final_df = pd.concat([genre_bridge_dim, new_genre_data_dim]).drop_duplicates()
    genre_bridge_dim_path = "s3://twitchdatapipelineproject/raw/dimension_table/genre_bridge_dimension.csv"
    wr.s3.to_csv(final_df, genre_bridge_dim_path, index=False)


def lambda_handler(event, context):
    wrapper, s3_client = get_credentials()
    category_df = access_category_dimension(s3_client)
    genre_bridge_dim = access_genre_bridge_dimension(s3_client)
    new_genre_data_dim = add_new_genre_data(wrapper, category_df, genre_bridge_dim)

    # Create temporary CSV of new category genres to be uploaded to Postgres
    new_category_genre_df = pd.DataFrame(new_genre_data_dim)
    new_category_genre_path = "s3://twitchdatapipelineproject/raw/other/new_data_temp/new_category_genres.csv"

    # Upload CSV to S3
    process_dim_csv_file(genre_bridge_dim, new_genre_data_dim)

    event_payload = {
                        "table_name": "genre_bridge",
                        "new_data_path": new_category_genre_path
                    }

    # Invokes another lambda to upload data to postgres db
    lambdaClient = boto3.client('lambda')
    response = lambdaClient.invoke(
        FunctionName='arn:aws:lambda:us-west-1:484743883065:function:insertDatatoDB',
        InvocationType='Event',
        Payload=json.dumps(event_payload)
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Successful program end!')
    }