import os
import requests
import json
import ast
from requests.exceptions import ConnectionError
from pathlib import Path
from datetime import datetime
import pandas as pd
import time

start = time.time()

repo_root = str(Path(__file__).parents[2])


###################################### SUMMARY #####################################
'''
    This script collects stream data for specified categories.
'''
####################################################################################


# Gets current date id based of date when script is executed
def get_date_id():
    # Gets date id
    date_dim_path = repo_root + "/data/dimension_tables/date_dimension.csv"
    date_df = pd.read_csv(date_dim_path)
    current_date = datetime.today()
    date_id = date_df[date_df["OurDate"] == str(current_date.date())].iloc[0, 0]
   
    return str(date_id)


# Gets time key based off of current time of script execution
def get_time_key():
    time_of_day_df = pd.read_csv(repo_root + "/data/dimension_tables/time_of_day_dimension.csv")
    cur_date = datetime.today()
    minimum_diff = 1000
    time_key = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(cur_date.year, cur_date.month, cur_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((cur_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_key = row[1]["time_key"]

    return time_key

#################################################################################################

# Calls Get Stream Twitch API to get stream data for specified categories
def get_data_from_API(stream_data_dict, category_set, headers):
    cursor = ""
    while cursor != "end":
        params = {
            "game_id": list(category_set),
            "first": 100,
            "after": cursor
        }
        # Calls API to get data for 100 categories
        response = requests.get("https://api.twitch.tv/helix/streams", headers=headers, params=params)
        output = response.json()
        for stream in output["data"]:
            # Adds stream data to stream data dictionary
            if stream["id"] not in stream_data_dict["stream_id"]:
                stream_data_dict["stream_id"].append(stream["id"])
                stream_data_dict["user_id"].append(stream["user_id"])
                stream_data_dict["category_id"].append(stream["game_id"])
                stream_data_dict["category_name"].append(stream["game_name"])
                stream_data_dict["viewer_count"].append(stream["viewer_count"])
                stream_data_dict["language_id"].append(stream["language"])
                stream_data_dict["user_name"].append(stream["user_name"])


        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "end"
        else:    
            cursor = output["pagination"]["cursor"]


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Gets categories this script will be getting stream data for
def get_categories():
    categories_path = repo_root + "/data/miscellaneous/example_SQS_batch_event_input.json"
    categories_to_process = []
    with open(categories_path, 'r') as f:
        message_batch = json.load(f)
        for message in message_batch["Records"]:
            category_list = ast.literal_eval(message["body"])
            categories_to_process.extend(category_list)

    return set(categories_to_process)


# Adds date and time values to data dictionary
def add_date_time_data(stream_data_dict):
    cur_date_id= get_date_id() # get date id using date dimension
    cur_time_key = get_time_key() # gets time key using time dimension
    num_of_streams = len(stream_data_dict["stream_id"])
    stream_data_dict["date_day_id"].extend(num_of_streams * [cur_date_id])
    stream_data_dict["time_of_day_id"].extend(num_of_streams * [cur_time_key])


def main():
    categories_to_process = get_categories()
    stream_data_dict = {
        "stream_id": [],
        "date_day_id": [],
        "time_of_day_id": [],
        "user_id": [],
        "category_id": [],
        "category_name": [],
        "viewer_count": [],
        "language_id": [],
        "user_name": []
    }
    headers = get_credentials()

    # Calls Twitch's Get Streams API for every 100 categories since 100 is max
    # Counts as one API request, minimizing API request number to better adhere to rate limits
    category_set = set()
    for i, category_id in enumerate(categories_to_process):
        ith_category = i + 1
        category_set.add(category_id)
        # Process categories in batches of 100 while including the last non-100 batch
        if (ith_category % 100 == 0) or len(categories_to_process) == ith_category:
            get_data_from_API(stream_data_dict, category_set, headers)
            category_set = set()
    
    # Add date and time values to data
    add_date_time_data(stream_data_dict)

    # Convert stream dict to dataframe
    stream_df = pd.DataFrame(stream_data_dict).drop_duplicates()

    # Group categories to get number of streamers for each one
    category_popularity_df = stream_df.groupby(["category_id"], as_index=False).agg(
                                        category_id=('category_id', 'first'),
                                        category_name=('category_name', 'first'),
                                        num_of_streamers=('stream_id', 'count')
                                   ).sort_values(by="num_of_streamers", ascending=False)

    # Convert dataframes to CSVs
    stream_df.to_csv(repo_root + "/data/fact_table_data/recent_stream_data/fact_table_data1.csv", index=False)
    category_popularity_df.to_csv(repo_root + "/data/fact_table_data/recent_category_popularity_data/category_popularity_data1.csv", index=False)


if __name__ == "__main__":
    main()


end = time.time()
print("Duration: " + str(end - start))