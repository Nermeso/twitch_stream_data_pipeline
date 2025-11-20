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


# Calls Get Stream Twitch API to get stream data for specified categories
def get_data_from_API(stream_data_dict, category_set):
    category_runtime = {"category_id": [], "runtime": []}
    headers = get_credentials()
    # For each category, calls stream api, gets output, then processes the data to be added to stream_data_dict
    for category_id in category_set:
        start1 = time.time()
        while True:
            try:
                stream_id_list, user_id_list, category_id_list, viewer_count_list, language_id_list, user_name_list, runtime = get_streams(category_id, headers)
                stream_data_dict["stream_id"].extend(stream_id_list)
                stream_data_dict["user_id"].extend(user_id_list)
                stream_data_dict["category_id"].extend(category_id_list)
                stream_data_dict["viewer_count"].extend(viewer_count_list)
                stream_data_dict["language_id"].extend(language_id_list)
                stream_data_dict["user_name"].extend(user_name_list)
                break
            except ConnectionError as e:
                print(e)
                continue
        end1 = time.time()
        duration = end1 - start1
        category_runtime["category_id"].append(category_id)
        category_runtime["runtime"].append(duration)

    return category_runtime

        

# Calls the Get Streams endpoint to collect stream data for a specific category
def get_streams(category_id, headers):
    url = "https://api.twitch.tv/helix/streams"
    stream_id_list = []; user_id_list = []; category_id_list = []
    viewer_count_list = []; language_id_list = []; user_name_list = []

    start_time = time.time()
    cursor = ""
    while cursor != "end":
        params = {
            "game_id": category_id,
            "first": 100,
            "after": cursor
        }
        response = requests.get(url, headers=headers, params=params)
        output = response.json()
        for stream in output["data"]:
            if stream["id"] not in stream_id_list:
                stream_id_list.append(stream["id"])
                user_id_list.append(stream["user_id"])
                category_id_list.append(stream["game_id"])
                viewer_count_list.append(stream["viewer_count"])
                language_id_list.append(stream["language"])
                user_name_list.append(stream["user_name"])
            else: # Do not include repeat streams
                continue

        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "end"
        else:    
            cursor = output["pagination"]["cursor"]

    end_time = time.time()
    runtime = end_time - start_time

    return stream_id_list, user_id_list, category_id_list, viewer_count_list, language_id_list, user_name_list, runtime


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
    categories_path = repo_root + "/data/dummy_data/example_SQS_batch_event_input.json"
    categories_to_process = []
    with open(categories_path, 'r') as f:
        message_batch = json.load(f)
        for message in message_batch["Records"]:
            category_list = ast.literal_eval(message["body"])
            categories_to_process.extend(category_list)

    return set(categories_to_process)


# Gets stream data for specified categories
def get_category_stream_data(category_set):
    stream_data_dict = {
        "stream_id": [],
        "date_day_id": [],
        "time_of_day_id": [],
        "user_id": [],
        "category_id": [],
        "viewer_count": [],
        "language_id": [],
        "user_name": []
    }

    # Call Twitch API to get data for streams and runtime for each category
    category_runtime = get_data_from_API(stream_data_dict, category_set)

    num_of_streams = len(stream_data_dict["stream_id"])
    cur_date_id= get_date_id() # get date id using date dimension
    cur_time_key = get_time_key() # gets time key
    stream_data_dict["date_day_id"].extend(num_of_streams * [cur_date_id])
    stream_data_dict["time_of_day_id"].extend(num_of_streams * [cur_time_key])
    stream_df = pd.DataFrame(stream_data_dict).drop_duplicates()
    category_runtime_df = pd.DataFrame(category_runtime).sort_values(by=['runtime'], ascending=False).drop_duplicates()

    return stream_df, category_runtime_df



def main():
    categories_to_process = get_categories()
    stream_df, category_runtime_df = get_category_stream_data(categories_to_process)
    stream_df.to_csv(repo_root + "/data/dummy_data/fact_table_data.csv", index=False)
    category_runtime_df.to_csv(repo_root + "/data/dummy_data/category_runtime_data.csv", index=False)


if __name__ == "__main__":
    main()


end = time.time()
print("Duration: " + str(end - start))