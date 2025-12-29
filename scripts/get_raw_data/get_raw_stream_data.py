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
def get_day_date_id():
    # Gets date id
    date_dim_path = repo_root + "/data/twitch_project_raw_layer/raw_day_dates_data/raw_day_dates_data.csv"
    date_df = pd.read_csv(date_dim_path)
    current_date = datetime.today()
    day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
   
    return str(day_date_id)


# Gets time of day id based off of current time of script execution
def get_time_of_day_id():
    time_of_day_df = pd.read_csv(repo_root + "/data/twitch_project_raw_layer/raw_time_of_day_data/raw_time_of_day_data.csv")
    cur_date = datetime.today()
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


# Expected input would be a batch of SQS messages in JSON format
# Obtains category data that would later be processed
def get_categories_to_process():
    categories_path = repo_root + "/data/sample_data/SQS_batch_event_input/example_SQS_batch_event_input1.json"
    categories_to_process = []
    with open(categories_path, 'r') as f:
        message_batch = json.load(f)
        for message in message_batch["Records"]:
            category_list = ast.literal_eval(message["body"])
            categories_to_process.extend(category_list)

    return set(categories_to_process)


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Calls Get Stream Twitch API to get stream data for specified categories
def get_raw_stream_data_from_API(raw_stream_data, category_set, headers):
    cursor = ""
    while cursor != "end":
        params = {
            "game_id": list(category_set),
            "first": 100,
            "after": cursor
        }
        # Calls API to get data for 100 categories
        while True:
            try:
                response = requests.get("https://api.twitch.tv/helix/streams", headers=headers, params=params)
                break
            except ConnectionError as e:
                print(e)
                continue
        
        output = response.json()
        raw_stream_data["data"].extend(output["data"])

        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "end"
        else:    
            cursor = output["pagination"]["cursor"]


def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()
    categories_to_process = get_categories_to_process()
    headers = get_credentials()

    raw_stream_data = {
        "day_date_id": day_date_id,
        "time_of_day_id": time_of_day_id,
        "data": []
    }

    # Calls Twitch's Get Streams API for every 100 categories since 100 is max categories to pass in parameter
    # Counts as one API request, minimizing API request number to better adhere to rate limits
    category_set = set()
    for i, category_id in enumerate(categories_to_process):
        ith_category = i + 1
        category_set.add(category_id)
        # Process categories in batches of 100 while including the last non-100 batch
        if (ith_category % 100 == 0) or len(categories_to_process) == ith_category:
            get_raw_stream_data_from_API(raw_stream_data, category_set, headers)
            category_set = set()

    # Write the raw category data to json file
    file_path = f"data/twitch_project_raw_layer/raw_streams_data/raw_stream_data_{day_date_id}_{time_of_day_id}.json"
    with open(file_path, 'w') as json_file:
        json.dump(raw_stream_data, json_file, indent=4)


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))