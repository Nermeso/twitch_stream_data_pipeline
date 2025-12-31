import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################ SUMMARY ################################
'''
    This script calls the "Get Top Games" Twitch endpoint to return
    currently streamed categories at the time of script execution. The
    output will be a JSON file containing category data The goal is to get
    information on all categories that Twitch has available.
'''
#########################################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


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
    time_of_day_df = pd.read_csv(repo_root + "/data/twitch_project_raw_layer/raw_time_of_day_data/raw_time_of_day_data.csv", dtype={"time_of_day_id": str})
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

    return str(time_of_day_id)


# Calls the "Get Top Games" Twitch endpoint to get data on currently streamed categories
# Returns raw API JSON output of categories 
def call_get_top_games_endpoint(headers, raw_category_data):
    # Twitch uses cursor-based pagination to show API results
    # Will need cursor value in one API call to get next set of results in other API call
    cursor = ""
    while cursor != "done":
        params = {
            "first": 100, # Can get max of 100 items in each API call
            "after": cursor
        }
        response = requests.get("https://api.twitch.tv/helix/games/top", headers=headers, params=params)
        output = response.json()
        raw_category_data["data"].extend(output["data"])

        # Ends pagination of pages when done
        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "done"
        else:
            cursor = output["pagination"]["cursor"]
        


def main():
    headers = get_credentials() # gets access token and client id needed to call Twitch API
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    raw_category_data = {
                    "day_date_id": day_date_id,
                    "time_of_day_id": time_of_day_id,
                    "data": []
                }

    # Do API calling twice to make sure no category is skipped over
    for _ in range(0, 2):
        # Sometimes calling API leads to connection error as a result of DNS issues
        # Loop aims to restart process if that happens
        while True:
            try:
                # Calls Twitch's "Get Top Games" endpoint to get currently streamed categories and category data we have not collected yet
                call_get_top_games_endpoint(headers, raw_category_data)
                break
            except ConnectionError as e:
                print(e)
                continue
    
    # Write the raw category data to json file
    output_file_path = Path(repo_root + f"/data/twitch_project_raw_layer/raw_categories_data/{day_date_id}/raw_categories_data_{day_date_id}_{time_of_day_id}.json")
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file_path, 'w') as json_file:
        json.dump(raw_category_data, json_file, indent=4)


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))