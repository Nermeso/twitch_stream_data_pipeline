import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time
import os

################################# SUMMARY #################################
'''
    This script processes the raw stream data by combining the JSON files
    into one big CSV file. Slight modifications will be made to columns
    and some values.
'''
###########################################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])

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


# Checks if string can be valid number or not
def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


# Process language id depending on the data
def process_language_id(language_id):
    if language_id == "":
        return "notavailable"
    else:
        return language_id



# Converts the raw stream data in JSON format to a dataframe
# Removes some data since it wouldn't fit in tabular format
def process_raw_stream_data(raw_stream_data, processed_stream_data_dict):
    for stream in raw_stream_data["data"]:
        # Check to see if stream is valid, sometimes there are test streams where stream id and user id are weird
        if is_integer(stream["id"]) and is_integer(stream["user_id"]):
            processed_stream_data_dict["id"].append(stream["id"])
            processed_stream_data_dict["user_id"].append(stream["user_id"])
        else:
            continue
        processed_stream_data_dict["user_login"].append(stream["user_login"])
        processed_stream_data_dict["user_name"].append(stream["user_name"])
        processed_stream_data_dict["game_id"].append(stream["game_id"])
        processed_stream_data_dict["game_name"].append(stream["game_name"])
        processed_stream_data_dict["title"].append(stream["title"])
        processed_stream_data_dict["viewer_count"].append(stream["viewer_count"])
        processed_stream_data_dict["started_at"].append(stream["started_at"])
        processed_stream_data_dict["language"].append(process_language_id(stream["language"]))
        processed_stream_data_dict["thumbnail_url"].append(stream["thumbnail_url"])
        processed_stream_data_dict["is_mature"].append(stream["is_mature"])


def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20260117"
    time_of_day_id = "1200"

    processed_stream_data_dict = {
            "id": [],
            "user_id": [],
            "user_login": [],
            "user_name": [],
            "game_id": [],
            "game_name": [],
            "title": [],
            "viewer_count": [],
            "started_at": [],
            "language": [],
            "thumbnail_url": [],
            "is_mature": []
        }

    # Reads all raw stream files of a certain time period and adds it to data dictinoary
    stream_data_directory = repo_root + f"/data/twitch_project_raw_layer/raw_streams_data/{day_date_id}_{time_of_day_id}/"
    for data_file_name in os.listdir(stream_data_directory):
        raw_stream_data_path = stream_data_directory + data_file_name
        
        # Access raw stream data
        with open(raw_stream_data_path, 'r') as f:
            raw_stream_data = json.load(f)
            process_raw_stream_data(raw_stream_data, processed_stream_data_dict)    

    # Drop duplicate streams
    processed_stream_df = pd.DataFrame(processed_stream_data_dict).drop_duplicates(subset=["id"], keep="first")

    # Upload CSV to processed layer
    processed_category_file_path = Path(repo_root + f"/data/twitch_project_processed_layer/processed_streams_data/{day_date_id}/processed_streams_data_{day_date_id}_{time_of_day_id}.csv")
    processed_category_file_path.parent.mkdir(parents=True, exist_ok=True)
    processed_stream_df.to_csv(processed_category_file_path, index=False)


if __name__ == "__main__":
    main()