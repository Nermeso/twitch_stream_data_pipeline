import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################# SUMMARY #################################
'''
    This script processes the raw stream data by converting it into a 
    CSV file.
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


# Converts the raw stream data in JSON format to a dataframe
# Removes some data since it wouldn't fit in tabular format
def process_raw_stream_data(raw_stream_data, processed_stream_data_dict):
    for stream in raw_stream_data["data"]:
        processed_stream_data_dict["id"].append(stream["id"])
        processed_stream_data_dict["user_id"].append(stream["user_id"])
        processed_stream_data_dict["user_login"].append(stream["user_login"])
        processed_stream_data_dict["user_name"].append(stream["user_name"])
        processed_stream_data_dict["game_id"].append(stream["game_id"])
        processed_stream_data_dict["game_name"].append(stream["game_name"])
        processed_stream_data_dict["title"].append(stream["title"])
        processed_stream_data_dict["viewer_count"].append(stream["viewer_count"])
        processed_stream_data_dict["started_at"].append(stream["started_at"])
        processed_stream_data_dict["language"].append(stream["language"])
        processed_stream_data_dict["thumbnail_url"].append(stream["thumbnail_url"])
        processed_stream_data_dict["is_mature"].append(stream["is_mature"])
    
    processed_stream_df = pd.DataFrame(processed_stream_data_dict)

    return processed_stream_df



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20251229" # test value
    time_of_day_id = "1115" # test value

    raw_category_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_streams_data/raw_stream_data_{day_date_id}_{time_of_day_id}.json"

    # Access raw category data
    with open(raw_category_data_path, 'r') as f:
        raw_stream_data = json.load(f)

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

    # Convert raw stream json to dataframe and make slight modifications
    processed_stream_df = process_raw_stream_data(raw_stream_data, processed_stream_data_dict)    

    print(processed_stream_df)

    # # Upload CSV to processed layer
    # processed_category_file_path = repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/processed_category_data_{day_date_id}_{time_of_day_id}.csv"
    # category_df.to_csv(processed_category_file_path, index=False)



if __name__ == "__main__":
    main()