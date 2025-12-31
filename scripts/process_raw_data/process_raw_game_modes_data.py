import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################# SUMMARY #################################
'''
    This script processes the raw game mode data by converting it into a 
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



def main():
    raw_game_mode_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_game_modes_data/raw_game_modes_data.json"

    # Access raw category data
    with open(raw_game_mode_data_path, 'r') as f:
        game_mode_data = json.load(f)

    game_mode_df = pd.DataFrame(game_mode_data["data"]).drop_duplicates() # convert to dataframe
    game_mode_df = game_mode_df.rename(columns = {"id": "game_mode_id", "name": "game_mode_name"}) # rename columns

    # Upload CSV to processed layer
    processed_game_mode_file_path = repo_root + f"/data/twitch_project_processed_layer/processed_game_modes_data/processed_game_modes_data.csv"
    game_mode_df.to_csv(processed_game_mode_file_path, index=False)



if __name__ == "__main__":
    main()