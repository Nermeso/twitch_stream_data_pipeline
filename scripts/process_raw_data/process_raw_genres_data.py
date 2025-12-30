import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################# SUMMARY #################################
'''
    This script processes the raw category data by converting it into a 
    CSV file. Slight modifications will also be made such as converting 
    empty IGDB values to "NA".
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



def main():
    raw_genre_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_genres_data/raw_genres_data.json"

    # Access raw category data
    with open(raw_genre_data_path, 'r') as f:
        genre_data = json.load(f)

    genre_df = pd.DataFrame(genre_data["data"]).drop_duplicates() # convert to dataframe
    genre_df = genre_df.rename(columns = {"id": "genre_id", "name": "genre_name"}) # rename columns

    # Upload CSV to processed layer
    processed_genre_file_path = repo_root + f"/data/twitch_project_processed_layer/processed_genres_data/processed_genre_data.csv"
    genre_df.to_csv(processed_genre_file_path, index=False)



if __name__ == "__main__":
    main()