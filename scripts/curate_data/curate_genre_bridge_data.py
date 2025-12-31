import pandas as pd
from pathlib import Path
import time
from datetime import datetime

########################### SUMMARY ###########################
'''
    Updates the current genre bridge dimension CSV file. Looks
    through most recently collected current category genres
    and inserts them into current dimension file.
'''
###############################################################

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


# Gets recent processed genre bridge dimension data and limits it to relevant columns
def get_processed_genre_bridge_data(day_date_id, time_of_day_id):
    file_path = repo_root + f"/data/twitch_project_processed_layer/processed_genre_bridge_data/{day_date_id}/processed_genre_bridge_data_{day_date_id}_{time_of_day_id}.csv"
    processed_genre_bridge_df = pd.read_csv(file_path, keep_default_na = False)
    processed_genre_bridge_df = processed_genre_bridge_df[["category_id", "igdb_id", "genre_id"]]

    return processed_genre_bridge_df


# Gets current genre bridge dim
def get_genre_bridge_dim():
    file_path = repo_root + f"/data/twitch_project_curated_layer/curated_genre_bridge_data/curated_genre_bridge_data.csv"
    try:
        genre_bridge_dim_df = pd.read_csv(file_path, keep_default_na = False)
    except FileNotFoundError: # create new genre bridge file if it does not exist already
        with open(file_path, 'w') as f:
            f.write("category_id,igdb_id,genre_id")
        genre_bridge_dim_df = pd.read_csv(file_path, keep_default_na = False, dtype={"igdb_id": int})

    return genre_bridge_dim_df


# Adds new genre data from processed genre bridge data to the curated dimension data
# Also returns dataframe filled with new cateogory genres not seen before in original curated genre bridge dimension data
def add_new_genre_data(processed_genre_bridge_df, curated_genre_bridge_df):
    curated_genre_bridge_df = pd.concat([curated_genre_bridge_df, processed_genre_bridge_df]).drop_duplicates(subset=["category_id", "igdb_id", "genre_id"]).reset_index()
    curated_genre_bridge_df["igdb_id"] = curated_genre_bridge_df["igdb_id"].astype(int)
    curated_genre_bridge_df = curated_genre_bridge_df[["category_id", "igdb_id", "genre_id"]]
    additional_category_genres = processed_genre_bridge_df

    return curated_genre_bridge_df, additional_category_genres



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    processed_genre_bridge_df = get_processed_genre_bridge_data(day_date_id, time_of_day_id)
    curated_genre_bridge_df = get_genre_bridge_dim()

    # adds new genre data to curated genre bridge dimension file
    curated_genre_bridge_dim_df, additional_category_genres = add_new_genre_data(processed_genre_bridge_df, curated_genre_bridge_df) 
    genre_bridge_dim_file_path = repo_root + "/data/twitch_project_curated_layer/curated_genre_bridge_data/curated_genre_bridge_data.csv"
    curated_genre_bridge_dim_df.to_csv(genre_bridge_dim_file_path, index=False) # convert genre bridge dim data to CSV

    # Converts new additional category genre data to CSV and uploads to temp file which will be uploaded to postgres
    additional_category_genres.to_csv(repo_root + "/data/twitch_project_miscellaneous/temp_table_data/new_genre_bridge_data.csv", index=False)





if __name__ == "__main__":
    main()