import pandas as pd
from pathlib import Path
import time
from datetime import datetime

########################### SUMMARY ###########################
'''
    Updates the current category dimension CSV file. Looks
    through most recently collected current category data
    and adds categories not already present in the data.
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


# Gets recent processed category data
def get_processed_category_data(day_date_id, time_of_day_id):
    data_path = repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/processed_category_data_{day_date_id}_{time_of_day_id}.csv"
    processed_category_df = pd.read_csv(data_path, keep_default_na=False)

    return processed_category_df


# Gets the current category dimension data
def get_category_dim_info():
    category_dim_path = repo_root + "/data/twitch_project_curated_layer/curated_category_data/official_categories_data.csv"
    try:
        category_dim_df = pd.read_csv(category_dim_path, keep_default_na = False)
    except FileNotFoundError:
        with open(category_dim_path, 'w') as f:
            f.write("category_id,igdb_id,category_name")
        category_dim_df = pd.read_csv(category_dim_path, keep_default_na = False)

    return category_dim_df


# Adds new category data from processed category data to the official dimension data
def add_new_category_data(processed_category_df, category_dim_df):
    official_category_dim_df = pd.concat([category_dim_df, processed_category_df]).drop_duplicates(subset=["category_id"]).reset_index()
    official_category_dim_df = official_category_dim_df[["category_id", "category_name", "igdb_id"]]
    additional_categories_df = processed_category_df.merge(
            category_dim_df,
            how="left",
            indicator=True
        ).query('_merge == "left_only"').drop(columns='_merge')

    return official_category_dim_df, additional_categories_df

def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()
    day_date_id = "20251228" # testing value
    time_of_day_id = "2100" # testing value
    processed_category_df = get_processed_category_data(day_date_id, time_of_day_id)
    category_dim_df = get_category_dim_info() # gets current category dimension data
    official_category_dim_df, additional_categories_df = add_new_category_data(processed_category_df, category_dim_df)
    print(official_category_dim_df)


if __name__ == "__main__":
    main()