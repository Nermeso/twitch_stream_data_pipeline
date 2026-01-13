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


# Gets recent processed category data
def get_processed_category_data(day_date_id, time_of_day_id):
    data_path = repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv"
    processed_category_df = pd.read_csv(data_path, keep_default_na=False)
    processed_category_df = processed_category_df[["category_id", "category_name", "igdb_id"]]

    return processed_category_df


# Gets the data for the categories we currently have info on already
def get_current_categories():
    data_path = repo_root + "/data/twitch_project_miscellaneous/current_data/current_categories.csv"
    try:
        current_category_df = pd.read_csv(data_path, keep_default_na = False)
    except FileNotFoundError: # create new categories file if it does not exist already
        with open(data_path, 'w') as f:
            f.write("category_id,igdb_id,category_name")
        current_category_df = pd.read_csv(data_path, keep_default_na = False)

    return current_category_df


# Adds new category data from processed category data to the current category data
# Also returns dataframe filled with new categories not seen before to the curated layer to be uploaded to postgres
def add_new_category_data(processed_category_df, current_category_df):
    new_current_category_df = pd.concat([current_category_df, processed_category_df]).drop_duplicates(subset=["category_id"], keep="first").reset_index()
    new_current_category_df = new_current_category_df[["category_id", "category_name", "igdb_id"]]

    # New additional categories are curated categories
    additional_categories_df = pd.concat([new_current_category_df.drop_duplicates(), current_category_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_current_category_df, additional_categories_df


def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    # Normally, these values will be passed by the event variable in the lambda function
    day_date_id = "20260111" # test value
    time_of_day_id = "1645" # test value

    # Gets recent processed category data
    processed_category_df = get_processed_category_data(day_date_id, time_of_day_id)

    # Gets categories we currently already have data for
    current_category_df = get_current_categories()

    # Curated category data contains new category data to be uploaded to postgres
    # Current categories is updated
    current_categories_df, curated_category_dim_df = add_new_category_data(processed_category_df, current_category_df)

    if curated_category_dim_df.empty:
        print("No new categories added to category dimension data.")
        exit()

    # Converts new additional category data to CSV and uploads to curated layer which will be uploaded to postgres
    curated_category_dim_df_path = Path(repo_root + f"/data/twitch_project_curated_layer/curated_categories_data/{day_date_id}/curated_categories_data_{day_date_id}_{time_of_day_id}.csv")
    curated_category_dim_df_path.parent.mkdir(parents=True, exist_ok=True)
    curated_category_dim_df.to_csv(curated_category_dim_df_path, index=False)

    # Update current categories
    current_categories_file_path = repo_root + "/data/twitch_project_miscellaneous/current_data/current_categories.csv"
    current_categories_df.to_csv(current_categories_file_path, index=False) 

   

if __name__ == "__main__":
    main()