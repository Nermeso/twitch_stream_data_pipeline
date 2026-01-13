import pandas as pd
from pathlib import Path
import time
from datetime import datetime

########################### SUMMARY ###########################
'''
    Updates the current user dimension CSV file.
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


# Gets recent processed user data
def get_processed_user_data(day_date_id, time_of_day_id):
    data_path = repo_root + f"/data/twitch_project_processed_layer/processed_users_data/{day_date_id}/processed_users_data_{day_date_id}_{time_of_day_id}.csv"
    processed_user_df = pd.read_csv(data_path, keep_default_na=False)
    processed_user_df = processed_user_df[["id", "login", "display_name", "broadcaster_type"]]

    return processed_user_df


# Gets the current user dimension data
def get_user_dim_info():
    user_dim_path = repo_root + "/data/twitch_project_curated_layer/curated_users_data/curated_users_data.csv"
    try:
        user_dim_df = pd.read_csv(user_dim_path, keep_default_na = False)
    except FileNotFoundError: # create new users file if it does not exist already
        with open(user_dim_path, 'w') as f:
            f.write("user_id,user_name,login_name,broadcaster_type")
        user_dim_df = pd.read_csv(user_dim_path, keep_default_na = False)

    return user_dim_df


# Gets current users we have info for already
def get_current_users():
    current_user_path = repo_root + "/data/twitch_project_miscellaneous/current_data/current_users.csv"
    try:
        current_user_df = pd.read_csv(current_user_path, keep_default_na = False)
    except FileNotFoundError: # create new current users file if it does not exist already
        current_user_df = pd.DataFrame(columns=["user_id", "user_name", "login_name", "broadcaster_type"])

    return current_user_df


# Adds new user data from processed user data to the current users data
# Also returns dataframe filled with new users not seen before in to the curated layer to be uploaded to the postgres DB
def add_new_user_data(processed_user_df, current_user_df):
    new_current_user_df = pd.concat([current_user_df, processed_user_df]).drop_duplicates(subset=["user_id"], keep="first").reset_index()
    new_current_user_df = new_current_user_df[["user_id", "user_name", "login_name", "broadcaster_type"]]

    # New additional users not seen before will be uploaded as curated data to curated layer
    additional_users_df = pd.concat([new_current_user_df.drop_duplicates(), current_user_df.drop_duplicates()]).drop_duplicates(keep=False).reset_index(drop=True)

    return new_current_user_df, additional_users_df



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20260111" # test
    time_of_day_id = "1715" # test

    processed_user_df = get_processed_user_data(day_date_id, time_of_day_id)
    
    # Change column names
    processed_user_df = processed_user_df.rename(columns={
        "id": "user_id",
        "display_name": "user_name",
        "login": "login_name"
    })

    current_users_df = get_current_users()

    # Curated user data contains new user data to be uploaded to postgres
    # Current users is updated
    current_users_df, curated_users_df = add_new_user_data(processed_user_df, current_users_df) 

    if curated_users_df.empty:
        print("No new users added to user dimension data.")
        exit()

    # Converts new additional user data to CSV and uploads to curated layer which will be uploaded to postgres
    curated_user_file_path = Path(repo_root + f"/data/twitch_project_curated_layer/curated_users_data/{day_date_id}/curated_users_data_{day_date_id}_{time_of_day_id}.csv")
    curated_user_file_path.parent.mkdir(parents=True, exist_ok=True)
    curated_users_df.to_csv(curated_user_file_path, index=False) # convert curated user data to CSV to be uploaded to postgres

    # Updates the current users we have data for already
    current_users_file_path = repo_root + "/data/twitch_project_miscellaneous/current_data/current_users.csv"
    current_users_df.to_csv(current_users_file_path, index=False)


if __name__ == "__main__":
    main()