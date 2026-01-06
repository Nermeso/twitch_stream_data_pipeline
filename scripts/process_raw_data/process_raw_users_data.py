import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################# SUMMARY #################################
'''
    This script processes the raw user data by converting it into a 
    CSV file. Slight modifications will also be made such as converting 
    empty values to some default value.
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
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20251229" # test value
    time_of_day_id = "1745" # test value

    raw_user_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_users_data/{day_date_id}/raw_users_data_{day_date_id}_{time_of_day_id}.json"

    # Access raw user data
    with open(raw_user_data_path, 'r') as f:
        raw_user_data = json.load(f)


    # Convert to dataframe and remove useless columns
    user_df = pd.DataFrame(raw_user_data["data"]).drop_duplicates()
    user_df = user_df.drop(columns=["view_count"]) # view count column is depredated

    # Replace empty strings with relevant value
    user_df["type"] = user_df["type"].replace("", "normal")
    user_df["broadcaster_type"] = user_df["broadcaster_type"].replace("", "normal")

    # Upload CSV to processed layer
    processed_user_file_path = Path(repo_root + f"/data/twitch_project_processed_layer/processed_users_data/{day_date_id}/processed_users_data_{day_date_id}_{time_of_day_id}.csv")
    processed_user_file_path.parent.mkdir(parents=True, exist_ok=True)
    user_df.to_csv(processed_user_file_path, index=False)



if __name__ == "__main__":
    main()