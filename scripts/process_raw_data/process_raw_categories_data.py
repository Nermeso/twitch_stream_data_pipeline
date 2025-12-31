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
    raw_category_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_categories_data/{day_date_id}/raw_categories_data_{day_date_id}_{time_of_day_id}.json"

    # Access raw category data
    with open(raw_category_data_path, 'r') as f:
        category_data = json.load(f)

    category_df = pd.DataFrame(category_data["data"]).drop_duplicates() # convert to dataframe
    category_df = category_df.rename(columns = {"id": "category_id", "name": "category_name"}) # rename columns

    # Replace empty strings with "NA"
    category_df["igdb_id"] = category_df["igdb_id"].replace("", "NA")
    category_df["box_art_url"] = category_df["box_art_url"].replace("", "NA")

    # Upload CSV to processed layer
    processed_category_file_path = Path(repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv")
    processed_category_file_path.parent.mkdir(parents=True, exist_ok=True)
    category_df.to_csv(processed_category_file_path, index=False)



if __name__ == "__main__":
    main()