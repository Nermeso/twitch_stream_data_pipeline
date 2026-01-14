import pandas as pd
from pathlib import Path
import time
from datetime import datetime

########################### SUMMARY ###########################
'''
    Produces curated game_mode_bridge data which only includes
    columns category_id, igdb_id, and game_mode_id
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



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20260111" # test value
    time_of_day_id = "1645" # test value

    # Get processed game_mode bridge dataframe
    file_path = repo_root + f"/data/twitch_project_processed_layer/processed_game_mode_bridge_data/{day_date_id}/processed_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.csv"
    processed_game_mode_bridge_df = pd.read_csv(file_path, keep_default_na = False)

    # Curate processed data to only include relevant data
    curated_game_mode_bridge_df = processed_game_mode_bridge_df[["category_id", "game_mode_id"]]
    curated_game_mode_bridge_df = curated_game_mode_bridge_df.drop_duplicates(subset=["category_id", "game_mode_id"]).reset_index(drop=True)

    # Convert game_mode bridge data to CSV and upload it
    game_mode_bridge_dim_file_path = Path(repo_root + f"/data/twitch_project_curated_layer/curated_game_mode_bridge_data/{day_date_id}/curated_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.csv")
    game_mode_bridge_dim_file_path.parent.mkdir(parents=True, exist_ok=True)
    curated_game_mode_bridge_df.to_csv(game_mode_bridge_dim_file_path, index=False) # convert game_mode bridge dim data to CSV



if __name__ == "__main__":
    main()