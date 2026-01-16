import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

############################ SUMMARY ############################
'''
    Processes the raw game_mode bridge data. Converts it into
    tabular format and adds the appropriate category IDs.
'''
#################################################################


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


# Gets category id associated with IGDB ID
def get_associated_category_id(category_df, igdb_id):
    category_row = category_df[category_df["igdb_id"] == str(igdb_id)]
    category_id = category_row["category_id"].iloc[0]

    return category_id



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    raw_game_mode_bridge_data_path = repo_root + f"/data/twitch_project_raw_layer/raw_game_mode_bridge_data/{day_date_id}/raw_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.json"

    # Access category dimension data
    curated_categories_path = repo_root + f"/data/twitch_project_curated_layer/curated_categories_data/{day_date_id}/curated_categories_data_{day_date_id}_{time_of_day_id}.csv"
    data_types = {
        'category_id': str, 
        'category_name': str,
        'igdb_id': str
    }
    category_df = pd.read_csv(curated_categories_path, keep_default_na=False, dtype=data_types)
    category_df = category_df.drop_duplicates(subset=["igdb_id"]).reset_index(drop=True)

    # Access raw category data
    with open(raw_game_mode_bridge_data_path, 'r') as f:
        game_mode_bridge_data = json.load(f)

    processed_game_mode_bridge_data_dict = {
        "igdb_id": [],
        "category_id": [],
        "game_name": [],
        "game_mode_id": []
    }

    # Add game_mode data to processed game_mode bridge data dict
    for game_info in game_mode_bridge_data["data"]: # some games have no associated game_modes
        if "game_modes" in game_info.keys():
            category_id = get_associated_category_id(category_df, game_info["id"])
            for game_mode_id in game_info["game_modes"]:
                processed_game_mode_bridge_data_dict["igdb_id"].append(game_info["id"])
                processed_game_mode_bridge_data_dict["category_id"].append(category_id)
                processed_game_mode_bridge_data_dict["game_name"].append(game_info["name"])
                processed_game_mode_bridge_data_dict["game_mode_id"].append(game_mode_id)

    # Convert data to dataframe
    processed_game_mode_bridge_df = pd.DataFrame(processed_game_mode_bridge_data_dict)

    # Upload CSV to processed layer
    processed_game_mode_bridge_file_path = Path(repo_root + f"/data/twitch_project_processed_layer/processed_game_mode_bridge_data/{day_date_id}/processed_game_mode_bridge_data_{day_date_id}_{time_of_day_id}.csv")
    processed_game_mode_bridge_file_path.parent.mkdir(parents=True, exist_ok=True)
    processed_game_mode_bridge_df.to_csv(processed_game_mode_bridge_file_path, index=False)



if __name__ == "__main__":
    main()


end = time.time()
print("Duration: " + str(end - start))