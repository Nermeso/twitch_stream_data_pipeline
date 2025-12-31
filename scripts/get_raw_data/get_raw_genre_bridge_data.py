import os
from igdb.wrapper import IGDBWrapper
import json
import pandas as pd
from pathlib import Path
import time
from datetime import datetime


############################## SUMMARY ##############################
'''
    This script calls the IGDB API to get raw genre data for
    for categories in the category dimension.
'''
#####################################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])


# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


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
    print(time_of_day_df)
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


# Accesses the genre bridge dimension
def access_genre_bridge_dimension():
    genre_bridge_dimension_path = repo_root +  "/data/twitch_project_curated_layer/curated_genre_bridge_data/curated_genre_bridge_data.csv"
    try:
        genre_bridge_df = pd.read_csv(genre_bridge_dimension_path, keep_default_na=False)
    except FileNotFoundError:
        with open(genre_bridge_dimension_path, 'w') as f:
            f.write('category_id,genre_id')

        genre_bridge_df = pd.read_csv(genre_bridge_dimension_path, keep_default_na=False)

    return genre_bridge_df


# Get raw genre data for new categories
def get_raw_genre_data(wrapper, category_df, genre_bridge_df, raw_category_genre_data_dict):
    exclude_list = genre_bridge_df["category_id"].tolist()
    new_category_df = category_df[~category_df["category_id"].isin(exclude_list)].reset_index() # only include categories that we did not get genre data on yet

    # One API call accepts max 100 IGDB ids
    # To minimize num of calls made, we make one API call per 100 games
    igdb_category_id_temp = {} # temporarily store what category ids are associated with igdb ids
    for i, row in new_category_df.iterrows():
        i += 1
        igdb_id = str(row["igdb_id"])
        category_id = str(row["category_id"])

        if igdb_id != "NA": # Only consider categories that have an associated IGDB ID
            igdb_category_id_temp[int(float(igdb_id))] = int(float(category_id))
        if i % 100 == 0 or i == len(new_category_df): # every 100 igdb games or if last one, make api call
            igdb_ids_tuple = tuple(igdb_category_id_temp.keys())
            igdb_ids_arg = str(igdb_ids_tuple)
            if len(igdb_ids_tuple) == 1:
                igdb_ids_arg = igdb_ids_arg.replace(',', '')
            raw_genre_data = get_igdb_genre(wrapper, igdb_ids_arg) # Calls api to get data
            raw_category_genre_data_dict["data"].extend(raw_genre_data)
            igdb_category_id_temp.clear()



# Calls IGDB API to get data on up to 100 games' genres
def get_igdb_genre(wrapper, igdb_ids_arg):
    byte_array = wrapper.api_request(
                    "games",
                    f"f name, genres; where id = {igdb_ids_arg}; limit 100;"
            )

    my_json = byte_array.decode('utf8').replace("'", '"')
    genre_data = json.loads(my_json)
            
    return genre_data


def main():
    wrapper = make_wrapper() # wrapper to be used to call IGDB API
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    print(time_of_day_id)
    exit()

    # Get curated category data
    category_dimension_path = repo_root + "/data/twitch_project_curated_layer/curated_categories_data/curated_categories_data.csv"
    category_df = pd.read_csv(category_dimension_path, keep_default_na=False)

    # Get genre bridge data
    genre_bridge_df = access_genre_bridge_dimension()

    raw_category_genre_data_dict = {
        "day_date_id": day_date_id,
        "time_of_day_id": time_of_day_id,
        "data": []
    }

    # Get new genre data
    get_raw_genre_data(wrapper, category_df, genre_bridge_df, raw_category_genre_data_dict)

    # Write the raw category genre data to json file
    output_file_path = Path(repo_root + f"/data/twitch_project_raw_layer/raw_genre_bridge_data/{day_date_id}/raw_genre_bridge_data_{day_date_id}_{time_of_day_id}.json")
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file_path, 'w') as json_file:
        json.dump(raw_category_genre_data_dict, json_file, indent=4)


   
    


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))