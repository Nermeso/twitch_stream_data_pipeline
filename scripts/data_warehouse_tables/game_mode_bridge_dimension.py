import os
from igdb.wrapper import IGDBWrapper
import json
import pandas as pd
from pathlib import Path
import time

start = time.time()

repo_root = str(Path(__file__).parents[2])


############################## SUMMARY ##############################
'''
    This script creates data for the game mode bridge dimension.
    This table contains the category_id and game_mode_id attribute
    This script requires the category_dimension.csv file. If the 
    bridge dimension data already exists, it updates it with new
    categories not seen before and their associated game modes.
'''
#####################################################################

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)

    return output

# Incredibox category ID: 874390264
# Incredibox IGDB ID: 88940


# Calls IGDB API to get data on up to 100 games' game modes
def get_igdb_game_mode(wrapper, igdb_ids_arg):
    byte_array = wrapper.api_request(
                    "games",
                    f"f name, game_modes; where id = {igdb_ids_arg}; limit 100;"
            )
    game_mode_data = byte_to_json(byte_array)

    return game_mode_data


# Parses through API call output for game mode data to add to the game mode bridge data dictionary
# that will later be converted to a dataframe for the game mode bridge dimension
def add_data_to_game_mode_bridge(game_mode_data, game_mode_bridge_dict, igdb_category_id_temp):
    # Iterates through each igdb id in the IGDB api output, then adds the game_mode data
    # associated with each one to game_mode_bridge_dict
    for game_mode_info in game_mode_data:
        igdb_id = game_mode_info["id"]
        category_id = int(igdb_category_id_temp[igdb_id])
        if "game_modes" not in game_mode_info: # if igdb game has no associated game modes, add "NA"
            game_mode_bridge_dict["category_id"].append(category_id)
            game_mode_bridge_dict["game_mode_id"].append("NA")
        else:
            for game_mode_id in game_mode_info["game_modes"]:
                game_mode_bridge_dict["category_id"].append(category_id)
                game_mode_bridge_dict["game_mode_id"].append(game_mode_id)

    # Searches through all IGDB ids that did not have any output from the API
    # Then gives them a Not Available game mode in the final output
    for igdb_id, category_id in igdb_category_id_temp.items():
        if category_id not in game_mode_bridge_dict["category_id"]:
            game_mode_bridge_dict["category_id"].append(category_id)
            game_mode_bridge_dict["game_mode_id"].append("NA")


# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


# Accesses the game mode bridge dimension
def access_game_mode_bridge_dimension():
    game_mode_bridge_dimension_path = repo_root + "/data/data_warehouse/game_mode_bridge_dimension.csv"
    try:
        game_mode_bridge_df = pd.read_csv(game_mode_bridge_dimension_path, keep_default_na=False)
    except FileNotFoundError:
        with open(game_mode_bridge_dimension_path, 'w') as f:
            f.write('category_id,game_mode_id')

        game_mode_bridge_df = pd.read_csv(game_mode_bridge_dimension_path, keep_default_na=False)

    return game_mode_bridge_df


# Accesses the category dimension
def access_category_dimension():
    category_dimension_path = repo_root + "/data/data_warehouse/category_dimension.csv"
    category_df = pd.read_csv(category_dimension_path, keep_default_na=False)

    return category_df


# Adds game mode data for categories not seen before which would be updated
# in the category dimension
def add_new_game_mode_data(wrapper, category_df, game_mode_bridge_dim):
    # dict holding category and game mode data not found in category_df already
    game_mode_bridge_dict = {
        "category_id": [],
        "game_mode_id": []
    }

    exclude_list = game_mode_bridge_dim["category_id"].tolist()
    new_category_df = category_df[~category_df["category_id"].isin(exclude_list)].reset_index()

    # One API call accepts max 100 IGDB ids
    # To minimize num of calls made, we make one API call per 100 games
    igdb_category_id_temp = {} # temporarily store what category ids are associated with igdb ids
    for i, row in new_category_df.iterrows():
        i += 1
        igdb_id = str(row["igdb_id"])
        category_id = str(row["category_id"])

        if igdb_id != "NA":
            igdb_category_id_temp[int(float(igdb_id))] = int(float(category_id))
        else:
            game_mode_bridge_dict["category_id"].append(category_id)
            game_mode_bridge_dict["game_mode_id"].append("NA")
        if i % 100 == 0 or i == len(new_category_df): # every 100 igdb games or if last one, make api call
            igdb_ids_tuple = tuple(igdb_category_id_temp.keys())
            igdb_ids_arg = str(igdb_ids_tuple)
            if len(igdb_ids_tuple) == 1:
                igdb_ids_arg = igdb_ids_arg.replace(',', '')
            game_mode_data = get_igdb_game_mode(wrapper, igdb_ids_arg)
            add_data_to_game_mode_bridge(game_mode_data, game_mode_bridge_dict, igdb_category_id_temp)
            igdb_category_id_temp.clear()
            
    return game_mode_bridge_dict


# Creates CSV file from the game mode bridge dimension dictionary
# Updates the CSV file if it already exists with new categories and their game modes
def process_dim_csv_file(game_mode_bridge_dim, new_game_mode_data_dim):
    new_game_mode_data_dim = pd.DataFrame(new_game_mode_data_dim)
    final_df = pd.concat([game_mode_bridge_dim, new_game_mode_data_dim]).drop_duplicates()
    game_mode_bridge_dim_path = repo_root + "/data/data_warehouse/game_mode_bridge_dimension.csv"
    final_df.to_csv(game_mode_bridge_dim_path, index=False)


def main():
    wrapper = make_wrapper()
    category_df = access_category_dimension()
    game_mode_bridge_dim = access_game_mode_bridge_dimension()
    new_game_mode_data_dim = add_new_game_mode_data(wrapper, category_df, game_mode_bridge_dim)
    process_dim_csv_file(game_mode_bridge_dim, new_game_mode_data_dim)


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))