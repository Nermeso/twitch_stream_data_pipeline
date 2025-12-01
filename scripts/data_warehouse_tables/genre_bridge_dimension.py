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
    This script creates data for the genre bridge dimension.
    This table contains the category_id and genre_id attribute
    This script requires the category_dimension.csv file. If the 
    bridge dimension data already exists, it updates it with new
    categories not seen before and their associated genres.
'''
#####################################################################

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)

    return output


# Calls IGDB API to get data on up to 100 games' genres
def get_igdb_genre(wrapper, igdb_ids_arg):
    byte_array = wrapper.api_request(
                    "games",
                    f"f name, genres; where id = {igdb_ids_arg}; limit 100;"
            )
    genre_data = byte_to_json(byte_array)

    return genre_data


# Parses through API call output for genre data to add to the genre bridge data dictionary
# that will later be converted to a dataframe for the genre bridge dimension
def add_data_to_genre_bridge(genre_data, genre_bridge_dict, igdb_category_id_temp):
    # Iterates through each igdb id in the IGDB api output, then adds the genre data
    # associated with each one to genre_bridge_dict
    for genre_info in genre_data:
        igdb_id = genre_info["id"]
        category_id = int(igdb_category_id_temp[igdb_id])
        if "genres" not in genre_info: # if igdb game has no associated genres, add "NA"
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")
        else:
            for genre_id in genre_info["genres"]:
                genre_bridge_dict["category_id"].append(category_id)
                genre_bridge_dict["genre_id"].append(genre_id)

    # Searches through all IGDB ids that did not have any output from the API
    # Then gives them a Not Available genre in the final output
    for igdb_id, category_id in igdb_category_id_temp.items():
        if category_id not in genre_bridge_dict["category_id"]:
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")


# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


# Accesses the genre  bridge dimension
def access_genre_bridge_dimension():
    genre_bridge_dimension_path = repo_root + "/data/dimension_tables/genre_bridge_dimension.csv"
    try:
        genre_bridge_df = pd.read_csv(genre_bridge_dimension_path, keep_default_na=False)
    except FileNotFoundError:
        with open(genre_bridge_dimension_path, 'w') as f:
            f.write('category_id,genre_id')

        genre_bridge_df = pd.read_csv(genre_bridge_dimension_path, keep_default_na=False)

    return genre_bridge_df


# Accesses the category dimension
def access_category_dimension():
    category_dimension_path = repo_root + "/data/dimension_tables/category_dimension.csv"
    category_df = pd.read_csv(category_dimension_path, keep_default_na=False)

    return category_df


# Adds genre data for categories not seen before which would be updated
# in the category dimension
def add_new_genre_data(wrapper, category_df, genre_bridge_dim):
    # dict holding category and genre data not found in category_df already
    genre_bridge_dict = {
        "category_id": [],
        "genre_id": []
    }

    exclude_list = genre_bridge_dim["category_id"].tolist()
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
            genre_bridge_dict["category_id"].append(category_id)
            genre_bridge_dict["genre_id"].append("NA")
        if i % 100 == 0 or i == len(new_category_df): # every 100 igdb games or if last one, make api call
            igdb_ids_tuple = tuple(igdb_category_id_temp.keys())
            igdb_ids_arg = str(igdb_ids_tuple)
            if len(igdb_ids_tuple) == 1:
                igdb_ids_arg = igdb_ids_arg.replace(',', '')
            genre_data = get_igdb_genre(wrapper, igdb_ids_arg)
            add_data_to_genre_bridge(genre_data, genre_bridge_dict, igdb_category_id_temp)
            igdb_category_id_temp.clear()
            
    return genre_bridge_dict


# Creates CSV file from the genre bridge dimension dictionary
# Updates the CSV file if it already exists with new categories and their genres
def process_dim_csv_file(genre_bridge_dim, new_genre_data_dim):
    new_genre_data_dim = pd.DataFrame(new_genre_data_dim)
    final_df = pd.concat([genre_bridge_dim, new_genre_data_dim]).drop_duplicates()
    genre_bridge_dim_path = repo_root + "/data/dimension_tables/genre_bridge_dimension.csv"
    final_df.to_csv(genre_bridge_dim_path, index=False)


def main():
    wrapper = make_wrapper()
    category_df = access_category_dimension()
    genre_bridge_dim = access_genre_bridge_dimension()
    new_genre_data_dim = add_new_genre_data(wrapper, category_df, genre_bridge_dim)
    process_dim_csv_file(genre_bridge_dim, new_genre_data_dim)


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))