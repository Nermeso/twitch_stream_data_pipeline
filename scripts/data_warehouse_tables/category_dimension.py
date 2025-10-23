import os
import requests
import pandas as pd
from pathlib import Path
import time


################################ SUMMARY ################################
'''
    This script calls the Twitch API to add new categories not seen
    already in the category dimension file. If the file does not exist,
    it makes it. It also produces a CSV containing the current streamed
    categories.
'''
#########################################################################


start = time.time()
repo_root = str(Path(__file__).parents[2])

# Creates dataframe of current category dimension and creates list of category ids in that dim
def get_category_dim_info():
    category_dim_path = repo_root + "/data/dimension_tables/category_dimension.csv"

    # Read current category dimension, if it does not exist, create an empty one
    try:
        current_category_dim_df = pd.read_csv(category_dim_path, keep_default_na = False)
        current_ids = current_category_dim_df["category_id"].tolist()
    except FileNotFoundError:
        with open(category_dim_path, 'w') as f:
            f.write("category_id,igdb_id,category_name")
        current_category_dim_df = pd.read_csv(category_dim_path, keep_default_na = False)
        current_ids = []
    
    return current_category_dim_df, current_ids


# Iteratively call get top games api to get current games that have at least one streamer to do two things:
# Gets all categories that are currently being streamed
# Get all categories that are not present in the category dimension and to add to it
def api_call_loop(url, headers, data, already_exist_ids):
    current_streamed_categories_dict = {"category_id": [], "category_name": []}
    cursor = ""
    while cursor != "done":
        params = {
            "first": 100,
            "after": cursor
        }
        response = requests.get(url, headers=headers, params=params)
        output = response.json()
        for category in output["data"]:
            category_id = category["id"]
            category_name = category["name"]
            igdb_id = category["igdb_id"]

            # Adds category to dict containing current streamed categories
            if int(category_id) not in current_streamed_categories_dict["category_id"]:
                current_streamed_categories_dict["category_id"].append(category_id)
                current_streamed_categories_dict["category_name"].append(category_name)  

            # Adds category to new category dict if not seen before in category dimension
            if int(category_id) in data["category_id"] or int(category_id) in already_exist_ids: # if category exists already, go to next category
                continue
            data["category_id"].append(category_id)
            data["category_name"].append(category_name)
            if igdb_id == "": # IGDB Id may be empty, fill it with NA if the case
                data["igdb_id"].append("NA")
            else:
                data["igdb_id"].append(igdb_id)

        # Ends pagination of pages when done
        if len(output["pagination"]) == 0: # if no cursor in pagination, no more pages
            cursor = "done"
        else:    
            cursor = output["pagination"]["cursor"]

    return current_streamed_categories_dict


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Call Twitch top games API preiodically to collect game info
def collect_twitch_category_data(headers):
    url = "https://api.twitch.tv/helix/games/top"
    data = {
        "category_id": [],
        "igdb_id": [],
        "category_name": []
    }
    current_category_dim_df, current_ids = get_category_dim_info()

    while True:
        try:
            current_streamed_categories_dict = api_call_loop(url, headers, data, current_ids)
            break
        except ConnectionError as e:
            data = {"category_id": [], "igdb_id": [],"category_name": []}
            continue
   
    return data, current_category_dim_df, current_streamed_categories_dict


# Converts dictionary of category data to csv
# Combines current data of categories with new categories
def data_to_csv(data_dict, current_dim_df):
    data = pd.DataFrame(data_dict)
    data["category_id"] = data["category_id"].astype(int)
    final_df = pd.concat([current_dim_df, data]).drop_duplicates() # combines current category data with new
    category_dim_path = repo_root + "/data/dimension_tables/category_dimension.csv"
    final_df.to_csv(category_dim_path, index=False)

    return


# Converts dictionary of currently streamed categories to CSV
def curr_streamed_categories_to_csv(current_streamed_categories_dict):
    data = pd.DataFrame(current_streamed_categories_dict)
    df_path = repo_root + "/data/miscellaneous/curr_streamed_categories.csv"
    data.to_csv(df_path, index = False)



def main():
    headers = get_credentials()
    new_category_data_dict, current_dim_df, current_streamed_categories_dict = collect_twitch_category_data(headers)
    data_to_csv(new_category_data_dict, current_dim_df)
    curr_streamed_categories_to_csv(current_streamed_categories_dict)


if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))