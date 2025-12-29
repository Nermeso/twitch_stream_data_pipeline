import json
from datetime import datetime
from igdb.wrapper import IGDBWrapper
import os
from pathlib import Path

repo_root = str(Path(__file__).parents[2])

################# SUMMARY #################
'''
    This script generates a JSON file to be
    of available genres from the IGDB API.
'''

########## Low-level functions ##########

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)

    return output


########## High-level functions ##########

# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


# Calls IGDB API to get data on all game genres
def get_IGDB_genre_data(wrapper):
    byte_array = wrapper.api_request(
                    "genres",
                    "f *; limit 100;"
            )
    output = byte_to_json(byte_array)

    return output


def main():
    wrapper = make_wrapper() # Makes IGDB wrapper to interact with IGDB API
    json_data = get_IGDB_genre_data(wrapper)

    year = str(datetime.today().year)
    month = str(datetime.today().month)
    day = str(datetime.today().day)
    day_date_id = year+month+day

    raw_genre_data = {
        "day_date_id": day_date_id,
        "data": json_data
    }

    # Write the raw genre data to json file
    file_path = f"data/twitch_project_raw_layer/raw_genres_data/raw_genres_data.json"
    with open(file_path, 'w') as json_file:
        json.dump(raw_genre_data, json_file, indent=4)


if __name__ == "__main__":
   main()
