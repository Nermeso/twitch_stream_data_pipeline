import json
from igdb.wrapper import IGDBWrapper
import os
from datetime import datetime
from pathlib import Path

repo_root = str(Path(__file__).parents[2])

################### SUMMARY ###################
'''
    This script generates a JSON file to be
    of available genres from the IGDB API.
'''
###############################################

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)

    return output


# Makes IGDB wrapper to interact with IGDB API
def make_wrapper():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    wrapper = IGDBWrapper(client_id, access_token)

    return wrapper


# Calls IGDB API to get data on all IGDB game modes
def get_game_mode_data(wrapper):
    byte_array = wrapper.api_request(
                    "game_modes",
                    "f *; limit 100;"
            )
    output = byte_to_json(byte_array)

    return output


def main():
    wrapper = make_wrapper()
    json_data = get_game_mode_data(wrapper)

    year = str(datetime.today().year)
    month = str(datetime.today().month)
    day = str(datetime.today().day)
    day_date_id = year+month+day

    raw_game_mode_data = {
        "day_date_id": day_date_id,
        "data": json_data
    }

    # Write the raw game mode data to json file
    file_path = f"data/twitch_project_raw_layer/raw_game_modes_data/raw_game_modes_data_{day_date_id}.json"
    with open(file_path, 'w') as json_file:
        json.dump(raw_game_mode_data, json_file, indent=4)


if __name__ == "__main__":
   main()
