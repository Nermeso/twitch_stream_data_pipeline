import json
from igdb.wrapper import IGDBWrapper
import os
from pathlib import Path

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
    # output = json.dumps(output, indent=4, sort_keys=True)

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


# Converts API call's json output to csv for dimension table
def json_to_csv(data):
    game_dimension_path = repo_root + "/data/data_warehouse/game_mode_dimension.csv"

    with open(game_dimension_path, 'w') as f:
        f.write('game_mode_id,game_mode_name\n')
        for game_mode in data:
            game_mode_id = game_mode["id"]
            game_mode_name = game_mode["name"]
            line = f"{game_mode_id},{game_mode_name}\n"
            f.write(line)
        f.write('NA,Not Available')

    return


def main():
    wrapper = make_wrapper()
    json_data = get_game_mode_data(wrapper)
    json_to_csv(json_data)


if __name__ == "__main__":
   main()
