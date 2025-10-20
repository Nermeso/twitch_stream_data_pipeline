import json
from igdb.wrapper import IGDBWrapper
import os
from pathlib import Path

repo_root = str(Path(__file__).parents[2])

################# SUMMARY #################
'''
    This script generates a CSV file to be
    inserted into the genre dimension table.
'''

########## Low-level functions ##########

# converts byte array output from wrapper into json
def byte_to_json(byte_array):
    my_json = byte_array.decode('utf8').replace("'", '"')
    output = json.loads(my_json)
    # output = json.dumps(output, indent=4, sort_keys=True)

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


# Converts API call's json output to csv for dimension table
def json_to_csv(data):
    genre_dimension_path = repo_root + "/data/data_warehouse/genre_dimension.csv"

    with open(genre_dimension_path, 'w') as f:
        f.write('genre_id,genre_name\n')
        for genre in data:
            genre_id = genre["id"]
            genre_name = genre["name"]
            line = f"{genre_id},{genre_name}\n"
            f.write(line)
        f.write('NA,Not Available')

    return


def main():
    wrapper = make_wrapper()
    json_data = get_IGDB_genre_data(wrapper)
    json_to_csv(json_data)


if __name__ == "__main__":
   main()
