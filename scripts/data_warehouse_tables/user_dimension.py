import os
import requests
import pandas as pd
from pathlib import Path
import time
import json
import ast

start = time.time()

repo_root = str(Path(__file__).parents[2])


###################################### SUMMARY ######################################
'''
    This script collects information on twitch broadcasters by calling the Twitch API.
'''
#####################################################################################


def pretty_print(input):
    return json.dumps(input, indent=4)


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Get the users who we will get more info on through the API
def get_users():
    users_list = []
    # Lambda file would only read files in folder of a specific time
    # ex: 20251201_1515 meaning would only read files in folder of stream data collected on December 1, 2025, at 3:15 PM
    for filepath in Path("data/fact_table_data/20251201_1515/").glob('**/*'):
        df_tmp = pd.read_csv(filepath)
        new_users = df_tmp["user_id"].tolist()
        users_list.extend(new_users)

    return list(set(users_list))


# if user from SQS message batch already exists in user dimension, we do not include it
def get_current_user_dim():
    user_dim_path = repo_root + "/data/dimension_tables/user_dimension.csv"
    try:
        current_user_dim_df = pd.read_csv(user_dim_path, index_col=False)
        current_users = current_user_dim_df["user_id"].tolist()
    except FileNotFoundError:
        with open(user_dim_path, 'w') as f:
            f.write("user_id,user_name,login_name,broadcaster_type")
        current_user_dim_df = pd.read_csv(user_dim_path, index_col=False)
        current_users = []

    return current_users, current_user_dim_df


# Update user list to only include users we do not have info on already
# in user dimension
def get_only_new_users(user_list, current_users):
    set1 = set(user_list)
    set2 = set(current_users)
    new_users = list(set1.difference(set2))

    return new_users


# Calls Twitch API to get additional info on users
def get_user_data(user_list):
    user_data_dict = {
        "user_id": [],
        "user_name": [],
        "login_name": [],
        "broadcaster_type": []
    }

    # Iteratively loops over user list when calling API
    get_data_from_API(user_data_dict, user_list)
    df = pd.DataFrame(user_data_dict)
    
    return df


# Calls API endpoint for getting users
def get_data_from_API(user_data_dict, user_list):
    # API endpoint for getting users accepts max 100 users at a time
    for i in range(0, len(user_list), 100):
        user_list_tmp = user_list[i:i + 100]
        params = {
            "id": user_list_tmp,
            "first": 100
        }

        while True:
            try:
                call_users_endpoint(params, user_data_dict)
                break
            except ConnectionError as e:
                print(e)
                continue


# Call Twitch's Get User Endpoint to get user data
def call_users_endpoint(params, user_data_dict):
    headers = get_credentials()
    url = "https://api.twitch.tv/helix/users"
    response = requests.get(url, params=params, headers=headers)
    output = response.json()
    for user_info in output["data"]:
        user_data_dict["user_id"].append(str(user_info["id"]))
        user_data_dict["user_name"].append(user_info["display_name"])
        user_data_dict["login_name"].append(user_info["login"])
        if user_info["broadcaster_type"] == "":
            user_data_dict["broadcaster_type"].append("normal") # make broadcaster type normal instead of empty string
        else:
            user_data_dict["broadcaster_type"].append(user_info["broadcaster_type"])


def main():
    user_list = get_users()
    current_users, current_user_dim_df = get_current_user_dim() # get current users we have data for
    new_users = get_only_new_users(user_list, current_users) # Returns user ids of users not in user dimension yet

    start2 = time.time()
    user_dim_df = get_user_data(new_users) # call api to get data for new users
    end2 = time.time()
    print("Calling API Duration: " + str(end2 - start2))

    new_df = pd.concat([current_user_dim_df, user_dim_df]).drop_duplicates()
    new_df["user_id"] = new_df["user_id"].astype(int) # prevents user_id from becoming float
    new_df.to_csv(repo_root + "/data/dimension_tables/user_dimension.csv", index=False)


if __name__ == "__main__":
    main()


end = time.time()
print("Duration: " + str(end - start))


