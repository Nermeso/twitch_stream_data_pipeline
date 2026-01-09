import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################ SUMMARY ################################
'''
    This script calls the "Get Users" Twitch endpoint to return
    information on Twitch broadcasters and users. The users to collect
    data for is based on recently collected curated stream data. The 
    output will be a JSON file containing user data. The goal is to get 
    information on all users that Twitch has available. Data is not
    returned for users that are banned.
'''
#########################################################################


start = time.time()
repo_root = str(Path(__file__).parents[2])


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# MAKE SURE TO CHANGE TO SO IT IS BASED OFF OF WHEN SCRIPT IS EXECUTED
# Gets current date id based of date when script is executed
def get_day_date_id():
    # Gets date id
    date_dim_path = repo_root + "/data/twitch_project_raw_layer/raw_day_dates_data/raw_day_dates_data.csv"
    date_df = pd.read_csv(date_dim_path)
    current_date = datetime.today()
    day_date_id = date_df[date_df["the_date"] == str(current_date.date())].iloc[0, 0]
   
    return str(day_date_id)

# MAKE SURE TO CHANGE TO SO IT IS BASED OFF OF WHEN SCRIPT IS EXECUTED
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


# Gets user ids that we will potentially call the API to get data on
def get_potential_new_users(day_date_id, time_of_day_id):
    # curated_stream_data_path = repo_root + f"/data/twitch_project_curated_layer/curated_streams_data/{day_date_id}/curated_stream_data_{day_date_id}_{time_of_day_id}.csv"
    ############### CHANGE LATER

    curated_stream_data_path = repo_root + f"/data/sample_data/curated_data/curated_streams_data_20260108_0830.csv"

    ##########################
    
    stream_df = pd.read_csv(curated_stream_data_path, dtype={"user_id": "string"})
    user_list = list(set(stream_df["user_id"].tolist()))

    return user_list


# Reads current user dimension data to get users we have data for already
def get_current_user_dim():
    curated_user_data_path = repo_root + f"/data/twitch_project_curated_layer/curated_users_data/curated_users_data.csv"
    try:
        current_user_dim_df = pd.read_csv(curated_user_data_path, index_col=False, dtype={"user_id": "string", "user_name": "string", "login_name": "string", "broadcaster_type": "string"})
        current_users = list(set(current_user_dim_df["user_id"].tolist()))
    except FileNotFoundError:
        current_users = []

    return current_users


# Calls Twitch's "Get Users" endpoint to get data on users
def get_data_from_API(user_list, raw_user_data, headers):

################################################
    user_list = ['917394619', '26008371', '137272103', '167127265', '127636569', '45961015', '927412726', '63407709', 'testDocumentId2', '210621785', '1210281932', '257710941', '225756320', '50965805', '1255795219', '547233081', '1008505012', '630495319', '233531638', '824481910', '1411591553', '62845611', '989738591', '1325335087', '93999843', '165265873', '523967160', '1307268087', '986896108', '451406072', '740218180', '52338002', '64473530', '1349389994', '475966084', '461917236', '121338098', '253676495', '845720737', '150073769', '141723092', '871764708', '81227602', '473452490', '167257993', '118824038', '1351838974', '1391440562', '178417606', '542348067', '504159907', '528044776', '793022139', '112143183', '451519704', '191670150', '479827861', '139770240', '596683242', '793029424', '886597892', '44189532', '155076302', '163688438', '837488421', '1420067311', '51630395', '168602974', '1406174153', '784190430', '115666389', '745781282', '148265150', '791092196', '590885642', '789735790', '567487121', '701636257', '1121931602', '158986006', '819240356', '52570862', '98511057', '1392062268', '413994878', '213893913', '783015092', '614123186', '960319816', '687506618', '402248571', '1298165925', '549544107', '797497902', '636414550', '820121643', '493334286', '464937934', '5324960', '1094393743']
############################################

    # API endpoint for getting users accepts max 100 users at a time
    for i in range(0, len(user_list), 100):
        user_list_tmp = user_list[i:i + 100]
        params = {
            "id": user_list_tmp,
            "first": 100
        }

        # Sometimes calling API leads to connection error as a result of DNS issues
        # Loop aims to restart process if that happens
        while True:
            try:
                response = requests.get("https://api.twitch.tv/helix/users", params=params, headers=headers)
                output = response.json()
                try:
                    raw_user_data["data"].extend(output["data"])
                except Exception as e:
                    print(user_list_tmp)
                    print(e)
                    print("Output: " + str(output))
                    exit()
                break
            except ConnectionError as e:
                print(e)
                continue



def main():
    headers = get_credentials()
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20251229" # test
    time_of_day_id = "0830" # test

    # Gets user IDs from recently collected stream data
    stream_user_list = get_potential_new_users(day_date_id, time_of_day_id)
     
    # Gets user IDs from user dimension
    current_user_list = get_current_user_dim()

    # Gets only users that we have not collected data of yet
    set1 = set(stream_user_list)
    set2 = set(current_user_list)
    need_data_users_list = list(set1.difference(set2))

    raw_user_data = {
        "day_date_id": day_date_id,
        "time_of_day_id": time_of_day_id,
        "data": []
    }

    # Calls Twitch's "Get Users" endpoint to get data on users
    get_data_from_API(need_data_users_list, raw_user_data, headers)

    # Write the raw user data to json file
    output_file_path = Path(repo_root + f"/data/twitch_project_raw_layer/raw_users_data/{day_date_id}/raw_users_data_{day_date_id}_{time_of_day_id}.json")
    output_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file_path, 'w') as json_file:
        json.dump(raw_user_data, json_file, indent=4)



if __name__ == "__main__":
    main()


end = time.time()
duration = end - start
print("Duration: " + str(duration))