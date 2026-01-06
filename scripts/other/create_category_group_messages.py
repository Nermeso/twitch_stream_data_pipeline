import pandas as pd
from pathlib import Path
import time
import numpy as np
from datetime import datetime


# This script triggers once processed_categories is uploaded
# First searches for category popularity data in miscellaneous bucket
# if that does not exist, refer to default weights and use that when creating
# category group messages


######################### SUMMARY #########################
'''
    Outputs to SQS messages that contains a group of
    category IDs. Each group should be approximately 
    equal to each other in terms of the number of 
    associated channels streaming. The size of each
    group and the associated categories is either based
    off of default weights or the most recently collected
    stream data.
'''
###########################################################

repo_root = str(Path(__file__).parents[2])

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


# Split categories into equal groups in terms of their number of channels/streamers using greedy algorithm
def split_categories_into_groups(weighted_category_df): 
    category_groups = [[] for _ in range(25)]
    weight_value_groups = [0 for _ in range(25)]
    # Go through each category, then assign it to a group
    for _, row in weighted_category_df.iterrows():
        num_of_streamers = row['num_of_streamers']
        category_id = row["category_id"]
        min_sum = 999999999
        min_idx = -1
        # Iterate through each weight value group to see which one is suitable for category
        for wvg_idx, group_weight_sum in enumerate(weight_value_groups):
            if group_weight_sum + num_of_streamers <= 7000:  # If end group weight sum is 7000 or less, add it first
                min_idx = wvg_idx
                break
            elif group_weight_sum == 0:   # weight group has no category yet, automatically add it
                min_idx = wvg_idx
                break
            elif group_weight_sum <= min_sum:  # if all groups don't have a group weight sum of 0 or would be more than 7000, start adding to smallest weight value groups
                min_sum = group_weight_sum
                min_idx = wvg_idx
        weight_value_groups[min_idx] += num_of_streamers
        category_groups[min_idx].append(category_id)

    return category_groups, weight_value_groups



def main():
    day_date_id = get_day_date_id()
    time_of_day_id = get_time_of_day_id()

    day_date_id = "20260102" # testing value
    time_of_day_id = "1545" # testing value

    # Get current streamed categories based off of processed_categories file
    processed_category_data_path = repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv"
    curr_streamed_categories_df = pd.read_csv(processed_category_data_path, keep_default_na=False)
    
    # Check if popularity data exists or not
    popularity_data_exists = False
    category_popularity_df = ""
    try:
        category_popularity_df = pd.read_csv(repo_root + "/data/twitch_project_miscellaneous/category_popularity_data/category_popularity_data.csv")
        popularity_data_exists = True
    except FileNotFoundError:
        popularity_data_exists = False


    # Produce category groups
    if popularity_data_exists: # use recent popularity data since it exists
        merged_df = pd.merge(curr_streamed_categories_df, category_popularity_df, on="category_id", how='left')
        merged_df['num_of_streamers'] = merged_df['num_of_streamers'].replace(np.nan, 1)
        category_groups, wvg = split_categories_into_groups(merged_df)
    else: # if no recent category popularity data found, use default popularity data
        default_popularity_path = repo_root + "/data/twitch_project_miscellaneous/category_popularity_data/default_category_weights.csv"
        default_pop_df = pd.read_csv(default_popularity_path)
        category_pop_df = pd.concat([curr_streamed_categories_df, default_pop_df], axis=1)
        category_pop_df = category_pop_df[["category_id", "category_name", "num_of_streamers"]].fillna(1)
        category_groups, wvg = split_categories_into_groups(category_pop_df)

    print(category_groups)
    print(wvg)



if __name__ == "__main__":
    main()