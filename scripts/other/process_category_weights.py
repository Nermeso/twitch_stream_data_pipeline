import os
import requests
import pandas as pd
from pathlib import Path
import time
import numpy as np

############################################## SUMMARY ##############################################
'''
    This script creates a file that assigns a weight to currently streamed categories indicating
    the number of channels currently streaming it. This is important input to serve the lambda
    functions collecting stream data.
'''
#####################################################################################################

start = time.time()

repo_root = str(Path(__file__).parents[2])


# Get locations of most recently collected stream data
def get_stream_data_paths():
    data_paths = []
    for filepath in Path("data/dummy_data/fact_table_data").glob('**/*'):
        data_paths.append(str(filepath))

    return data_paths


# Combines recently created fact table CSVs into one aggregated by category to get total # of streams
def combine_fact_table_data(fact_table_data_paths):
    master_df = pd.DataFrame(columns=["stream_id", "date_day_id", "time_of_day_id", "user_id", "category_id", "viewer_count", "language_id", "user_name"])
    for data_path in fact_table_data_paths:
        df = pd.read_csv(data_path)
        master_df = pd.concat([master_df, df])
    master_df = master_df.drop_duplicates()
    grouped_df = master_df.groupby('category_id').size().reset_index(name='num_of_streams').sort_values(by='num_of_streams', ascending=False).reset_index(drop=True)
    
    return grouped_df

    

# Produce dataframe containing currently streamed categories with their number of streams associated with it
# Categories with no num_of_streams value is given a default value of 1
def produce_category_weights(grouped_df, curr_streamed_categories):
    merged_df = pd.merge(curr_streamed_categories, grouped_df, on='category_id', how='left')
    merged_df['num_of_streams'] = merged_df['num_of_streams'].replace(np.nan, 1)
    
    return merged_df


# Split categories into equal groups in terms of their weights (num_of_streams) using greedy algorithm
def split_categories_into_groups(weighted_category_df):
    category_groups = [[] for _ in range(20)]
    weight_value_groups = [0 for _ in range(20)]
    # Go through each category, then assign it to a group
    for cat_idx, row in weighted_category_df.iterrows():
        num_of_streams = row['num_of_streams']
        category_id = row["category_id"]
        min_sum = 999999999
        min_idx = -1
        # Iterate through each weight value group to see which one is suitable for category
        for wvg_idx, group_weight_sum in enumerate(weight_value_groups):
            # weight group has no category yet, automatically add it
            if group_weight_sum == 0:
                min_sum = group_weight_sum
                min_idx = wvg_idx
                break
            # aim for category to be assigned to group with the lowest total summed weight
            elif group_weight_sum <= min_sum:
                min_sum = group_weight_sum
                min_idx = wvg_idx
        weight_value_groups[min_idx] += num_of_streams
        category_groups[min_idx].append(category_id)
        
    return category_groups, weight_value_groups




def main():
    fact_table_data_paths = get_stream_data_paths()
    curr_streamed_categories = pd.read_csv("data/miscellaneous/curr_streamed_categories.csv")
    grouped_df = combine_fact_table_data(fact_table_data_paths)
    weighted_category_df = produce_category_weights(grouped_df, curr_streamed_categories)
    print(weighted_category_df)
    category_groups, wvg = split_categories_into_groups(weighted_category_df)

    ########################################
    # for group in category_groups:
    #     total_value = 0
    #     for category_id in group:
    #         try:
    #             weight = weighted_category_df[weighted_category_df["category_id"] == category_id]["num_of_streams"].iloc[0]
    #             total_value += weight
    #         except IndexError:
    #             total_value += 1
    #     print(group)
    #     print(total_value)
    #     print()
    print(wvg)



if __name__ == "__main__":
    main()


end = time.time()
print("Duration: "+ str(end - start))