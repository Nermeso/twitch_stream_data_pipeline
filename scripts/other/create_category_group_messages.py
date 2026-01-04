import pandas as pd
from pathlib import Path
import time
import numpy as np


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

def main():
    day_date_id = "20260102" # testing value
    time_of_day_id = "1545" # testing value

    # Get current streamed categories based off of processed_categories file
    processed_category_data_path = repo_root + f"/data/twitch_project_processed_layer/processed_categories_data/{day_date_id}/processed_categories_data_{day_date_id}_{time_of_day_id}.csv"
    processed_category_df = pd.read_csv(processed_category_data_path, keep_default_na=False)
    curr_streamed_categories = list(set(processed_category_df["category_id"].tolist()))
    
    




if __name__ == "__main__":
    main()