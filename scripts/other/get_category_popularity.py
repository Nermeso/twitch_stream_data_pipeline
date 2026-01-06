import pandas as pd
from datetime import datetime
from pathlib import Path
import json
import time

################################# SUMMARY #################################
'''
    This script produces a CSV file that contains the popularity of each
    category based off of the most recently collected stream data.
'''
###########################################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])


def main():
    day_date_id = "20251230" # test value, event input should provide this
    time_of_day_id = "1330" # test value, event input should provide this

    curated_stream_df = pd.read_csv(repo_root + f"/data/twitch_project_curated_layer/curated_streams_data/{day_date_id}/curated_stream_data_{day_date_id}_{time_of_day_id}.csv")
    category_popularity_df = curated_stream_df.groupby(["category_id"], as_index=False).agg(
                                        category_id=('category_id', 'first'),
                                        num_of_streamers=('stream_id', 'count')
                                   ).sort_values(by="num_of_streamers", ascending=False).reset_index(drop=True)
    
    category_popularity_df.to_csv(repo_root + "/data/twitch_project_miscellaneous/category_popularity_data/category_popularity_data.csv", index=False)


if __name__ == "__main__":
    main()