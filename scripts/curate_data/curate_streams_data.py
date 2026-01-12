from pathlib import Path
import pandas as pd
import time
import os

####################### SUMMARY #######################
'''
    Removes unnecessary columns from processed stream
    data CSV. Will be used for data to be inserted
    into PostgreSQL database.
'''
#######################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])
pd.options.mode.chained_assignment = None  # default='warn'


def main():
    # Actual lambda function implementation of this will have the day date id and 
    # time of day info passed to it
    day_date_id = "20260111"
    time_of_day_id = "1715"

    processed_data_path = repo_root + f"/data/twitch_project_processed_layer/processed_streams_data/{day_date_id}/processed_streams_data_{day_date_id}_{time_of_day_id}.csv"
    processed_stream_df = pd.read_csv(processed_data_path)

    # Limit columns to only relevant ones
    curated_stream_df = processed_stream_df[["id", "user_id", "game_id", "language", "viewer_count"]]

    # Rename columns to ones established in data model
    curated_stream_df = curated_stream_df.rename(columns={
        "id": "stream_id",
        "game_id": "category_id",
        "language": "language_id"
    })

    # Add time columns
    date_values = [day_date_id] * len(curated_stream_df)
    time_values = [time_of_day_id] * len(curated_stream_df)
    curated_stream_df.insert(loc = 1, column = "day_date_id", value = date_values)
    curated_stream_df.insert(loc = 2, column = "time_of_day_id", value = time_values)

    # Add hours watched metric column
    curated_stream_df["hours_watched"] = curated_stream_df["viewer_count"] * 0.25

    # Drop duplicates if exist
    curated_stream_df = curated_stream_df.drop_duplicates(subset=["stream_id", "time_of_day_id", "day_date_id"], keep="first")

    # Upload file as CSV
    curated_data_path = Path(repo_root + f"/data/twitch_project_curated_layer/curated_streams_data/{day_date_id}/curated_stream_data_{day_date_id}_{time_of_day_id}.csv")
    curated_data_path.parent.mkdir(parents=True, exist_ok=True)
    curated_stream_df.to_csv(curated_data_path, index=False)



if __name__ == "__main__":
    main()


end = time.time()
print("Duration: " + str(end - start))