from pathlib import Path
import pandas as pd
import os

repo_root = str(Path(__file__).parents[2])


def main():
    processed_genre_data_path = repo_root + "/data/twitch_project_processed_layer/processed_genres_data/processed_genre_data.csv"
    genre_df = pd.read_csv(processed_genre_data_path)
    genre_df = genre_df[["genre_id", "genre_name"]] # Limit only to columns we need
    genre_df.to_csv(repo_root + "/data/twitch_project_curated_layer/curated_genres_data/curated_genres_data.csv", index=False)


if __name__ == "__main__":
    main()