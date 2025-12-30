from pathlib import Path
import pandas as pd
import os

repo_root = str(Path(__file__).parents[2])


def main():
    processed_game_mode_data_path = repo_root + "/data/twitch_project_processed_layer/processed_game_modes_data/processed_game_modes_data.csv"
    game_mode_df = pd.read_csv(processed_game_mode_data_path)
    game_mode_df = game_mode_df[["game_mode_id", "game_mode_name"]] # Limit only to columns we need
    game_mode_df.to_csv(repo_root + "/data/twitch_project_curated_layer/curated_game_modes_data/curated_game_modes_data.csv", index=False)


if __name__ == "__main__":
    main()