from pathlib import Path
import pandas as pd
import time

####################### SUMMARY #######################
'''
    Removes unnecessary columns from processed stream
    data CSV. Will be used for data to be inserted
    into PostgreSQL database.
'''
#######################################################

start = time.time()
repo_root = str(Path(__file__).parents[2])


def main():
    # Actual lambda function implementation of this will have the day date id and 
    # time of day info passed to it
    day_date_id = "20251230"
    time_of_day_id = "1215"
    



if __name__ == "__main__":
    main()