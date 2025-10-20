import pandas as pd
from pathlib import Path
import pandas as pd

repo_root = str(Path(__file__).parents[2])

##################### SUMMARY #####################
'''
    This script creates the time of day dimension.
    Each record is a moment of time every 15 minutes
    in a singular day.
'''
###################################################

# Creates the time key attribute
def create_time_key(data):
    for hour_num in range(24):
        hour = str(hour_num).zfill(2)
        for time_num in range(0, 60, 15):
            minute = str(time_num).zfill(2)
            time_key = hour + minute
            data["time_key"].append(time_key)


# Creates time_24h attribute
def create_time_24h(data):
    for hour_num in range(24):
        hour = str(hour_num).zfill(2)
        for time_num in range(0, 60, 15):
            minute = str(time_num).zfill(2)
            time_24h = hour + ":" + minute
            data["time_24h"].append(time_24h)


# Creates time_12h attribute
def create_time12h(data):
    for hour_num in [12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
        hour = str(hour_num).zfill(2)
        for time_num in range(0, 60, 15):
            minute = str(time_num).zfill(2)
            time_12h = hour + ":" + minute + " AM"
            data["time_12h"].append(time_12h)

    for hour_num in [12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]:
        hour = str(hour_num).zfill(2)
        for time_num in range(0, 60, 15):
            minute = str(time_num).zfill(2)
            time_12h = hour + ":" + minute + " PM"
            data["time_12h"].append(time_12h)


# Creates the hour attribute
def create_hour(data):
    hour = 0
    for i in range(1, 97):
        data["hour"].append(hour)
        if i % 4 == 0:
            hour += 1


# Creates the minute attribute
def create_minute(data):
    for i in range(0, 24):
        for minute in range(0, 60, 15):
            data["minute"].append(minute)


# Creates the AM_PM attribute
def create_AM_PM(data):
    for i in range(1, 49):
        data["AM_PM"].append("AM")
    for i in range(49, 97):
        data["AM_PM"].append("PM")


# Creates part of day attribute
def create_part_of_day(data):
    name_list = ["night"] * 20 + ["morning"] * 28 + ["afternoon"] * 20 + ["evening"] * 16 + ["night"] * 12
    data["part_of_day"] = name_list



# Creates a dataframe for the time of day dimension
def create_time_of_day_data(data):
    create_time_key(data)
    create_time_24h(data)
    create_time12h(data)
    create_hour(data)
    create_minute(data)
    create_AM_PM(data)
    create_part_of_day(data)
    time_of_day_df = pd.DataFrame(data)

    return time_of_day_df



def main():
    data = {
        "time_key": [],
        "time_24h": [],
        "time_12h": [],
        "hour": [],
        "minute": [],
        "AM_PM": [],
        "part_of_day": []
    }
    time_of_day_df = create_time_of_day_data(data)
    time_of_day_dimension_path = repo_root + "/data/data_warehouse/time_of_day_dimension.csv"
    time_of_day_df.to_csv(time_of_day_dimension_path, index=False)


if __name__ == "__main__":
    main()