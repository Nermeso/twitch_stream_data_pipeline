import pandas as pd
from datetime import datetime
from pathlib import Path

repo_root = str(Path(__file__).parents[2])

#################### SUMMARY ####################
'''
    Creates the date dimension table.
'''
#################################################

def main():
    years = range(2025,2027)
    months = range(1,13)
    days = range(1,32)
    dateResultList = []
    date_id = 0

    week_day_names = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}

    for year in years:
        for month in months:
            for day in days:
                try:
                    dateResult = pd.to_datetime(f'{year}-{month:02d}-{day:02d}')
                    date = dateResult.strftime('%m/%d/%Y')
                    week_day_num = datetime.weekday(dateResult)
                    day_of_week = week_day_names[week_day_num]
                    month_num = str(month).zfill(2)
                    day_num = str(day).zfill(2)
                    year_num = str(year)
                    monthname = dateResult.strftime('%B')
                    monthabbrev = dateResult.strftime('%b').upper()
                    shortenedyear = str(year)[-2:]
                    dateResultList.append([date_id,dateResult,date,day_of_week,month_num,day_num,year_num,monthname,monthabbrev,shortenedyear])
                    date_id += 10
                except ValueError:
                    continue

    date_dimension_table = pd.DataFrame(dateResultList,columns=["date_id","OurDate","Date_MMDDYYYY","Day_of_Week","Month","Day","Year","Month_Name","MonthAbbrev","Year_YY"])
    date_dimension_path = repo_root + "/data/data_warehouse/date_dimension.csv"
    date_dimension_table.to_csv(date_dimension_path ,index=False)



if __name__ == "__main__":
    main()