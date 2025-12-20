import json
import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import boto3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

######################  SUMMARY  ##########################
'''
    Every 15 minutes, inserts stream data from CSV into
    Postgresql database. Runs 12 minutes after collection
    stream data.
'''
###########################################################


# Gets date id associated with when stream data was collected 30 minutes before time of this script execution
def get_date_id(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/date_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for date dimension. Status - {status}")
        date_df = pd.read_csv(response.get("Body"), keep_default_na=False)
    time_to_subtract = timedelta(minutes=19)     ##################################################
    data_date = datetime.today() - time_to_subtract
    data_date = data_date.astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    date_id = date_df[date_df["the_date"] == str(data_date.date())].iloc[0, 0]
   
    return str(date_id)


# Gets time key associated with when stream data was collected 30 minutes before time of this script execution
def get_time_key(s3_client):
    response = s3_client.get_object(Bucket="twitchdatapipelineproject", Key="raw/dimension_table/time_of_day_dimension.csv")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        print(f"Successful S3 get_object response for time of day dimension. Status - {status}")
        time_of_day_df = pd.read_csv(response.get("Body"), keep_default_na=False)

    time_to_subtract = timedelta(minutes=19)     ###########################################
    data_date = datetime.today() - time_to_subtract
    data_date = data_date.astimezone(ZoneInfo("US/Pacific")).replace(tzinfo=None)
    minimum_diff = 1000
    time_key = ""
    for row in time_of_day_df.iterrows():
        time = row[1]["time_24h"]
        date_time_compare = datetime(data_date.year, data_date.month, data_date.day, int(time[0:2]), int(time[3:5]))
        diff = abs((data_date - date_time_compare).total_seconds())
        if diff < minimum_diff:
            minimum_diff = diff
            time_key = row[1]["time_of_day_id"]

    return time_key


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    date_id = get_date_id(s3_client)
    time_of_day_id = get_time_key(s3_client)

    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            port=os.environ["DB_PORT"]
        )
        cursor = conn.cursor()

        table_name = "streams"
        column_list = ""
        bucket_name = "twitchdatapipelineproject"
        region = "us-west-1"
        folder_path = f"raw/fact_table/{date_id}_{time_of_day_id}/"
        response = s3_client.list_objects_v2(Bucket="twitchdatapipelineproject", Delimiter='/', Prefix=folder_path)

        final_query = ""
        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].endswith(".csv"):
                    obj_key = obj["Key"]
                    query = f'''
                                SELECT aws_s3.table_import_from_s3(
                                    '{table_name}',
                                    '{column_list}',
                                    '(format csv, header true)',
                                    aws_commons.create_s3_uri('{bucket_name}', '{obj_key}', '{region}')
                                );
                            '''
                    final_query += query
        else:
            print("No CSV files found in the folder.")
            return {
                'statusCode': 200,
                'body': json.dumps('No CSV files found in the folder.')
            }        

        cursor.execute(final_query) # Executes all the queries that will insert data into the database

        conn.commit() # commits changes

    except Exception as e:
        print(f"An error occurred: {e}.")
        if conn:
            print("Rolling back transaction.")
            conn.rollback()

    finally:
        # Close the cursor and connection in the finally block
        if cursor:
            cursor.close()
        if conn:
            conn.close()


