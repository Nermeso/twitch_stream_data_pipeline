import json
import os
import pandas as pd
import awswrangler as wr
from sqlalchemy import create_engine
import psycopg2
import boto3
import ast

########################### SUMMARY ###########################
'''
    Inserts data into the Postgresql database. Executes when
    any object that is a CSV is uploaded to the curated layer
'''
###############################################################



def lambda_handler(event, context):
    if event:
        # Event input is slightly different for S3 trigger and SNS trigger
        try:
            event_source = event["Records"][0]["eventSource"]
        except:
            event_source = event["Records"][0]["EventSource"]

        # Get curated data info
        if event_source == "aws:s3": # users, genre_bridge, and game_mode_bridge data
            bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
            file_key = event["Records"][0]["s3"]["object"]["key"]
        elif event_source == "aws:sns": # categories and streams data
            event_notification = ast.literal_eval(event["Records"][0]["Sns"]["Message"])
            bucket_name = event_notification["Records"][0]["s3"]["bucket"]["name"]
            file_key = event_notification["Records"][0]["s3"]["object"]["key"]
        else:
            print("Invalid event source.")
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid event source.")
            }

        # Time info
        day_date_id = file_key.split("/")[1]
        time_of_day_id = file_key.split("/")[2][-8:-4] 

        # Table info
        start = "curated_"
        end = "_data"
        table_name = file_key.split(start)[1].split(end)[0]
        region = "us-west-2"
        column_list = ""

        try:
            conn = psycopg2.connect(
                host=os.environ["DB_HOST"],
                database=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASS"],
                port=os.environ["DB_PORT"]
            )
            cursor = conn.cursor()

            query = f'''
                        SELECT aws_s3.table_import_from_s3(
                            '{table_name}',
                            '{column_list}',
                            '(format csv, header true)',
                            aws_commons.create_s3_uri('{bucket_name}', '{file_key}', '{region}')
                        );
                    '''

            cursor.execute(query)
            conn.commit()

            print(f"Inserted data for table {table_name}!")


        except psycopg2.Error as e:
            print(f"An error occurred: {e}.")
            # If any operation fails, roll back the transaction
            if conn:
                print("Rolling back transaction.")
                conn.rollback()

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
