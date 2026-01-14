import json
import os
import pandas as pd
import awswrangler as wr
from sqlalchemy import create_engine
import psycopg2
import boto3

########################### SUMMARY ###########################
'''
    Inserts data into the Postgresql database. Executes when
    new dimension data is uploaded to temp_table_data folder
    in miscellaneous bucket.
'''
###############################################################



def lambda_handler(event, context):
    print("Event: ")
    print(event)
    print()

    if event:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        file_key = event["Records"][0]["s3"]["object"]["key"]
        region = event["Records"][0]["awsRegion"]
        start = 'temp_table_data/new_'
        end = '_data.csv'
        table_name = file_key[file_key.find(start)+len(start):file_key.rfind(end)]
        column_list = ""

        print()
        print(bucket_name)
        print(file_key)
        print(region)
        print(table_name)
        print()

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
