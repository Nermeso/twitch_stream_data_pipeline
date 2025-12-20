import os
import time
import pandas as pd
import psycopg2
import boto3

def lambda_handler(event, context):
    if event:
        new_data_path = event["new_data_path"]
        obj_key = "/".join(new_data_path.split("/", 3)[3:])
        table_name = event["table_name"]
        column_list = ""
        bucket_name = "twitchdatapipelineproject"
        region = "us-west-1"

        print(f"Inserting data for table {table_name}")

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
                            aws_commons.create_s3_uri('{bucket_name}', '{obj_key}', '{region}')
                        );
                    '''

            cursor.execute(query)
            conn.commit()

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

            # Delete the temporary new category data file            
            s3_client = boto3.client("s3")
            s3_client.delete_object(Bucket=bucket_name, Key=obj_key)

            

