import pandas as pd
import boto3
import awswrangler as wr
import time

####################### SUMMARY #######################
'''
    Removes unnecessary columns from processed stream
    data CSV. Will be used for data to be inserted
    into PostgreSQL database.
'''
#######################################################


pd.options.mode.chained_assignment = None  # default='warn'

# Get processed stream data from S3
def get_processed_stream_data(bucket_name, file_key, s3_client):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    status = response["ResponseMetadata"]["HTTPStatusCode"]
    if status == 200:
        print(f"Successful S3 get_object response for the processed stream data. Status - {status}")
        processed_stream_df = pd.read_csv(response.get("Body"), keep_default_na = False)
    else:
        print(f"Unsuccessful S3 get_object response for the stream data. Status - {status}")
        print(response)
        exit()

    return processed_stream_df



def lambda_handler(event, context):
    start = time.time()
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    day_date_id = str(file_key[-17:-9])
    time_of_day_id = str(file_key[-8:-4])

    s3_client = boto3.client("s3")

    # Get processed stream data from S3
    processed_stream_df = get_processed_stream_data(bucket_name, file_key, s3_client)

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

    # Upload file as CSV to curated layer in S3
    wr.s3.to_csv(
            df=curated_stream_df,
            path=f"s3://twitch-project-curated-layer/curated_streams_data/{day_date_id}/curated_streams_data_{day_date_id}_{time_of_day_id}.csv",
            index=False
        )

    # Upload file as CSV to miscellaneous bucket so data can be inserted into postgres
    wr.s3.to_csv(
                df=curated_stream_df,
                path=f"s3://twitch-project-miscellaneous/temp_table_data/new_streams_data.csv",
                index=False
            )

    end = time.time()
    print("Duration: " + str(end - start))

    return {
        'statusCode': 200,
        'body': "Success"
    }
