import os
import requests
from requests.exceptions import ConnectionError
from pathlib import Path
from datetime import datetime
import pandas as pd
import time

start = time.time()

repo_root = str(Path(__file__).parents[2])


# Gets client id and credentials
def get_credentials():
    client_id = os.environ["client_id"]
    access_token = os.environ["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Client-Id": f"{client_id}"
    }

    return headers


# Gets current categories streamed and current viewership data
def get_category_info():
    curr_categories_path = repo_root + "/data/miscellaneous/curr_streamed_categories.csv"
    curr_categories_df = pd.read_csv(curr_categories_path, keep_default_na = False, na_values = ["NA"])
    recent_categories_path = repo_root + /data/fact_table_data/
    curr_categories_IDs = curr_categories_df["category_id"].tolist()#[1:1000]####
    curr_categories_names = curr_categories_df["category_name"].tolist()#[1:1000]####

    return curr_categories_IDs, curr_categories_names



def main():
    headers = get_credentials()
    category_id_list, category_name_list = get_category_info()





if __name__ == "__main__":
    main()


end = time.time()#
duration = end - start#
print(duration)#