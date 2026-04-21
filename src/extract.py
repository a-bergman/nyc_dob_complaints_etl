## Last Updated    : 2026-04-21
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

import os
import traceback
import logging
import datetime
import time
import pandas as pd
import requests as rq
from pathlib import Path

##### User Analyst #####

# User will need to update
analyst = "etl-extract"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

###### API Configuration #####
api = "https://data.cityofnewyork.us/resource/eabe-havv.csv"

# Hard cap at 10,000
limit = 10000

# the date entered field has a space in it
order = "'Date Entered' DESC"

api_params = {"$limit": limit, "$order": order}

##### Raw Data Directory/File #####

raw_dir = "../data/raw"

raw_name = "raw_dob_311.csv"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_PATH = PROJECT_ROOT / "data" / "raw"

RAW_DATA = PROJECT_ROOT / "data" / "raw" / "raw_dob_311.csv"

RAW_EXTRACT = PROJECT_ROOT / "data" / "raw" / "dob_311_extract.csv"

DESC_PATH = PROJECT_ROOT / "data" / "raw" / "nyc_311_dob_comp_codes.csv"

##### Runner Function #####

start_time = time.perf_counter()


def runner():
    """This function connects to NYC Open Data, requests the 311 DoB Complaints data, saves the file, and returns some
    data about the file.

    Returns:
        raw_dob_311.csv: A .csv file containing the raw data from the pull
        etl_runner-log.log: A dynamically named logging file containing step by step information from the execution of the script
    """

    # Getting the date in YYYY-MM-DD format &
    # the time in HH:MM format. Both are used for
    # naming the log file. The HH:MM is used to
    # prevent files being overridden
    today = datetime.datetime.today().strftime("%Y-%m-%d")
    run_time = str(datetime.datetime.now())[11:16].replace(":", "꞉")
    # date-time down to minute/second for specific time logging
    dt_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ### Creating A Logger ###

    # Defining the location (will need to be updated), file name, message formatting, and level
    logging.basicConfig(
        filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-extract_runner-log.log",
        format="%(levelname)s %(asctime)s :: %(message)s",
        level=logging.INFO,
    )

    print(f">> [INFO] {analyst} @ {dt_now}: Beginning Extraction Of NYC Open Data")
    logging.info(f"{analyst}: Beginning Extraction For NYC Open Data")

    ### Connection ###

    # Requesting data from NYC Open Data
    # In the `try`/`except`` to catch connection/http errors
    try:
        print(f">> [INFO] {analyst} @ {dt_now}: Making a request to NYC Open Data")
        logging.info(f"{analyst}: Making a request to NYC Open Data")
        # Analyst will need to update parameters above if needed; timeout set to 60 seconds
        response = rq.get(url=api, params=api_params, timeout=60)
        response.raise_for_status()
        response_time = response.elapsed.total_seconds()
        response_len = format(len(response.content), ",")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Request Successful: {limit} rows of data ({response_len} bytes) in {response_time} seconds"
        )
        logging.info(
            f"{analyst}: Request Successful: {limit} rows of data ({response_len} bytes) in {response_time} seconds"
        )
    except rq.exceptions.HTTPError as ex:
        print(f">> [ERROR] {analyst} @ {dt_now}: An HTTP Error Occurred: {ex}")
        logging.error(f"[ERROR] {analyst}: An HTTP Error Occurred: {ex}")
    except rq.exceptions.RequestException as ex:
        print(f">> [ERROR] {analyst} @ {dt_now}: A Request Error Occurred: ex")
        logging.error(f"[ERROR] {analyst}: A Request Error Occurred: ex")

    ### Saving Data ###

    ## TO DO: create the directory if it doesn't exist

    # Checking to see if the raw data directory exists
    if os.path.isdir(RAW_PATH):
        # Creating a full file path
        raw_data = RAW_DATA
        # Writing the request response to a .csv file
        # Need "wb" since this isn't just text
        with open(raw_data, "wb") as data:
            data.write(response.content)
    else:
        print(f">> [ERROR] {analyst} @ {dt_now}: {raw_dir} is not a directory")
        logging.error(f"[ERROR] {analyst} : {raw_dir} is not a directory")

    ### Mapping complaint codes to a description from NYC DoB ###

    # Reading a csv in that has code:description
    comp_names = pd.read_csv(DESC_PATH)

    # Converting the .csv file into a dictionary for mapping
    comp_dict = dict(zip(comp_names["comp_code"], comp_names["desc"]))

    ### Printing Confirmation ###

    raw_df = pd.read_csv(RAW_DATA)
    print(
        f">> [INFO] {analyst} @ {dt_now}: Extraction Successful: {raw_df.shape[0]} rows and {raw_df.shape[1]} columns of data"
    )
    logging.info(
        f"{analyst}: Extraction Successful: {raw_df.shape[0]} rows and {raw_df.shape[1]} columns of data"
    )
    # Mapping code to description
    raw_df["complaint_description"] = raw_df["complaint_category"].map(
        comp_dict, na_action="ignore"
    )
    raw_df.to_csv(RAW_EXTRACT)
    print(
        f">> [INFO] {analyst} @ {dt_now}: Created new column `comp_desc`; mapped codes to NYC DoB description"
    )
    logging.info(
        f"{analyst}: `{raw_name}` Created new column `comp_desc`; mapped codes to NYC DoB description"
    )
    print(
        f">> [INFO] {analyst} @ {dt_now}: `dob_311_extract.csv` saved in: /{raw_dir}/"
    )
    logging.info(f"{analyst}: `dob_311_extract.csv` saved in: /{raw_dir}/")


end_time = time.perf_counter()
elapsed_time = end_time - start_time

# Running the extract runner
if __name__ == "__main__":
    try:
        runner()
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `extract.runner()` ran in {elapsed_time} seconds"
        )
        logging.info(f"{analyst}: `extract.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
