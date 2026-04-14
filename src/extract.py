## Last Updated    : 2026-04-14
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

import os
import traceback
import logging
import datetime
import pandas as pd
import requests as rq

##### User Analyst #####

# User will need to update
analyst = "andrew.bergman"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

###### API Configuration #####
api = "https://data.cityofnewyork.us/resource/eabe-havv.csv"

# Hard cap at 10,000
limit = 1000

# the date entered field has a space in it
order = "'Date Entered' DESC"

api_params = {"$limit": limit, "$order": order}

##### Raw Data Directory/File #####

raw_dir = "../data/raw"

raw_name = "raw_dob_311.csv"

##### Runner Function #####


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
        filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-etl_runner-log.log",
        format="%(levelname)s %(asctime)s :: %(message)s",
        level=logging.DEBUG,
    )
    # Basic information about who ran this, when, and where
    logging.debug(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.debug(f"Analyst........{analyst}")
    logging.debug(f"Script Run.....extract.py: runner()")
    logging.debug(f"Directory......{os.getcwd()} \n")

    print(f">> [INFO] {analyst} @ {dt_now}: Beginning Extraction Of NYC Open Data...")
    logging.debug(f"{analyst}: Beginning Extraction For NYC Open Data...")

    ### Connection ###

    # Requesting data from NYC Open Data
    # In the `try`/`except`` to catch connection/http errors
    try:
        print(
            f">> [INFO] {analyst} @ {dt_now}: Making a request to NYC Open Data......."
        )
        logging.debug(f"{analyst}: Making a request to NYC Open Data........")
        # Analyst will need to update parameters above if needed; timeout set to 60 seconds
        response = rq.get(url=api, params=api_params, timeout=60)
        response.raise_for_status()
        response_time = response.elapsed.total_seconds()
        response_len = format(len(response.content), ",")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Request Successful: {limit} rows of data ({response_len} bytes) in {response_time} seconds"
        )
        logging.debug(
            f"{analyst}: Request Successful: {limit} rows of data ({response_len} bytes) in {response_time} seconds"
        )
    except rq.exceptions.HTTPError as ex:
        print(f">> [ERROR] {analyst} @ {dt_now}: An HTTP Error Occurred: {ex}")
        logging.debug(f"[ERROR] {analyst}: An HTTP Error Occurred: {ex}")
    except rq.exceptions.RequestException as ex:
        print(f">> [ERROR] {analyst} @ {dt_now}: A Request Error Occurred: ex")
        logging.debug(f"[ERROR] {analyst}: A Request Error Occurred: ex")

    ### Saving Data ###

    ## TO DO: create the directory if it doesn't exist

    # Checking to see if the raw data directory exists
    if os.path.isdir(raw_dir):
        # Creating a full file path
        raw_data = os.path.join(raw_dir, raw_name)
        # Writing the request response to a .csv file
        # Need "wb" since this isn't just text
        with open(raw_data, "wb") as data:
            data.write(response.content)
    else:
        print(f">> [ERROR] {analyst} @ {dt_now}: {raw_dir} is not a directory")
        logging.debug(f"[ERROR] {analyst} : {raw_dir} is not a directory")

    ### Printing Confirmation ###

    raw_df = pd.read_csv(os.path.join(raw_dir, raw_name))
    print(
        f">> [INFO] {analyst} @ {dt_now}: Extraction Successful: {raw_df.shape[0]} rows and {raw_df.shape[1]} columns of data"
    )
    logging.debug(
        f"{analyst}: Extraction Successful: {raw_df.shape[0]} rows and {raw_df.shape[1]} columns of data"
    )
    print(f">> [INFO] {analyst} @ {dt_now}: `{raw_name}` saved in: /{raw_dir}/")
    logging.debug(f"{analyst}: `{raw_name}` saved in: /{raw_dir}/")

    return raw_df


# Running the extract runner
if __name__ == "__main__":
    try:
        runner()
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
