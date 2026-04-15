## Last Updated    : 2026-04-15
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

import datetime
import duckdb
import logging
import os
import traceback
import time
import pandas as pd

##### User Analyst #####

# User will need to update
analyst = "andrew.bergman"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Runner Function #####

start_time = time.perf_counter()

##### Runner Function #####

start_time = time.perf_counter()


def runner():

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
        filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-load_runner-log.log",
        format="%(levelname)s %(asctime)s :: %(message)s",
        level=logging.DEBUG,
    )
    # Basic information about who ran this, when, and where
    logging.debug(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.debug(f"Analyst........{analyst}")
    logging.debug(f"Script Run.....load.py: runner()")
    logging.debug(f"Directory......{os.getcwd()} \n")

    print(f">> [INFO] {analyst} @ {dt_now}: Loading Transformed NYC DoB 311 Data")
    logging.debug(f"{analyst}: Loading Transformed Raw NYC DoB 311 Data")

    ### Creating A duckdb Table ###

    # Formatting the query for the log
    transform_query = """ """

    with duckdb.connect("../data/cleaned/dob_311_clean.db") as duck:
        duck.execute("DROP TABLE IF EXISTS dob_311_trans")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Dropping table `dob_311_clean` if it exists"
        )
        logging.debug(f"{analyst}: Dropping table `dob_311_clean` if it exists")
        duck.execute(""" """)
        print(f">> [INFO] {analyst} @ {dt_now}: Created table: dob_311_clean")
        logging.debug(f"{analyst}: Created table: dob_311_clean")
        logging.debug(f"{analyst}: SQL Executed: ")
        logging.debug(f"{analyst}: {transform_query}")
    # Defining a connection to the duckdb
    con = duckdb.connect("../data/cleaned/dob_311_clean.db")
    print(f">> [INFO] {analyst} @ {dt_now}: Connecting to table: dob_311_clean")
    logging.debug(f"{analyst}: Connecting to table: dob_311_clean")

    # Basic stats

    con.close()
    print(f">> [INFO] {analyst} @ {dt_now}: Database saved to: ../data/cleaned")
    logging.debug(f"{analyst}: Database saved to: ../data/cleaned")
    print(f">> [INFO] {analyst} @ {dt_now}: Closing connection to table: dob_311_clean")
    logging.debug(f"{analyst}: Closing connection table: dob_311_clean")


end_time = time.perf_counter()
elapsed_time = end_time - start_time

# Running the transform runner
if __name__ == "__main__":
    try:
        runner()
        # need a better way to do the time
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `load.runner()` ran in {elapsed_time} seconds"
        )
        logging.debug(f"{analyst}: `load.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
