## Last Updated    : 2026-04-22
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
import tabulate as tb
from pathlib import Path

##### User Analyst #####

# User will need to update
analyst = "etl-load"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Directories & Files #####

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEAN_DATA = PROJECT_ROOT / "data" / "cleaned" / "dob_311_clean.db"

RAW_TRANSFORMED = PROJECT_ROOT / "data" / "raw" / "dob_311_trans.db"

##### Helper Function #####


def generate_table(data, headers):
    """
    Parameters:
    -----------
    headers : list of str column header names for the table : list : :
    data    : list of var data to be included in the table  : list : :

    Description:
    ------------
    Takes the formatted measure statements from the above functions and creates a table for display

    Returns:
    A formatted table in the command line showing the values extracted from the JSON object
    """
    print(tb.tabulate(data, headers=headers, tablefmt="outline"))


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
        level=logging.INFO,
    )
    print(
        f">> [INFO] {analyst} @ {dt_now}: Beginning to load the transformed NYC DoB 311 data"
    )
    logging.info(f"{analyst}: Beginning to load the transformed NYC DoB 311 data")

    ### Creating A duckdb Table ###

    # Formatting the query for the log
    load_query = """
    CREATE TABLE dob_311_clean AS
        SELECT
            id,
            borough,
            report_date,
            comp_resolution,
            comp_category,
            description,
            action,
            CONCAT(house_number, ' ', house_street) AS address,
            zip,
            bin,
            community_board,
            special_district,
            comp_unit,
            disp_code,
            disp_description,
            insp_date,
            CASE WHEN STDDEV_SAMP(days_to_insp) OVER () = 0 
                 THEN NULL 
                 ELSE (days_to_insp - AVG(days_to_insp) OVER ()) / STDDEV_SAMP(days_to_insp) OVER ()
                 END AS zscore,
            days_to_insp
        FROM trans.dob_311_transformed
        WHERE days_to_insp >= 0
        QUALIFY ABS(zscore) < 2
    """
    logging.info(f"{analyst}: Loading transformed data from `dob_311_trans.db`")
    with duckdb.connect(CLEAN_DATA) as duck:
        duck.execute("DROP TABLE IF EXISTS dob_311_clean")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Dropping table `dob_311_clean` if it exists"
        )
        logging.info(f"{analyst}: Dropping table `dob_311_clean` if it exists")
        duck.execute(f"ATTACH '{RAW_TRANSFORMED}' AS trans")
        print(f">> [INFO] {analyst} @ {dt_now}: Attaching table: dob_311_transformed")
        logging.info(f"{analyst}: Attaching table: dob_311_transformed")
        # There's no input here so we can run the query defined above
        duck.execute(load_query)
        print(f">> [INFO] {analyst} @ {dt_now}: Created table: dob_311_clean")
        logging.info(f"{analyst}: Created table: dob_311_clean")
        logging.info(f"{analyst}: SQL Executed: {load_query}")

        # Defining a connection to the duckdb
        print(f">> [INFO] {analyst} @ {dt_now}: Connecting to table: dob_311_clean")
        logging.info(f"{analyst}: Connecting to table: dob_311_clean")

        ##### Counting the number of rows loaded #####
        rows = duck.execute("SELECT COUNT(*) FROM dob_311_clean").fetchone()[0]
        cols = duck.execute(
            "SELECT COUNT(*) AS column_count FROM information_schema.columns WHERE table_name = 'dob_311_clean'"
        ).fetchone()[0]
        print(
            f">> [INFO] {analyst} @ {dt_now}: {rows} rows and {cols} columns loaded into: dob_311_clean.db"
        )
        logging.info(f"{analyst}: {rows} rows and {cols} loaded into: dob_311_clean.db")

        duck.close()
        print(f">> [INFO] {analyst} @ {dt_now}: Database saved to: ../data/clean")
        logging.info(f"{analyst}: Database saved to: ../data/clean")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Closing connection to table: dob_311_clean"
        )
        logging.info(f"{analyst}: Closing connection table: dob_311_clean")


end_time = time.perf_counter()
elapsed_time = end_time - start_time


# Running the transform runner
if __name__ == "__main__":
    try:
        runner()
        # need a better way to do the time
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `load.runner()` ran in {round(elapsed_time,5)} seconds"
        )
        logging.info(f"{analyst}: `load.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
