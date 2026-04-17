## Last Updated    : 2026-04-17
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

##### User Analyst #####

# User will need to update
analyst = "andrew.bergman"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

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
        level=logging.DEBUG,
    )
    # Basic information about who ran this, when, and where
    logging.debug(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.debug(f"Analyst........{analyst}")
    logging.debug(f"Script Run.....load.py: runner()")
    logging.debug(f"Directory......{os.getcwd()} \n")

    print(
        f">> [INFO] {analyst} @ {dt_now}: Beginning to load the transformed NYC DoB 311 data"
    )
    logging.debug(f"{analyst}: Beginning to load the transformed NYC DoB 311 data")

    ### Creating A duckdb Table ###

    # Formatting the query for the log
    load_query = """
    CREATE TABLE dob_311_clean AS
        SELECT
            id,
            comp_resolution,
            comp_category,
            house_number,
            CONCAT(house_number, ' ', house_street) AS address,
            zip,
            bin,
            community_board,
            special_district,
            comp_unit,
            disp_code,
            insp_date,
            days_to_insp
        FROM trans.dob_311_transformed
    """

    # Defining the path to the transform db
    transformed = "../data/raw/dob_311_trans.db"

    with duckdb.connect("../data/cleaned/dob_311_clean.db") as duck:
        duck.execute("DROP TABLE IF EXISTS dob_311_clean")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Dropping table `dob_311_clean` if it exists"
        )
        logging.debug(f"{analyst}: Dropping table `dob_311_clean` if it exists")
        duck.execute(f"ATTACH '{transformed}' AS trans")
        print(f">> [INFO] {analyst} @ {dt_now}: Attaching table: dob_311_transformed")
        logging.debug(f"{analyst}: Attaching table: dob_311_transformed")
        duck.execute(
            """
            CREATE TABLE dob_311_clean AS
                SELECT
                    id,
                    comp_resolution,
                    comp_category,
                    house_number,
                    CONCAT(house_number, ' ', house_street) AS address,
                    zip,
                    bin,
                    community_board,
                    special_district,
                    comp_unit,
                    disp_code,
                    insp_date,
                    days_to_insp
                FROM trans.dob_311_transformed
            """
        )
        print(f">> [INFO] {analyst} @ {dt_now}: Created table: dob_311_clean")
        logging.debug(f"{analyst}: Created table: dob_311_clean")
        logging.debug(f"{analyst}: SQL Executed: {load_query}")

        # Defining a connection to the duckdb
        print(f">> [INFO] {analyst} @ {dt_now}: Connecting to table: dob_311_clean")
        logging.debug(f"{analyst}: Connecting to table: dob_311_clean")

        ### Counting the number of rows loaded ###
        count = duck.execute("SELECT COUNT(*) FROM dob_311_clean").fetchone()[0]
        print(
            f">> [INFO] {analyst} @ {dt_now}: {count} rows loaded into: dob_311_clean.db"
        )
        logging.debug(f"{analyst}: {count} rows loaded into: dob_311_clean.db")

        ### Calculating Top Complaints ###

        complaint_query = """
                SELECT comp_category, COUNT(*) AS count
                FROM dob_311_clean
                GROUP BY comp_category
                ORDER BY count DESC
                LIMIT 10
            """

        print(f">> [INFO] {analyst} @ {dt_now}: Calculating most frequent complaints")
        top_comp = duck.execute(
            """ SELECT comp_category, COUNT(*) AS count
                                    FROM dob_311_clean
                                    GROUP BY comp_category
                                    ORDER BY count DESC
                                    LIMIT 5
                                """
        ).df()
        data_tab = [
            [i, cat, total]
            for i, (cat, total) in enumerate(
                zip(top_comp["comp_category"], top_comp["count"]), start=1
            )
        ]
        generate_table(data_tab, headers=["Rank", "Complaint Type", "Count"])

        logging.debug(f"{analyst}: Executed SQL: {complaint_query}")

        ### Calculating Days To Inspection By Complaint ###

        ### Calculating Complaints By Zip Code###

        ### Calculating Days To Inspection By Complaint ###

        duck.close()
        print(f">> [INFO] {analyst} @ {dt_now}: Database saved to: ../data/clean")
        logging.debug(f"{analyst}: Database saved to: ../data/clean")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Closing connection to table: dob_311_clean"
        )
        logging.debug(f"{analyst}: Closing connection table: dob_311_clean")


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
        logging.debug(f"{analyst}: `load.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
