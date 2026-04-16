## Last Updated    : 2026-04-16
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
        filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-transform_runner-log.log",
        format="%(levelname)s %(asctime)s :: %(message)s",
        level=logging.DEBUG,
    )
    # Basic information about who ran this, when, and where
    logging.debug(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.debug(f"Analyst........{analyst}")
    logging.debug(f"Script Run.....transform.py: runner()")
    logging.debug(f"Directory......{os.getcwd()} \n")

    print(
        f">> [INFO] {analyst} @ {dt_now}: Beginning Transformation Of Raw NYC DoB 311 Data"
    )
    logging.debug(f"{analyst}: Beginning Transformation Of Raw NYC DoB 311 Data")

    ### Creating A duckdb Table ###

    # Defining the path to the raw data from the extract step
    data = pd.read_csv("../data/raw/raw_dob_311.csv")
    print(
        f">> [INFO] {analyst} @ {dt_now}: Loading raw data from: ../data/raw/raw_dob_311.csv"
    )
    logging.debug(f"{analyst}: Loading raw data from: ../data/raw/raw_dob_311.csv")

    # Formatting the query for the log
    transform_query = """
    CREATE TABLE dob_311_transformed AS
    SELECT
        complaint_number AS id,
        CONCAT(UPPER(LEFT(status, 1)), LOWER(RIGHT(status, LENGTH(status) - 1))) AS status,
        STRPTIME(CAST(date_entered AS VARCHAR), '%m/%d/%Y') AS report_date,
        complaint_category as comp_category,
        house_number,
        house_street,
        SUBSTR(CAST(zip_code AS VARCHAR), 1, 5) AS zip,
        bin,
        community_board,
        case when special_district is null then 'No' else special_district end AS special_district,
        unit as comp_unit,
        STRPTIME(CAST(disposition_date AS VARCHAR), '%m/%d/%Y') AS disp_date,
        disposition_code as disp_code,
        STRPTIME(CAST(inspection_date AS VARCHAR), '%m/%d/%Y') AS insp_date,
        case when status = 'Active' then NULL else DATEDIFF('day', report_date, insp_date) end AS days_to_insp,
        STRPTIME(SUBSTR(CAST(dobrundate AS VARCHAR), 1, 8), '%Y%m%d') AS run_date,
        case when insp_date IS NULL then 'Pending' else 'Resolved' end AS comp_resolution
    FROM
        data
    WHERE comp_resolution = 'Resolved'
        AND id IS NOT NULL
        AND report_date IS NOT NULL
        AND house_number IS NOT NULL
        AND house_street IS NOT NULL
        AND comp_unit IS NOT NULL
        AND zip IS NOT NULL
    """

    with duckdb.connect("../data/raw/dob_311_trans.db") as duck:
        duck.execute("DROP TABLE IF EXISTS dob_311_transformed")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Dropping table `dob_311_trans` if it exists"
        )
        logging.debug(f"{analyst}: Dropping table `dob_311_trans` if it exists")
        duck.execute(
            """
            CREATE TABLE dob_311_transformed AS
                SELECT
                    complaint_number AS id,
                    CONCAT(UPPER(LEFT(status, 1)), LOWER(RIGHT(status, LENGTH(status) - 1))) AS status,
                    STRPTIME(CAST(date_entered AS VARCHAR), '%m/%d/%Y') AS report_date,
                    complaint_category as comp_category,
                    house_number,
                    house_street,
                    SUBSTR(CAST(zip_code AS VARCHAR), 1, 5) AS zip,
                    bin,
                    community_board,
                    case when special_district is null then 'No' else special_district end AS special_district,
                    unit as comp_unit,
                    STRPTIME(CAST(disposition_date AS VARCHAR), '%m/%d/%Y') AS disp_date,
                    disposition_code as disp_code,
                    STRPTIME(CAST(inspection_date AS VARCHAR), '%m/%d/%Y') AS insp_date,
                    case when status = 'Active' then NULL else DATEDIFF('day', report_date, insp_date) end AS days_to_insp,
                    STRPTIME(SUBSTR(CAST(dobrundate AS VARCHAR), 1, 8), '%Y%m%d') AS run_date,
                    case when insp_date IS NULL then 'Pending' else 'Resolved' end AS comp_resolution
                FROM
                    data
                WHERE comp_resolution = 'Resolved'
                    AND id IS NOT NULL
                    AND report_date IS NOT NULL
                    AND house_number IS NOT NULL
                    AND house_street IS NOT NULL
                    AND comp_unit IS NOT NULL
                    AND zip IS NOT NULL
            """
        )
        print(f">> [INFO] {analyst} @ {dt_now}: Created table: dob_311_transformed")
        logging.debug(f"{analyst}: Created table: dob_311_transformed")
        logging.debug(f"{analyst}: SQL Executed: ")
        logging.debug(f"{analyst}: {transform_query}")
    # Defining a connection to the duckdb
    con = duckdb.connect("../data/raw/dob_311_trans.db")
    print(f">> [INFO] {analyst} @ {dt_now}: Connecting to table: dob_311_transformed")
    logging.debug(f"{analyst}: Connecting to table: dob_311_transformed")

    # Counting the number of rows loaded
    count = con.execute("SELECT COUNT(*) FROM dob_311_transformed").fetchone()[0]
    print(f">> [INFO] {analyst} @ {dt_now}: {count} rows loaded into: dob_311_trans.db")
    logging.debug(f"{analyst}: {count} rows loaded into: dob_311_trans.db")

    con.close()
    print(f">> [INFO] {analyst} @ {dt_now}: Database saved to: ../data/raw")
    logging.debug(f"{analyst}: Database saved to: ../data/raw")
    print(
        f">> [INFO] {analyst} @ {dt_now}: Closing connection to table: dob_311_transformed"
    )
    logging.debug(f"{analyst}: Closing connection table: dob_311_transformed")


end_time = time.perf_counter()
elapsed_time = end_time - start_time

# Running the transform runner
if __name__ == "__main__":
    try:
        runner()
        # need a better way to do the time
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `transform.runner()` ran in {elapsed_time} seconds"
        )
        logging.debug(f"{analyst}: `transform.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
