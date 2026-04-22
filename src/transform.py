## Last Updated    : 2026-04-21
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

from pathlib import Path

##### User Analyst #####

# User will need to update
analyst = "etl-transform"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Directories & Files #####

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA = PROJECT_ROOT / "data" / "raw" / "dob_311_extract.csv"

RAW_EXTRACT = PROJECT_ROOT / "data" / "raw" / "dob_311_trans.db"

RAW_BOROUGH = PROJECT_ROOT / "data" / "raw" / "borough_map.db"

DESC_PATH = PROJECT_ROOT / "data" / "raw" / "nyc_311_dob_comp_codes.csv"

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
        level=logging.INFO,
    )
    print(
        f">> [INFO] {analyst} @ {dt_now}: Beginning Transformation Of Raw NYC DoB 311 Data"
    )
    logging.info(f"{analyst}: Beginning Transformation Of Raw NYC DoB 311 Data")

    ### Creating A duckdb Table ###

    # Defining the path to the raw data from the extract step
    data = pd.read_csv(RAW_DATA)
    print(
        f">> [INFO] {analyst} @ {dt_now}: Loading raw data from: ../data/raw/dob_311_extract.csv"
    )
    logging.info(f"{analyst}: Loading raw data from: ../data/raw/raw_dob_311.csv")

    transform_query = """
    CREATE TABLE dob_311_transformed AS    
    WITH borough_map AS (SELECT '1' AS code, 'Manhattan' AS borough UNION ALL
                         SELECT '2', 'Bronx' UNION ALL
                         SELECT '3', 'Brooklyn' UNION ALL
                         SELECT '4', 'Queens' UNION ALL
                         SELECT '5', 'Staten Island')
    SELECT
        d.complaint_number AS id,
        m.borough,
        CONCAT(UPPER(LEFT(d.status, 1)), LOWER(SUBSTR(d.status, 2))) AS status,
        STRPTIME(CAST(d.date_entered AS VARCHAR), '%m/%d/%Y') AS report_date,
        d.complaint_category AS comp_category,
        d.complaint_description AS description,
        d.house_number,
        d.house_street,
        SUBSTR(CAST(d.zip_code AS VARCHAR), 1, 5) AS zip,
        d.bin,
        d.community_board,
        CASE WHEN d.special_district IS NULL THEN 'No' 
                ELSE d.special_district 
                END AS special_district,
        d.unit AS comp_unit,
        STRPTIME(CAST(d.disposition_date AS VARCHAR), '%m/%d/%Y') AS disp_date,
        d.disposition_code AS disp_code,
        d.bis_description as disp_description,
        STRPTIME(CAST(d.inspection_date AS VARCHAR), '%m/%d/%Y') AS insp_date,
        CASE WHEN d.status = 'Active' THEN NULL 
                ELSE DATEDIFF('day', STRPTIME(CAST(d.date_entered AS VARCHAR), '%m/%d/%Y'),
                                     STRPTIME(CAST(d.inspection_date AS VARCHAR), '%m/%d/%Y'))
                END AS days_to_insp,
        STRPTIME(SUBSTR(CAST(d.dobrundate AS VARCHAR), 1, 8), '%Y%m%d') AS run_date,
        CASE WHEN d.inspection_date IS NULL THEN 'Pending' ELSE 'Resolved' END AS comp_resolution,
        CASE WHEN d.disposition_code = 'I2'THEN 'No Violation'
             WHEN d.disposition_code = 'XX' THEN 'Other'
             ELSE 'Violation'
             END AS action
    FROM data AS d
    LEFT JOIN borough_map AS m
        ON SUBSTRING(CAST(d.complaint_number AS TEXT), 1, 1) = m.code
    WHERE
        d.complaint_number IS NOT NULL
        AND d.date_entered IS NOT NULL
        AND d.house_number IS NOT NULL
        AND d.house_street IS NOT NULL
        AND d.unit IS NOT NULL
        AND d.zip_code IS NOT NULL
        AND d.inspection_date IS NOT NULL;
    """

    with duckdb.connect(RAW_EXTRACT) as duck:
        duck.execute("DROP TABLE IF EXISTS dob_311_transformed")
        print(
            f">> [INFO] {analyst} @ {dt_now}: Dropping table `dob_311_trans` if it exists"
        )
        logging.info(f"{analyst}: Dropping table `dob_311_trans` if it exists")
        # There's no input here so we can run the query defined above
        duck.execute(transform_query)
        print(f">> [INFO] {analyst} @ {dt_now}: Created table: dob_311_transformed")
        logging.info(f"{analyst}: Created table: dob_311_transformed")
        logging.info(f"{analyst}: SQL Executed: {transform_query}")
    # Defining a connection to the duckdb
    con = duckdb.connect(RAW_EXTRACT)
    print(f">> [INFO] {analyst} @ {dt_now}: Connecting to table: dob_311_transformed")
    logging.info(f"{analyst}: Connecting to table: dob_311_transformed")

    # Counting the number of rows loaded
    rows = con.execute("SELECT COUNT(*) FROM dob_311_transformed").fetchone()[0]
    cols = con.execute(
        "SELECT COUNT(*) AS column_count FROM information_schema.columns WHERE table_name = 'dob_311_transformed'"
    ).fetchone()[0]
    print(
        f">> [INFO] {analyst} @ {dt_now}: {rows} rows and {cols} columns loaded into: dob_311_trans.db"
    )
    logging.info(
        f"{analyst}: {rows} rows and {cols} columns rows loaded into: dob_311_trans.db"
    )

    con.close()
    print(f">> [INFO] {analyst} @ {dt_now}: Database saved to: ../data/raw")
    logging.info(f"{analyst}: Database saved to: ../data/raw")
    print(
        f">> [INFO] {analyst} @ {dt_now}: Closing connection to table: dob_311_transformed"
    )
    logging.info(f"{analyst}: Closing connection table: dob_311_transformed")


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
        logging.info(f"{analyst}: `transform.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
