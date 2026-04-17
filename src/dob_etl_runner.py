## Last Updated    : 2026-04-17
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

##### User Analyst #####

import datetime
import logging
import time
import traceback
import extract
import transform
import load
import os

# User will need to update
analyst = "andrew.bergman"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Start Runner Function #####

start_time = time.perf_counter()

##### Runner Function #####


def pipeline_runner():

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
        filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-nyc_311_pipeline_runner-log.log",
        format="%(levelname)s %(asctime)s :: %(message)s",
        level=logging.DEBUG,
    )
    # Basic information about who ran this, when, and where
    logging.debug(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.debug(f"Analyst........{analyst}")
    logging.debug(
        f"Script Run.....dob_etl_runner.py: dob_etl_runner.pipeline_runner(), extract.runner(), transform.runner(), load.runner()"
    )
    logging.debug(f"Directory......{os.getcwd()} \n")

    print(f">> [INFO] {analyst} @ {dt_now}: Beginning NYC DoB 311 ETL pipeline \n")
    logging.debug(f"{analyst}: Beginning NYC DoB 311 ETL pipeline \n")

    try:

        # Extract - Step 1
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 1 - extraction")
        logging.debug(
            f"{analyst}: Beginning pipeline step 1 - extraction - `extract.runner()`"
        )
        extract.runner()
        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 1 - extraction \n"
        )
        logging.debug(
            f"{analyst}: Completed pipeline step 1 - extraction - `extract.runner()` \n"
        )

        # Transform - Step 2
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 2 - transform")
        logging.debug(
            f"{analyst}: Beginning pipeline step 2 - transform - `transform.runner()`"
        )
        transform.runner()
        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 2 - transform \n"
        )
        logging.debug(
            f"{analyst}: Completed pipeline step 2 - transform - `transform.runner()` \n"
        )

        # Load - Step 3
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 3 - load")
        logging.debug(f"{analyst}: Beginning pipeline step 3 - load - `load.runner()`")
        load.runner()
        print(f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 3 - load \n")
        logging.debug(
            f"{analyst}: Completed pipeline step 3 - load - `load.runner()` \n"
        )

        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed NYC DoB 311 ETL pipeline successfully"
        )
        logging.debug(f"{analyst}: Completed NYC DoB 311 ETL pipeline successfully \n")

    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )


##### End Runner Function ######
end_time = time.perf_counter()
elapsed_time = end_time - start_time

# Running the pipeline runner
if __name__ == "__main__":
    try:
        pipeline_runner()
        # need a better way to do the time
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `dob_etl_runner.pipeline_runner()` ran in {round(elapsed_time,10)} seconds"
        )
        logging.debug(f"{analyst}: `load.runner()` ran in {elapsed_time} seconds")
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
