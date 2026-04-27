## Last Updated    : 2026-04-27
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

##### User Analyst #####

import datetime
import logging
import traceback

from . import extract
from . import transform
from . import load

# User will need to update
analyst = "etl-pipeline_runner"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

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
        level=logging.INFO,
    )

    print(f">> [INFO] {analyst} @ {dt_now}: Beginning NYC DoB 311 ETL pipeline \n")
    logging.info(f"{analyst}: Beginning NYC DoB 311 ETL pipeline \n")

    try:

        ########## Extract - Step 1 ##########
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 1 - extraction")
        logging.info(
            f"{analyst}: Beginning pipeline step 1 - extraction - `extract.runner()`"
        )
        extract.runner()
        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 1 - extraction \n"
        )
        logging.info(
            f"{analyst}: Completed pipeline step 1 - extraction - `extract.runner()` \n"
        )

        ########## Transform - Step 2 ##########
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 2 - transform")
        logging.info(
            f"{analyst}: Beginning pipeline step 2 - transform - `transform.runner()`"
        )
        transform.runner()
        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 2 - transform \n"
        )
        logging.info(
            f"{analyst}: Completed pipeline step 2 - transform - `transform.runner()` \n"
        )

        ########## Load - Step 3 ##########
        print(f">> [INFO] {analyst} @ {dt_now}: Beginning pipeline step 3 - load")
        logging.info(f"{analyst}: Beginning pipeline step 3 - load - `load.runner()`")
        load.runner()
        print(f">> [INFO] {analyst} @ {dt_now}: Completed pipeline step 3 - load \n")
        logging.info(
            f"{analyst}: Completed pipeline step 3 - load - `load.runner()` \n"
        )

        print(
            f">> [INFO] {analyst} @ {dt_now}: Completed NYC DoB 311 ETL pipeline successfully"
        )
        logging.info(f"{analyst}: Completed NYC DoB 311 ETL pipeline successfully \n")

    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )


# Running the pipeline runner
if __name__ == "__main__":
    try:
        pipeline_runner()
        # need a better way to do the time
        print(
            f">> [INFO] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: `dob_etl_runner.pipeline_runner()` completed successfully"
        )
        logging.info(
            f"{analyst}: `dob_etl_runner.runner()` ran in completed successfully"
        )
    # Catches any error that crops up; bare `except` clauses are discouraged
    except Exception as ex:
        logging.error(traceback.format_exc())
        print(
            f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
        )
