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

# User will need to update
analyst = "andrew.bergman"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Start Runner Function #####

start_time = time.perf_counter()

##### Runner Function #####

def pipeline_runner():
    
##### End Runner Function ######
end_time = time.perf_counter()
elapsed_time = end_time - start_time

# Running the pipeline runner
if __name__ == "__main__":
    try:
        pipeline_runner()
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
