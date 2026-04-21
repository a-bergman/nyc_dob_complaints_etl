## Last Updated    : 2026-04-21
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

import datetime
import logging
import os

import duckdb as db
import pandas as pd
import streamlit as st

from pathlib import Path
from src.dob_etl_runner import pipeline_runner

# User will need to update
analyst = "streamlit-app"

# Unique ID obtained by opening inspector and searching for "octolytics-dimension-repository_id"
octo = "1210547273"

##### Directories & Files #############################################

PROJECT_ROOT = Path(__file__).resolve().parent

CLEAN_DIR = PROJECT_ROOT / "data" / "cleaned"

CLEAN_DATA = PROJECT_ROOT / "data" / "cleaned" / "dob_311_clean.db"

##### Datetime Info ###################################################

# Getting the date in YYYY-MM-DD format &
# the time in HH:MM format. Both are used for
# naming the log file. The HH:MM is used to
# prevent files being overridden
today = datetime.datetime.today().strftime("%Y-%m-%d")
run_time = str(datetime.datetime.now())[11:16].replace(":", "꞉")
# date-time down to minute/second for specific time logging
dt_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

##### Defining A Logger ###############################################

logging.basicConfig(
    filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-streamlit_etl_pipeline-log.log",
    format="%(levelname)s %(asctime)s :: %(message)s",
    level=logging.INFO,
)
# Silencing the streamlit app logging
logging.getLogger("streamlit").setLevel(logging.WARNING)
logging.getLogger("watchdog").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

##### Streamlit Page Layout ###########################################

st.set_page_config(page_title="311 Pipeline Dashboard", layout="centered")

st.title("NYC DOB 311 Pipeline Dashboard")

st.caption("Run the ETL pipeline and connect to the cleaned database")

with st.container(border=True):
    st.subheader("Pipeline Execution")

    col1, col2 = st.columns([1, 2])

    with col1:
        run_clicked = st.button(
            "Run Pipeline", type="primary", use_container_width=True
        )

    with col2:
        st.write("Execute full ETL process to build the cleaned DOB dataset.")

##### Main Functions ##################################################

if "etl_success" not in st.session_state:
    st.session_state.etl_success = False


# Runs the ETL process and displays a message
def run_etl():
    with st.status("Executing the pipeline...", expanded=True):
        pipeline_runner()
    st.session_state.etl_success = True


# Upon successful completion of the ETL process
# connects to the db & returns the connection
def db_connect():
    if not os.path.isfile(CLEAN_DATA):
        logging.error(f"{analyst}: File `dob_311_clean.db` does not exist!")
    return db.connect(CLEAN_DATA)


##### Run The Pipeline ################################################

if run_clicked:
    # Basic information about who ran this, when, and where
    logging.info(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.info(f"Analyst........{analyst}")
    logging.info(
        f"Script Run.....dob_311_app.py: dob_etl_runner.pipeline_runner(), extract.runner(), transform.runner(), load.runner()"
    )
    logging.info(f"Directory......{os.getcwd()} \n")
    logging.info(f"{analyst}: Ready to execute the pipeline")
    with st.status("Running ETL pipeline...", expanded=True) as status:
        try:
            st.write("Starting pipeline...")
            logging.info(f"{analyst}: Beginning to execute the pipeline \n")
            run_etl()

            st.session_state.etl_success = True

            st.write("Pipeline completed successfully!")
            logging.info(f"{analyst}: Completed the pipeline successfully")
            status.update(label="Pipeline complete", state="complete")

            st.success("ETL finished successfully")

        except Exception as ex:
            st.session_state.etl_success = False

            status.update(label="Pipeline failed", state="error")
            st.error("Pipeline execution failed")
            st.exception(ex)
            logging.error(f"{analyst}: {ex}")

##### Connect To The Cleaned Database #################################

with st.container(border=True):
    st.subheader("Database Connection")

    if st.session_state.get("etl_success", False):
        logging.info(f"{analyst}: Attempting connection to `dob_311_clean.db`")
        try:
            con = db_connect()
            st.success("Connected to `dob_311_clean.db`")
            st.write("Database is ready for queries and analysis")
            logging.info(f"{analyst}: Connected to `dob_311_clean.db`")

        except Exception as ex:
            st.error("Failed to connect to database")
            st.exception(ex)
            logging.error(f"{analyst}: Failed to connect to `dob_311_clean.db`")

    else:
        st.info("Run the pipeline first to enable database connection")

##### SQL Query: Complaints Per Borough ###############################

##### SQL Query: Most Common Complaints ###############################

##### SQL Query: Least Common Complaints ##############################

##### SQL Query: 10 Fastest Complaints To Inspection ##################

##### SQL Query: 10 Slowest Complaints To Inspection ##################

##### SQL Query: Top Fastest Days To Inspection Per Zip Code ##########

##### SQL Query: Bottom Fastest Days To Inspection Per Zip Code #######

##### SQL Query: Top 10 Most Number Of Complaints Per Zip Code ########

##### SQL Query: Bottom 10 Most Number Of Complaints Per Zip Code #####
