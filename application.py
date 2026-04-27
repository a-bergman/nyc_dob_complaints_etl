## Last Updated    : 2026-04-23
## Last Updated By : andrew-bergman
## Project Version : 1.0

# Analyst will need to update their paths
# Logs stored in: Windows : N/A
# Logs stored in: Linux   : `/home/andrew-bergman/Documents/Python Logs`

import datetime
import logging
import os

import altair as alt
import duckdb as db
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

##### Analysis Queries ################################################


# Source of population numbers for `sql_query_1`:
# https://www.nyc.gov/assets/planning/downloads/pdf/our-work/reports/new-york-city-population-estimates-and-trends_may-2025.pdf


sql_query_1 = """
    WITH borough_pop_2024 AS (SELECT * FROM (VALUES
                                                ('Manhattan', 1660664),
                                                ('Brooklyn', 2617631),
                                                ('Queens', 2316841),
                                                ('Bronx', 1384724),
                                                ('Staten Island', 498212)) AS t(borough, population))
    SELECT 
        c.borough AS Borough,
        COUNT(*) AS Total,
        ROUND(COUNT(*) * 100000.0 / p.population, 2) AS "Complaints Per 100k",
        ROUND(AVG(days_to_insp)) AS 'Average Response (Days)'
    FROM dob_311_clean as c
    JOIN borough_pop_2024 p
        ON c.borough = p.borough
    GROUP BY c.borough, p.population
    ORDER BY c.borough ASC
"""

sql_most_common_comp = f"""
    SELECT 
        comp_category AS 'Complaint Code', 
        description AS Description, 
        COUNT(*) AS Total 
    FROM dob_311_clean 
    GROUP BY comp_category, description
    ORDER BY Total DESC
    LIMIT X
"""

sql_least_common_comp = f"""
    SELECT 
        comp_category AS 'Complaint Code', 
        description AS Description, 
        COUNT(*) AS Total 
    FROM dob_311_clean 
    GROUP BY comp_category, description
    ORDER BY Total ASC
    LIMIT X
"""

sql_fastest_insp = f"""
    SELECT
        comp_category AS 'Complaint Code',
        description AS Description,
        days_to_insp AS 'Days To Inspection' 
    FROM dob_311_clean 
    ORDER BY days_to_insp ASC
    LIMIT X
"""

sql_slowest_insp = f"""
    SELECT
        comp_category AS 'Complaint Code',
        description AS Description,
        days_to_insp AS 'Days To Inspection' 
    FROM dob_311_clean 
    ORDER BY days_to_insp DESC
    LIMIT X
"""

sql_zip_most_comp = """
    SELECT 
        borough AS Borough,
        zip AS ZIP,
        COUNT(id) AS Total
    FROM dob_311_clean
    GROUP BY borough,zip
    ORDER BY Total DESC
    LIMIT X
"""

sql_zip_least_comp = """
    SELECT 
        borough AS Borough,
        zip AS ZIP,
        COUNT(id) AS Total
    FROM dob_311_clean
    GROUP BY borough,zip
    ORDER BY Total ASC
    LIMIT X
"""

sql_most_comp_building = """
    SELECT 
        address AS 'Street Address',
        bin AS 'Building ID',
        COUNT(id) AS Total
    FROM dob_311_clean
    GROUP BY bin,address
    ORDER BY Total DESC
    LIMIT 10
"""

sql_action_count = """
    SELECT 
        action AS Action,
        COUNT(action) AS Count,
        CONCAT(ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2), '%') AS 'Count Percentage'
    FROM dob_311_clean
    GROUP BY action
    ORDER BY count DESC
"""

sql_month_agg = """
    SELECT 
        DATE_TRUNC('month', report_date) AS Month,
        COUNT(*) AS Count
    FROM dob_311_clean
    WHERE month > '2015-12-31'
    GROUP BY 1
    ORDER BY 1
"""

sql_year_agg = """
    SELECT 
        EXTRACT(year FROM report_date) AS Year,
        COUNT(*) AS Total
    FROM dob_311_clean
    GROUP BY 1
    ORDER BY 1
"""

sql_df_insp_time = """
    SELECT
        borough AS Borough,
        quantile_cont(days_to_insp, 0.25) AS 'P25%',
        quantile_cont(days_to_insp, 0.5)  AS Median,
        quantile_cont(days_to_insp, 0.75) AS 'P75%',
        quantile_cont(days_to_insp, 0.9)  AS 'P90%'
    FROM dob_311_clean
    GROUP BY borough
    ORDER BY borough
"""

sql_box_insp_time = """
    SELECT
        borough AS Borough,
        days_to_insp AS 'Response Time',
    FROM dob_311_clean
"""

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
    filename=f"/home/andrew-bergman/Documents/Python Logs/{octo}-{today}@{run_time}-streamlit_app-log.log",
    format="%(levelname)s %(asctime)s :: %(message)s",
    level=logging.INFO,
)
# Silencing the streamlit app logging
logging.getLogger("streamlit").setLevel(logging.WARNING)
logging.getLogger("watchdog").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

##### Streamlit Page Layout ###########################################

st.set_page_config(page_title="311 Pipeline Dashboard", layout="wide")

st.title("NYC DOB 311 Pipeline Dashboard")

st.caption("Run The ETL Pipeline & Connect To The Cleaned Database")

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
    st.cache_data.clear()
    # Basic information about who ran this, when, and where
    logging.info(f"Day............{today} @ {str(datetime.datetime.now())[11:16]}")
    logging.info(f"Analyst........{analyst}")
    logging.info(
        f"Script Run.....dob_311_app.py: dob_etl_runner.pipeline_runner(), extract.runner(), transform.runner(), load.runner()"
    )
    logging.info(f"Directory......{os.getcwd()} \n")
    logging.info(f"{analyst}: Ready to execute the pipeline \n")
    with st.status("Running ETL pipeline...", expanded=True) as status:
        try:
            st.write("Starting pipeline... \n")
            logging.info(f"{analyst}: Beginning to execute the pipeline \n")
            run_etl()

            st.session_state.etl_success = True

            st.write("Pipeline completed successfully!")
            logging.info(f"{analyst}: Completed the pipeline successfully \n")
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
            logging.info(f"{analyst}: Preparing to run analysis queries \n")

            ##### Running Analysis Queries ##################################

            print(f"\n>> [INFO] {analyst} @ {dt_now}: Preparing to run analyses")

            ##### SQL Query: Complaints, Complaints Per 100k, & Mean Response Per Borough #########

            st.subheader(
                "Total Number of Complaints, Complaints Per 100k, And Mean Response By Borough"
            )

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating total number of complaints, complaints per 100k, and mean response by borough"
            )

            # No user input; can use predefined query
            query_total_avg_resp = con.execute(sql_query_1).df()
            logging.info(
                f"{analyst}: Calculating total number of complaints, complaints per 100k, and mean response by borough"
            )
            logging.info(f"{analyst}: SQL Executed & DataFrame created: {sql_query_1}")
            st.dataframe(query_total_avg_resp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Quartiles Of Complaint Response Times By Borough #####

            st.subheader("Median and 90th Percentile Of Days To Inspection")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the 25th, Median, 75th, and 90% percentiles of days to ispection"
            )
            # No user input; can use predefined query
            query_df_response = con.execute(sql_df_insp_time).df()
            query_box_response = con.execute(sql_box_insp_time).df()
            logging.info(
                f"{analyst}: Calculating the 25th, Median, 75th, and 90% percentiles of days to ispection"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_df_insp_time}"
            )
            logging.info(
                f"{analyst}: SQL Executed & box plot created: {sql_box_insp_time}"
            )
            # box plot for easy visuals
            box = (
                alt.Chart(query_box_response)
                .mark_boxplot(extent="min-max")
                .encode(
                    alt.Y("Borough:N"),
                    alt.X("Response Time:Q").scale(zero=False),
                    alt.Color(
                        "Borough:N",
                        scale=alt.Scale(
                            range=[
                                "#1F77B4",
                                "#b45d1f",
                                "#5d1fb4",
                                "#B41F76",
                                "#1FB45D",
                            ]
                        ),
                    ),
                )
                .properties(height=350, title=" ")
                .configure(background="#FAFAFA")
                .configure_axis(labelColor="black", titleColor="black")
                .configure_legend(labelColor="black", titleColor="black")
            )
            # Box plot for easy viewing
            st.altair_chart(box, width="stretch")

            # DataFrame for specifics
            st.dataframe(query_df_response, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Breakdown Of Complaint Action ########################

            st.subheader("Breakdown Of Complaints: Violation Or Not")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating breakdown of complaint actions"
            )

            # No user input; can use predefined query
            query_action_count = con.execute(sql_action_count).df()
            logging.info(f"{analyst}: Calculating breakdown of complaint actions")
            logging.info(
                f"{analyst}: SQL Executed & bar chart created: {sql_action_count}"
            )
            chart = (
                alt.Chart(query_action_count)
                .mark_bar()
                .encode(
                    x=alt.X("Action:N", sort="-y", axis=alt.Axis(labels=False)),
                    y=alt.Y("Count:Q"),
                    color=alt.Color(
                        "Action:N", scale=alt.Scale(range=["#1F77B4", "#b45d1f"])
                    ),
                    tooltip=["Action:N", "Count:Q"],
                )
            )
            st.altair_chart(chart, width="stretch")

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Total Complaints By Year #############################

            st.subheader("Total Complaints Per Year")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating total complaints by year"
            )

            # No user input; can use predefined query
            query_year_comp = con.execute(sql_year_agg).df()
            logging.info(f"{analyst}: Calculating the total complaints by year")
            logging.info(f"{analyst}: SQL Executed & bar chart created: {sql_year_agg}")

            year_chart = (
                alt.Chart(query_year_comp)
                .mark_bar()
                .encode(
                    x=alt.X("Year:O", title=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("Total:Q", title=None),
                    color=alt.Color("Year:O", scale=alt.Scale(scheme="dark2")),
                )
                .properties(width=alt.Step(45))
            )
            st.altair_chart(year_chart, width="stretch")

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Monthly Complaint Trends #############################

            st.subheader("Monthly complaint trend")
            st.text("Monthly aggregation over time")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the monthly complaint trends"
            )

            # No user input; can use predefined query
            query_month_comp = con.execute(sql_month_agg).df()
            logging.info(f"{analyst}: Calculating the monthly complaint trends")
            logging.info(
                f"{analyst}: SQL Executed & timeseries line chart created: {sql_month_agg}"
            )
            m_chart = (
                alt.Chart(query_month_comp)
                .mark_line(color="#b45d1f")
                .encode(x=alt.X("Month:T", title=None), y=alt.Y("Count:Q", title=None))
                .properties(height=350)
            )
            st.altair_chart(m_chart, width="stretch")

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Top 10 Most Number Of Complaints Per Zip Code ########

            n = st.slider("Count", 3, 15)

            st.subheader(f"Zip Codes With The {n} Most Complaints")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the zip codes with the {n} most complaints"
            )

            query_zip_most_comp = con.execute(
                f"""
                SELECT 
                    borough AS Borough,
                    zip AS ZIP,
                    COUNT(id) AS Total
                FROM dob_311_clean
                GROUP BY borough,zip
                ORDER BY Total DESC
                LIMIT {n}
                """
            )
            logging.info(
                f"{analyst}: Calculating the zip codes with the {n} most complaints"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_zip_most_comp}"
            )
            st.dataframe(query_zip_most_comp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Bottom 10 Least Number Of Complaints Per Zip Code #####

            st.subheader(f"Zip Codes With The {n} Least Complaints")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the zip codes with the {n} least complaints"
            )

            query_zip_least_comp = con.execute(
                f"""
                SELECT 
                    borough AS Borough,
                    zip AS ZIP,
                    COUNT(id) AS Total
                FROM dob_311_clean
                GROUP BY borough,zip
                ORDER BY Total ASC
                LIMIT {n}
                """
            )
            logging.info(
                f"{analyst}: Calculating the zip codes with the {n} least complaints"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_zip_least_comp}"
            )
            st.dataframe(query_zip_least_comp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Buildings With The Most Complaints ##################

            st.subheader("Buildings With The Most Complaints")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the buildings with the most complaints"
            )

            # No user input; can use predefined query
            query_most_comp_building = con.execute(sql_most_comp_building)
            logging.info(
                f"{analyst}: Calculating the buildings with the most complaints"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_most_comp_building}"
            )
            st.dataframe(query_most_comp_building, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Most Common Complaints ##############################

            st.subheader("Most Common Complaints")

            limit = st.slider("Count", 5, 20)

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the {limit} most common complaints"
            )

            query_most_common_comp = con.execute(
                f"""
                SELECT 
                    comp_category AS 'Complaint Code', 
                    description AS Description, 
                    COUNT(*) AS Total 
                FROM dob_311_clean 
                GROUP BY comp_category, description
                ORDER BY Total DESC
                LIMIT {limit}
            """
            )
            logging.info(f"{analyst}: Calculating the {limit} most common complaints")
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_most_common_comp}"
            )
            st.dataframe(query_most_common_comp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: Least Common Complaints ##############################

            st.subheader("Least Common Complaints")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the {limit} least common complaints"
            )

            query_least_common_comp = con.execute(
                f"""
                SELECT 
                    comp_category AS 'Complaint Code', 
                    description AS Description, 
                    COUNT(*) AS Total 
                FROM dob_311_clean 
                GROUP BY comp_category, description
                ORDER BY Total ASC
                LIMIT {limit}
            """
            )
            logging.info(f"{analyst}: Calculating the {limit} least common complaints")
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_least_common_comp}"
            )
            st.dataframe(query_least_common_comp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: N Fastest Complaints To Inspection ##################

            st.subheader("Fastest Time To An Inspection Per Complaint")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the {limit} fastest times to inspection per complaint"
            )

            query_fastest_insp = con.execute(
                f"""SELECT
                        comp_category AS 'Complaint Code',
                        description AS Description,
                        days_to_insp AS 'Days To Inspection' 
                    FROM dob_311_clean 
                    ORDER BY days_to_insp ASC
                    LIMIT {limit}"""
            ).df()
            logging.info(
                f"{analyst}: Calculating the {limit} fastest times to inspection per complaint"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_fastest_insp}"
            )
            st.dataframe(query_fastest_insp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            ##### SQL Query: N Slowest Complaints To Inspection ##################

            st.subheader("Slowest Time To An Inspection Per Complaint")

            print(
                f">> [INFO] {analyst} @ {dt_now}: Calculating the {limit} slowest times to inspection per complaint"
            )

            query_slowest_insp = con.execute(
                f"""SELECT
                        comp_category AS 'Complaint Code',
                        description AS Description,
                        days_to_insp AS 'Days To Inspection' 
                    FROM dob_311_clean 
                    ORDER BY days_to_insp DESC
                    LIMIT {limit}"""
            ).df()
            logging.info(
                f"{analyst}: Calculating the {limit} slowest times to inspection per complaint"
            )
            logging.info(
                f"{analyst}: SQL Executed & DataFrame created: {sql_slowest_insp}"
            )
            st.dataframe(query_slowest_insp, hide_index=True)

            print(f">> [INFO] {analyst} @ {dt_now}: Calculation completed successfully")

            logging.info(
                f"\n {analyst}: The entire ETL Pipeline and all SQL analyses have all completed successfully"
            )

            print(
                f"\n >> [INFO] {analyst} @ {dt_now}: ETL pipeline and SQL analyses are all complete"
            )

        except Exception as ex:
            st.error("Failed to connect to database")
            st.exception(ex)
            logging.error(f"{analyst}: Failed to connect to `dob_311_clean.db`")
            print(
                f">> [ERROR] {analyst} @ {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: An Error Occurred:  {ex}"
            )

    else:
        st.info(
            "Run the pipeline first to enable database connection & perform analyses"
        )
