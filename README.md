# NYC DoB Complaints

Exploring Department of Buildings (DOB) complaints using NYC Open Data.

## Project Structure

```
nyc_dob_complaints_etl/
├─ data/
│  ├─ clean                                  # Cleaned data & db files (ignored))
│  ├─ raw                                    # Raw data from NYC Open Data (ignored)
│  ├─ README.md
├─ src/
│  ├─ dob_etl_runner.py                      # Runs the entire pipeline
│  ├─ extract.py                             # Extracts data from NYC Open Data
│  ├─ transform.py                           # Transforms & cleans data using DuckDB SQL
│  ├─ load.py                                # Loads clean data into DuckDB table
├─ .gitignore
├─ README.md
├─ application.py                            # Streamlit app to run pipeline and perform analyses
├─ data_dictionary.md                        # Description of data layout from NYC Open Data
├─ NYC_DoB_311_Complaint_Codes_Dictionary.md # Descriptions of NYC DoB Complaint Codes
```

## Description

This is a simple ETL pipe that extracts, manipulates, and loads data from NYC DoB Complaints to a Streamlit dashboard which runs in a series of steps:

1. Extracts data, maps descriptions to two ID columns, and saves the output to a .csv file;
2. Transforms the data in a DuckDB database using with SQL to transform, add, and clean the data;
3. Loads a subset of the clean data into another DuckDB database from which the Streamlit app selects;
4. Launches a Streamlit app that provides complaint statistics calculated using SQL.

## Using The App

### On Linux

```
1. Clone the repository using SSH
2. Navigate to the main folder: cd nyc_dob_complaints_etl
3. Launch a virtual environment: python3 -m venv venv
4. Activate the virtual environment . venv/bin/activate
5. Install requirements: pip install -r requirements.txt
6. Launch the application: streamlit run application.py 
```

### On Windows

```
1. Clone the repository using SSH
2. Navigate to the main folder: cd nyc_dob_complaints_etl
3. Launch a virtual environment: python3 -m venv venv
4. Activate the virtual environment .venv\Scripts\activate
5. Install requirements: pip install -r requirements.txt
6. Launch the application: streamlit run application.py 
```

## Data Source

The [data](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data) for this project comes from NYC's Open Data [portal](https://opendata.cityofnewyork.us/).
