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
├─ dob_311_app.py                            # Streamlit app to run pipeline and perform analyses
├─ data_dictionary.md                        # Description of data layout from NYC Open Data
├─ NYC_DoB_311_Complaint_Codes_Dictionary.md # Descriptions of NYC DoB Complaint Codes
```

## Description

## Requirements

`datetime`

`duckdb`

`logging`

`os`

`pandas`

`requests`

`streamlit`

`time`

`traceback`


# Data Source

The [data](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data) for this project comes from NYC's Open Data [portal](https://opendata.cityofnewyork.us/).
