
# Raw Data From NYC DoB

Data from [NYC Open Data](https://data.cityofnewyork.us/Housing-Development/DOB-Complaints-Received/eabe-havv/about_data)

| API Name           | Column Name        | Data Type | Description                                                                                                        |
| ------------------ | ------------------ | --------- | ------------------------------------------------------------------------------------------------------------------ |
| complaint_number   | Complaint Number   | text      | Complaint number starting with borough code: (1= Manhattan, 2= Bronx, 3 = Brooklyn, 4 = Queens, 5 = Staten Island) |
| status             | Status             | text      | Status of Complaint                                                                                                |
| date_entered       | Date Entered       | text      | Date Complaint was Entered                                                                                         |
| house_number       | House Number       | text      | House Number of Complaint                                                                                          |
| house_street       | House Street       | text      | House Street of Complaint                                                                                          |
| zip_code           | ZIP Code           | text      | Zip code of complaint                                                                                              |
| bin                | BIN                | text      | Number assigned by City Planning to a specific building                                                            |
| community_board    | Community Board    | text      | 3-digit identifier: Borough code = first position, last 2 = community board                                        |
| special_district   | Special District   | text      | Is Complaint in Special District                                                                                   |
| complaint_category | Complaint Category | text      | DOB Complaint Category Code                                                                                        |
| unit               | Unit               | text      | Unit dispositioning Complaint                                                                                      |
| disposition_date   | Disposition Date   | text      | Date Complaint was Dispositioned                                                                                   |
| disposition_code   | Disposition Code   | text      | Disposition Code of Complaint                                                                                      |
| inspection_date    | Inspection Date    | text      | Inspection Date of Complaint                                                                                       |
| dobrundate         | DOBRunDate         | text      | Date when query is run and pushed to Open Data                                                                     |

---

# Cleaned Data 

| Column Name      | Data Type | Description                                                                                                        |
| ---------------- | --------- | ------------------------------------------------------------------------------------------------------------------ |
| id               | text      | Complaint number starting with borough code: (1= Manhattan, 2= Bronx, 3 = Brooklyn, 4 = Queens, 5 = Staten Island) |
| report_date      | datetime  | Date Complaint was Entered                                                                                         |
| comp_resolution  | text      | Resolution status: Resolved, Pending                                                                               |
| comp_category    | text      | DOB Complaint Category Code                                                                                        |
| description      | text      | Description of the DOB complaint code                                                                              |
| address          | text      | Building street address of complaint                                                                               |
| zip              | text      | Zip code of complaint                                                                                              |
| bin              | text      | Number assigned by City Planning to a specific building                                                            |
| community_board  | text      | 3-digit identifier: Borough code = first position, last 2 = community board                                        |
| special_district | text      | Is Complaint in Special District                                                                                   |
| comp_unit        | text      | Unit dispositioning Complaint                                                                                      |
| disp_code        | text      | Disposition Code of Complaint                                                                                      |
| insp_date        | datetime  | Inspection Date of Complaint                                                                                       |
| days_to_insp     | integer   | Elapsed time between                                                                                               |
