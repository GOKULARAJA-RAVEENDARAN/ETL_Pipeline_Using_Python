# ETL_Pipeline_Using_Python
A Comprehensive ETL Workflow with Python for Data Engineers

OBJECTIVES:
1.	Extract data from CSV, JSON, and XML files.
2.	Transform raw data into a standardized format, including:
   - Heights from inches to meters
   - Weights from pounds to kilograms
3.	Load transformed data into a CSV file for database use or analysis.
4.	Log every step of the ETL process with timestamps for monitoring and debugging.

ETL_Project/   WORKFLOW
│
├── data/                         # Folder containing source CSV, JSON, and XML files
├── etl_pipeline.py              # Main ETL logic with extraction, transformation, loading
├── log_file.txt                 # Log file recording each ETL operation with timestamps
├── transformed_data.csv         # Output file with cleaned and transformed data

Technologies Used
•	Python 3
•	pandas – For data manipulation
•	glob – For handling multiple file types
•	xml.etree.ElementTree – For parsing XML data
•	datetime – For generating timestamps during logging
#################################################1###########################################
1️) Extraction Phase
•	Extracts data from all files in the `data/` directory.
•	Supports:
•	  - .csv files using pandas.read_csv
•	  - .json files using pandas.read_json
•	  - .xml files using ElementTree
•	All extracted data is combined into a single DataFrame.
####################################################2########################################
2️) Transformation Phase
•	Converts height from inches to meters.
•	Converts weight from pounds to kilograms.
•	Ensures data consistency and prepares it for loading.
##################################################3##########################################
3️) Loading Phase
•	Saves the transformed data into transformed_data.csv.
•	File is ready for direct import into any relational database (MySQL, PostgreSQL, etc.)
###################################################4#########################################
4️) Logging
•	Each ETL phase logs:
•	  - Start time
•	  - Completion time
•	  - Status messages
•	All logs are stored in log_file.txt.
#############################################################################################
HOW TO RUN : python project2.py
