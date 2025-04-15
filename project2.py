"""
A Comprehensive ETL Workflow with Python for Data Engineers

Author: GOKULARAJA R
"""

# Importing Modules
import os
import glob
import json
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Logging function
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log_file.txt", "a") as f:
        f.write(f"{timestamp} - {message}\n")

# Extraction
def extract_from_csv(file_path):                                #for csv
    log(f"Extracting data from CSV file: {file_path}")
    return pd.read_csv(file_path)

def extract_from_json(file_path):                               #for json
    log(f"Extracting data from JSON file: {file_path}")
    try:
        return pd.read_json(file_path, lines=True)
    except ValueError as e:
        log(f"Error reading JSON file {file_path}: {str(e)}")
        return pd.DataFrame()

def extract_from_xml(file_path):                                #for extracting from xml
    log(f"Extracting data from XML file: {file_path}")
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        rows = []
        for person in root.findall('person'):
            name = person.find('name').text
            height = float(person.find('height').text)
            weight = float(person.find('weight').text)
            rows.append({"name": name, "height": height, "weight": weight})
        return pd.DataFrame(rows)
    except Exception as e:
        log(f"Error reading XML file {file_path}: {str(e)}")
        return pd.DataFrame()

def extract_file(file_path):                     #extract file and calling the respectice filetype's function
    if file_path.endswith(".csv"):
        return extract_from_csv(file_path)
    elif file_path.endswith(".json"):
        return extract_from_json(file_path)
    elif file_path.endswith(".xml"):
        return extract_from_xml(file_path)
    else:
        log(f"Unsupported file format: {file_path}")          #if someother file type is there gives us a prompt
        return pd.DataFrame()

def extract_data(files):
    log("Starting extraction phase.")
    all_records = []

    for file_path in files:
        df = extract_file(file_path)
        all_records.extend(df.to_dict(orient="records"))

    all_data = pd.DataFrame(all_records)
    log("Extraction completed.")
    return all_data

# Transformation
def transform_data(df):
    log("Transforming data: converting height (inches to meters), weight (pounds to kg)")

    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()

    df['height'] = df['height'].astype(float) * 0.0254
    df['weight'] = df['weight'].astype(float) * 0.453592
    df = df.round({'height': 2, 'weight': 2})

    log("Transformation completed.")
    return df

# Loading
def load_data(df):
    output_path = "transformed_data.csv"
    log(f"Loading data into {output_path}")
    df.to_csv(output_path, index=False)

#  ETL Execution main function head 
def run_etl():
    log("ETL Job Started")

    files = glob.glob("*.*")
    log(f"Found {len(files)} files to process.")

    # Extraction
    log("Phase 1: Extraction Started")
    extracted_data = extract_data(files)
    log(f"Extracted {len(extracted_data)} records.")
    log("Phase 1: Extraction Completed")

    # Transformation of the data (converting height (inches to meters), weight (pounds to kg))
    log("Phase 2: Transformation Started")
    transformed_data = transform_data(extracted_data)

    # Duplicate removal because we have source 1,2,3 duplicated in the unzipped file
    log("Removing duplicate records (if any).")
    transformed_data = transformed_data.drop_duplicates()
    log(f"Data after duplicate removal: {len(transformed_data)} records.")
    log("Phase 2: Transformation Completed")

    # Loading into CSV 
    log("Phase 3: Loading Started")
    load_data(transformed_data)
    log("Phase 3: Loading Completed")

    log("ETL Job Finished")
    log("#############################################################################################################")


#Main 
if __name__ == "__main__":
    run_etl()
    