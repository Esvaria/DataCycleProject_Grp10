# ETL Process Documentation

This document describes the ETL (Extract, Transform, Load) process that feeds data into the machine data import system.

## Summary

This ETL process provides a robust and automated solution for importing machine data into the SQL Server database. It includes:

1. **Data Extraction** from the SMB server
2. **Data Transformation** through Bronze and Silver stages
3. **Data Loading** into the SQL Server database
4. **Automation** through GitHub Actions
5. **Backup and History** preservation at each stage

The system is designed to be fault-tolerant, with tracking of processed files to avoid duplicates, validation of data to ensure quality, and error handling throughout the process.


## Overview

The ETL process consists of three main stages:

1. **Extract**: Python scripts download raw data files from the SMB server
2. **Transform**: Python scripts process the raw data files and convert them to the "Silver" format
3. **Load**: SQL Server stored procedures import the "Silver" formatted data into the database

## Directory Structure

The ETL process uses the following directory structure:

```
DataCycleProject/
│
├── EversysDatFiles/                 # Source directory for raw .dat files
│
├── BronzeRawData/                   # Bronze layer - Consolidated raw data  
│   ├── Cleaning/
│   │   ├── current/                 # Current consolidated data
│   │   │   ├── Cleaning.dat         # Main data file
│   │   │   └── cleaned_lines.txt    # Tracking file for processed lines
│   │   └── YYYY/MM/DD/              # Historical data organized by date
│   ├── Rinse/
│   │   ├── current/                 # Current consolidated data
│   │   │   ├── Rinse.dat            # Main data file
│   │   │   └── cleaned_lines.txt    # Tracking file for processed lines
│   │   └── YYYY/MM/DD/ 
│   ├── Info/
│   │   ├── current/                 # Current consolidated data
│   │   │   ├── Info.dat             # Main data file
│   │   │   └── cleaned_lines.txt    # Tracking file for processed lines
│   │   └── YYYY/MM/DD/
│   └── Product/
│   │   ├── current/                 # Current consolidated data
│   │   │   ├── Product.dat          # Main data file
│   │   │   └── cleaned_lines.txt    # Tracking file for processed lines
│   │   └── YYYY/MM/DD/ 
│
├── SilverRawData/                   # Silver layer - Cleaned and validated data
│   ├── Cleaning/
│   │   ├── current/                 # Current cleaned data
│   │   │   └── Silver_Cleaning.dat  # Main cleaned data file
│   │   └── YYYY/MM/DD/              # Historical data organized by date
│   ├── Rinse/
│   │   ├── current/                 # Current cleaned data
│   │   │   └── Silver_Rinse.dat     # Main cleaned data file
│   │   └── YYYY/MM/DD/              # Historical data organized by date
│   ├── Info/
│   │   ├── current/                 # Current cleaned data
│   │   │   └── Silver_Info.dat      # Main cleaned data file
│   │   └── YYYY/MM/DD/              # Historical data organized by date
│   └── Product/
│   │   ├── current/                 # Current cleaned data
│   │   │   └── Silver_Product.dat   # Main cleaned data file
│   │   └── YYYY/MM/DD/              # Historical data organized by date
│
├── TempDatFiles/                    # Temporary processing directory
│
├── processed_files.txt              # Tracker file for processed source files
│
└── scripts/                         # Python processing scripts
    ├── bronze_processor.py          # Bronze layer ETL
    ├── silver_cleaning.py           # Silver layer ETL for cleaning data
    ├── silver_info.py               # Silver layer ETL for info messages
    └── silver_rinse.py              # Silver layer ETL for rinse operations
```

## Python Scripts

### 1. SMB Data Downloader

This script connects to the SMB server and downloads the raw data files.

**File: `downloadeversysdata.py`**

```python
import os
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, FilePipePrinterAccessMask
from smbprotocol.file_info import FileInformationClass
from smbprotocol.open import ImpersonationLevel, CreateDisposition, CreateOptions, ShareAccess, FileAttributes

# Configuration SMB
SMB_SERVER = "10.130.25.152"
SMB_SHARE = "Eversys"
SMB_USER = "Student"
SMB_PASSWORD = "3uw.AQ!SWxsDBm2zi3"

# Local folder to save DAT files (placed one level up from the script directory)
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
DAT_FILES_FOLDER = os.path.join(BASE_DIR, "EversysDatFiles")
os.makedirs(DAT_FILES_FOLDER, exist_ok=True)

# Connect to SMB
def connect_smb():
    try:
        conn = Connection(uuid.uuid4(), SMB_SERVER, 445)
        conn.connect(timeout=30)

        session = Session(conn, SMB_USER, SMB_PASSWORD)
        session.connect()

        tree = TreeConnect(session, f"\\\\{SMB_SERVER}\\{SMB_SHARE}")
        tree.connect()

        print(f"Successfully connected to SMB: {SMB_SERVER}\\{SMB_SHARE}")
        return tree, conn, session
    except Exception as e:
        print(f"SMB Connection failed: {e}")
        return None, None, None

# Retrieve SMB file list
def list_files(tree):
    """Retrieve list of DAT files from the SMB share."""
    try:
        dir_open = Open(tree, "")
        dir_open.create(
            impersonation_level=ImpersonationLevel.Impersonation,
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,
            file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_OPEN,
            create_options=CreateOptions.FILE_DIRECTORY_FILE
        )

        entries = []
        while True:
            try:
                batch = dir_open.query_directory(pattern="*", file_information_class=FileInformationClass.FILE_NAMES_INFORMATION)
                batch_files = [entry["file_name"].get_value().decode("utf-16-le") for entry in batch]

                # Filter only .DAT files and remove "." and ".."
                batch_files = [f for f in batch_files if f.endswith(".dat") and f not in (".", "..")]

                if not batch_files:
                    break

                entries.extend(batch_files)
            except Exception as e:
                if "STATUS_NO_MORE_FILES" in str(e):
                    print("No more files to retrieve from the server.")
                    break
                else:
                    print(f"Error retrieving batch: {e}")
                    break

        dir_open.close()
        print(f"Total DAT files found: {len(entries)}")
        return entries
    except Exception as e:
        print(f"Error listing files: {e}")
        return []

# Check if file already exists locally
def local_file_exists(filename):
    return os.path.exists(os.path.join(DAT_FILES_FOLDER, filename))

# Download SMB file
def download_file(tree, filename):
    local_path = os.path.join(DAT_FILES_FOLDER, filename)

    if local_file_exists(filename):
        print(f"Skipping {filename} - Already exists locally.")
        return None

    try:
        file_open = Open(tree, filename)
        file_open.create(
            impersonation_level=ImpersonationLevel.Impersonation,
            desired_access=FilePipePrinterAccessMask.GENERIC_READ,
            file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
            share_access=ShareAccess.FILE_SHARE_READ,
            create_disposition=CreateDisposition.FILE_OPEN,
            create_options=CreateOptions.FILE_NON_DIRECTORY_FILE
        )

        data = file_open.read(0, file_open.end_of_file)
        file_open.close()

        if data:
            with open(local_path, "wb") as f:
                f.write(data)
            print(f"Downloaded {filename}")
            return local_path
        else:
            print(f"Skipping {filename}: Empty file")
            return None
    except Exception as e:
        print(f"Error opening file: {filename} - {e}")
        return None

# Multithreading for faster downloads
def threaded_download(tree, file_list):
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda f: download_file(tree, f), file_list)

# Main function
def main():
    tree, conn, session = connect_smb()
    if not tree:
        return

    all_files = list_files(tree)
    new_files = [f for f in all_files if not local_file_exists(f)]

    if not new_files:
        print("No new DAT files to download.")
    else:
        print(f"Downloading {len(new_files)} new DAT files...")
        threaded_download(tree, new_files)

    session.disconnect()
    conn.disconnect()
    print("All DAT files have been downloaded successfully.")

if __name__ == "__main__":
    main()
```

### 2. Bronze Data Processor

This script processes the raw data files and converts them to the "Bronze" format.

**File: `process_bronze_data.py`**

```python
import os
import pandas as pd
from io import StringIO
from datetime import datetime
from shutil import copy2

# Configuration
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))  # Move one level up
DAT_FILES_FOLDER = os.path.join(BASE_DIR, "EversysDatFiles")  # Folder containing raw .dat files
OUTPUT_FOLDER = os.path.join(BASE_DIR, "BronzeRawData")  # Final output folder for merged .dat files
TEMP_FOLDER = os.path.join(BASE_DIR, "TempDatFiles")  # Temporary folder to process files safely
PROCESSED_FILES_TRACKER = os.path.join(BASE_DIR, "processed_files.txt")  # Processed files tracker

# Ensure necessary directories exist
os.makedirs(TEMP_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# File Type Mapping & Folder Structure
FILE_MAPPING = {
    "Cleaning_History": "Cleaning",
    "Rinse_History": "Rinse",
    "Info_Message_History": "Info",
    "Product_History": "Product"
}

# Load processed files (ensure file exists)
def load_processed_files():
    if not os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, "w"):  # Create the file if missing
            pass
    with open(PROCESSED_FILES_TRACKER, "r") as f:
        return set(f.read().splitlines())

# Save a processed file immediately
def save_processed_file(filename):
    with open(PROCESSED_FILES_TRACKER, "a") as f:
        f.write(f"{filename}\n")

# List all DAT files in the folder
def list_local_files():
    if not os.path.exists(DAT_FILES_FOLDER):  # Check if folder exists
        print(f"The folder '{DAT_FILES_FOLDER}' does not exist. Exiting process.")
        return []  # Return empty list, preventing errors
    return [f for f in os.listdir(DAT_FILES_FOLDER) if f.endswith(".dat")]

# Create a backup copy in history folder (year/month/day/)
def backup_file(source_path, category_folder):
    now = datetime.now()
    history_folder = os.path.join(category_folder, str(now.year), f"{now.month:02d}", f"{now.day:02d}")

    os.makedirs(history_folder, exist_ok=True)  # Ensure history directory exists
    backup_path = os.path.join(history_folder, os.path.basename(source_path))
    
    copy2(source_path, backup_path)  # Copy file for historization
    print(f"Backup created: {backup_path}")

# Process and temporarily store each file
def process_files(files, processed_files):
    for filename in files:
        if filename in processed_files:  # Ensure we don't reprocess already saved files
            continue

        file_path = os.path.join(DAT_FILES_FOLDER, filename)
        file_type = filename.split("-")[-1].replace(".dat", "").replace(".DAT", "")
        category = FILE_MAPPING.get(file_type)

        if not category:
            continue  # Skip unrecognized files

        # Define paths
        category_folder = os.path.join(OUTPUT_FOLDER, category)
        current_folder = os.path.join(category_folder, "current")
        temp_path = os.path.join(TEMP_FOLDER, filename)  # Temp file path
        output_path = os.path.join(current_folder, f"{category}.dat")  # Final .dat file

        os.makedirs(current_folder, exist_ok=True)  # Ensure category and current folder exist

        is_new_file = not os.path.exists(output_path)

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            df = pd.read_csv(StringIO(content), delimiter=';', dtype=str)

            if df.empty:
                print(f"Skipping empty file: {filename}")
                continue

            # Write fully processed data to the temp folder first
            with open(temp_path, "w", encoding="utf-8") as f_temp:
                df.to_csv(f_temp, sep=';', index=False, header=is_new_file)

            # Move the fully processed file to the final location
            with open(temp_path, "r", encoding="utf-8") as f_temp, open(output_path, "a", encoding="utf-8") as f_out:
                if not is_new_file:  # Skip header if appending to an existing file
                    next(f_temp)
                for line in f_temp:
                    f_out.write(line)

            # Remove the temporary file after successful processing
            os.remove(temp_path)

            print(f"Merged {filename} into {output_path}")

            #  Backup before marking as processed
            backup_file(output_path, category_folder)

            #  IMMEDIATELY mark file as processed
            save_processed_file(filename)

        except Exception as e:
            print(f"Error processing file {filename}: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)  # Cleanup temp file on failure

# Ensure that at least one file exists in each "current" folder
def ensure_current_files():
    for category in FILE_MAPPING.values():
        category_folder = os.path.join(OUTPUT_FOLDER, category)
        current_folder = os.path.join(category_folder, "current")
        output_path = os.path.join(current_folder, f"{category}.dat")

        os.makedirs(current_folder, exist_ok=True)  # Ensure the "current" folder exists

        if not os.path.exists(output_path):  # If no file exists, create an empty one
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("")  # Create an empty .dat file

            print(f"Created empty placeholder file: {output_path}")

        # Always create a backup even if no new files were processed
        backup_file(output_path, category_folder)

# Main function
def main():
    if not os.path.exists(DAT_FILES_FOLDER):
        print(f"The folder '{DAT_FILES_FOLDER}' does not exist. Exiting process.")
        return

    processed_files = load_processed_files()
    all_files = list_local_files()
    new_files = [f for f in all_files if f not in processed_files]  # Only process untracked files

    if new_files:
        print(f"Found {len(new_files)} new DAT files to process.")
        process_files(new_files, processed_files)
        print("Processing complete.")
    else:
        print("🔹 No new files to process.")

    ensure_current_files()

if __name__ == "__main__":
    print("Starting new execution cycle...")
    main()
```

### 3. Silver Data Processor - Cleaning Data

This script processes the "Bronze" data into the "Silver" format for the Cleaning logs.

**File: `process_silver_cleaning.py`**

```python
import os
import csv
import pandas as pd
from datetime import datetime
import hashlib
from shutil import copy2

# === CONFIG ===
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
INPUT_FILE = os.path.join(BASE_DIR, "BronzeRawData", "Cleaning", "current", "Cleaning.dat")

SILVER_BASE = os.path.join(BASE_DIR, "SilverRawData", "Cleaning")
SILVER_CURRENT = os.path.join(SILVER_BASE, "current")
SILVER_HISTORY = os.path.join(SILVER_BASE, datetime.now().strftime("%Y/%m/%d"))

FILENAME_OUT = "Silver_Cleaning.dat"
OUTPUT_FILE = os.path.join(SILVER_CURRENT, FILENAME_OUT)
OUTPUT_FILE_HIST = os.path.join(SILVER_HISTORY, FILENAME_OUT)

PROCESSED_LINES_TRACKER = os.path.join(BASE_DIR, "BronzeRawData", "Cleaning", "current", "cleaned_lines.txt")

# === HELPERS ===
def hash_line(line):
    return hashlib.md5(line.encode("utf-8")).hexdigest()

def clean_value(val):
    if not isinstance(val, str):
        val = str(val)
    return val.replace("\n", "").replace("\r", "").replace('"', "").replace("'", "").strip()

def parse_two_values(val, col_name):
    val = clean_value(val)
    if val == "":
        return None, None
    parts = val.split(";")
    if len(parts) != 2:
        print(f"Warning: Invalid format in column '{col_name}' (expected 'X;Y') -> {val}")
        return None, None
    try:
        part1 = int(float(parts[0].strip()))
        part2 = int(float(parts[1].strip()))
        return part1, part2
    except Exception:
        print(f"Warning: Invalid number(s) in column '{col_name}' -> {val}")
        return None, None

def validate_int(val, min_val, max_val, col_name):
    try:
        val = clean_value(val)
        val = int(float(val))
        if min_val <= val <= max_val:
            return val
        print(f"Warning: {col_name} out of range ({val})")
    except:
        if str(val).strip() != "":
            print(f"Warning: Invalid value in {col_name} -> {val}")
    return None

def validate_binary(val, col_name):
    return validate_int(val, 0, 1, col_name)

def validate_number(val, col_name):
    try:
        val = clean_value(val)
        return int(float(val))
    except:
        if val.strip() != "":
            print(f"Warning: Invalid number in {col_name} -> {val}")
        return None

def format_date(val, col_name):
    val = clean_value(val)
    try:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            return datetime.strptime(val, "%m/%d/%Y %H:%M:%S")
        except:
            if val.strip() != "":
                print(f"Warning: Invalid datetime in {col_name} -> {val}")
            return None

def is_valid_machine_id(val):
    try:
        val = clean_value(val)
        return 0 <= int(val) <= 32767
    except:
        return False

# === MAIN PROCESS ===
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    processed_hashes = set()
    if os.path.exists(PROCESSED_LINES_TRACKER):
        with open(PROCESSED_LINES_TRACKER, "r") as f:
            processed_hashes = set(f.read().splitlines())

    cleaned_rows = []
    new_hashes = []

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        for row in reader:
            if not is_valid_machine_id(row.get("machine_id", "")):
                continue

            line = ";".join([row[h] for h in reader.fieldnames if h in row])
            line_hash = hash_line(line)
            if line_hash in processed_hashes:
                continue

            ts = format_date(row["timestamp_start"], "timestamp_start")
            te = format_date(row["timestamp_end"], "timestamp_end")

            cleaned = {
                "machine_id": clean_value(row["machine_id"]),
                "timestamp_start": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                "timestamp_end": te.strftime("%Y-%m-%d %H:%M:%S") if te else "",
                "powder_clean_status": validate_int(row["powder_clean_status"], 0, 4, "powder_clean_status"),
                "tabs_status_left": validate_int(row["tabs_status_left"], 0, 7, "tabs_status_left"),
                "tabs_status_right": validate_int(row["tabs_status_right"], 0, 7, "tabs_status_right"),
                "detergent_status_left": validate_int(row["detergent_status_left"], 0, 9, "detergent_status_left"),
                "detergent_status_right": validate_int(row["detergent_status_right"], 0, 9, "detergent_status_right"),
                "milk_pump_error_left": validate_binary(row["milk_pump_error_left"], "milk_pump_error_left"),
                "milk_pump_error_right": validate_binary(row["milk_pump_error_right"], "milk_pump_error_right"),
                "milk_temp_left_1": validate_number(row["milk_temp_left_1"], "milk_temp_left_1"),
                "milk_temp_left_2": validate_number(row["milk_temp_left_2"], "milk_temp_left_2"),
                "milk_temp_right_1": validate_number(row["milk_temp_right_1"], "milk_temp_right_1"),
                "milk_temp_right_2": validate_number(row["milk_temp_right_2"], "milk_temp_right_2"),
                "milk_rpm_left_1": validate_number(row["milk_rpm_left_1"], "milk_rpm_left_1"),
                "milk_rpm_left_2": validate_number(row["milk_rpm_left_2"], "milk_rpm_left_2"),
                "milk_rpm_right_1": validate_number(row["milk_rpm_right_1"], "milk_rpm_right_1"),
                "milk_rpm_right_2": validate_number(row["milk_rpm_right_2"], "milk_rpm_right_2"),
            }

            for col in [
                "milk_clean_temp_left", "milk_clean_temp_right",
                "milk_clean_rpm_left", "milk_clean_rpm_right",
                "milk_seq_cycle_left", "milk_seq_cycle_right"
            ]:
                v1, v2 = parse_two_values(row[col], col)
                cleaned[f"{col}_1"] = v1
                cleaned[f"{col}_2"] = v2

            cleaned_rows.append(cleaned)
            new_hashes.append(line_hash)

    if cleaned_rows:
        df = pd.DataFrame(cleaned_rows)

        os.makedirs(SILVER_CURRENT, exist_ok=True)
        os.makedirs(SILVER_HISTORY, exist_ok=True)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else str(int(x)) if isinstance(x, (int, float)) and float(x).is_integer() else str(x))

        if os.path.exists(OUTPUT_FILE):
            df_existing = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str)
            df = pd.concat([df_existing, df], ignore_index=True)
        df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
        copy2(OUTPUT_FILE, OUTPUT_FILE_HIST)

        print(f"Cleaned data written to {OUTPUT_FILE}")
        print(f"Copied to history folder: {OUTPUT_FILE_HIST}")
    else:
        print("No new data to clean.")

    if new_hashes:
        with open(PROCESSED_LINES_TRACKER, "a") as f:
            for h in new_hashes:
                f.write(h + "\n")

if __name__ == "__main__":
    main()
```

### 4. Silver Data Processor - Info Data

This script processes the "Bronze" data into the "Silver" format for the Info logs.

**File: `process_silver_info.py`**

```python
import os
import pandas as pd
from datetime import datetime
import hashlib
from shutil import copy2

# === CONFIG ===
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
INPUT_FILE = os.path.join(BASE_DIR, "BronzeRawData", "Info", "current", "Info.dat")

SILVER_BASE = os.path.join(BASE_DIR, "SilverRawData", "Info")
SILVER_CURRENT = os.path.join(SILVER_BASE, "current")
SILVER_HISTORY = os.path.join(SILVER_BASE, datetime.now().strftime("%Y/%m/%d"))

FILENAME_OUT = "Silver_Info.dat"
OUTPUT_FILE = os.path.join(SILVER_CURRENT, FILENAME_OUT)
OUTPUT_FILE_HIST = os.path.join(SILVER_HISTORY, FILENAME_OUT)

PROCESSED_LINES_TRACKER = os.path.join(BASE_DIR, "BronzeRawData", "Info", "current", "cleaned_lines.txt")

# === HELPERS ===
def hash_line(line):
    return hashlib.md5(line.encode("utf-8")).hexdigest()

def clean_value(val):
    if not isinstance(val, str):
        val = str(val)
    return val.replace("\n", "").replace("\r", "").replace('"', "").replace("'", "").strip()

def validate_smallint(val, col_name):
    try:
        val = int(clean_value(val))
        if -32768 <= val <= 32767:
            return val
        print(f"Warning: {col_name} out of smallint range -> {val}")
    except:
        if str(val).strip():
            print(f"Warning: Invalid smallint in {col_name} -> {val}")
    return None

def validate_int(val, col_name):
    try:
        return int(clean_value(val))
    except:
        if str(val).strip():
            print(f"Warning: Invalid int in {col_name} -> {val}")
        return None

def format_date(val, col_name):
    val = clean_value(val)
    try:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            return datetime.strptime(val, "%m/%d/%Y %H:%M:%S")
        except:
            if val.strip():
                print(f"Warning: Invalid datetime in {col_name} -> {val}")
            return None

# === MAIN PROCESS ===
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    processed_hashes = set()
    if os.path.exists(PROCESSED_LINES_TRACKER):
        with open(PROCESSED_LINES_TRACKER, "r") as f:
            processed_hashes = set(f.read().splitlines())

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]

    if not raw_lines:
        print("Input file is empty or only contains blank lines.")
        return

    header = raw_lines[0].split(";")
    data_lines = raw_lines[1:]

    cleaned_rows = []
    new_hashes = []

    for line in data_lines:
        line_hash = hash_line(line)
        if line_hash in processed_hashes:
            continue

        parts = line.split(";")
        if len(parts) < len(header):
            print(f"Warning: Line skipped due to missing fields -> {line}")
            continue

        row = dict(zip(header, parts))

        machine_id = validate_smallint(row.get("machine_id", ""), "machine_id")
        if machine_id is None:
            continue  # Skip if invalid machine_id

        ts = format_date(row["timestamp"], "timestamp")

        cleaned = {
            "machine_id": machine_id,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
            "number": clean_value(row["number"]),
            "typography": clean_value(row["typography"]),
            "type_number": validate_int(row["type_number"], "type_number")
        }

        cleaned_rows.append(cleaned)
        new_hashes.append(line_hash)

    if cleaned_rows:
        df = pd.DataFrame(cleaned_rows)

        os.makedirs(SILVER_CURRENT, exist_ok=True)
        os.makedirs(SILVER_HISTORY, exist_ok=True)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else str(x))

        if os.path.exists(OUTPUT_FILE):
            df_existing = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str)
            df = pd.concat([df_existing, df], ignore_index=True)

        df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
        copy2(OUTPUT_FILE, OUTPUT_FILE_HIST)

        print(f"Cleaned data written to {OUTPUT_FILE}")
        print(f"Copied to history folder: {OUTPUT_FILE_HIST}")
    else:
        print("No new data to clean.")

    if new_hashes:
        with open(PROCESSED_LINES_TRACKER, "a") as f:
            for h in new_hashes:
                f.write(h + "\n")

if __name__ == "__main__":
    main()
```

### 5. Silver Data Processor - Product Data

This script processes the "Bronze" data into the "Silver" format for the Product logs.

**File: `process_silver_product.py`**

```python
import os
import pandas as pd
from datetime import datetime
import hashlib
from shutil import copy2

# === CONFIG ===
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
INPUT_FILE = os.path.join(BASE_DIR, "BronzeRawData", "Product", "current", "Product.dat")

SILVER_BASE = os.path.join(BASE_DIR, "SilverRawData", "Product")
SILVER_CURRENT = os.path.join(SILVER_BASE, "current")
SILVER_HISTORY = os.path.join(SILVER_BASE, datetime.now().strftime("%Y/%m/%d"))

FILENAME_OUT = "Silver_Product.dat"
OUTPUT_FILE = os.path.join(SILVER_CURRENT, FILENAME_OUT)
OUTPUT_FILE_HIST = os.path.join(SILVER_HISTORY, FILENAME_OUT)

PROCESSED_LINES_TRACKER = os.path.join(BASE_DIR, "BronzeRawData", "Product", "current", "cleaned_lines.txt")

# === HELPERS ===
def hash_line(line):
    return hashlib.md5(line.encode("utf-8")).hexdigest()

def clean_value(val):
    if not isinstance(val, str):
        val = str(val)
    return val.replace("\n", "").replace("\r", "").replace('"', "").strip()

def validate_float(val, col_name):
    try:
        val = clean_value(val)
        return round(float(val), 2)
    except:
        if val.strip() != "":
            print(f"Warning: Invalid float in {col_name} -> {val}")
        return None

def validate_int(val, col_name):
    try:
        val = clean_value(val).lower()
        if val == "true":
            return 1
        if val == "false":
            return 0
        val = int(float(val))
        return val
    except:
        if val.strip() != "":
            print(f"Warning: Invalid int in {col_name} -> {val}")
        return None

def format_date(val, col_name):
    val = clean_value(val)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(val, fmt)
        except:
            continue
    if val.strip() != "":
        print(f"Warning: Invalid datetime in {col_name} -> {val}")
    return None

# === MAIN PROCESS ===
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    processed_hashes = set()
    if os.path.exists(PROCESSED_LINES_TRACKER):
        with open(PROCESSED_LINES_TRACKER, "r") as f:
            processed_hashes = set(f.read().splitlines())

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]

    if not raw_lines:
        print("Input file is empty or only contains blank lines.")
        return

    header = raw_lines[0].split(";")
    data_lines = raw_lines[1:]

    cleaned_rows = []
    new_hashes = []

    for line in data_lines:
        line_hash = hash_line(line)
        if line_hash in processed_hashes:
            continue

        parts = line.split(";")
        if len(parts) < len(header):
            print(f"Warning: Line skipped due to missing fields -> {line}")
            continue

        row = dict(zip(header, parts))

        machine_id = validate_int(row["machine_id"], "machine_id")
        if machine_id is None or not (0 <= machine_id <= 32767):
            print(f"Skipping line due to invalid machine_id -> {row['machine_id']}")
            continue

        ts = format_date(row["timestamp"], "timestamp")

        cleaned = {
            "machine_id": machine_id,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
            "press_before": validate_float(row["press_before"], "press_before"),
            "press_after": validate_float(row["press_after"], "press_after"),
            "press_final": validate_float(row["press_final"], "press_final"),
            "grind_time": validate_float(row["grind_time"], "grind_time"),
            "ext_time": validate_float(row["ext_time"], "ext_time"),
            "water_qnty": validate_int(row["water_qnty"], "water_qnty"),
            "water_temp": validate_int(row["water_temp"], "water_temp"),
            "prod_type": validate_int(row["prod_type"], "prod_type"),
            "double_prod": validate_int(row["double_prod"], "double_prod"),
            "bean_hopper": validate_int(row["bean_hopper"], "bean_hopper"),
            "outlet_side": validate_int(row["outlet_side"], "outlet_side"),
            "stopped": validate_int(row["stopped"], "stopped"),
            "milk_temp": validate_int(row["milk_temp"], "milk_temp"),
            "steam_pressure": validate_float(row["steam_pressure"], "steam_pressure"),
            "grind_adjust_left": validate_int(row["grind_adjust_left"], "grind_adjust_left"),
            "grind_adjust_right": validate_int(row["grind_adjust_right"], "grind_adjust_right"),
            "milk_time": validate_float(row["milk_time"], "milk_time"),
            "boiler_temp": validate_int(row["boiler_temp"], "boiler_temp"),
        }

        cleaned_rows.append(cleaned)
        new_hashes.append(line_hash)

    if cleaned_rows:
        df = pd.DataFrame(cleaned_rows)
        os.makedirs(SILVER_CURRENT, exist_ok=True)
        os.makedirs(SILVER_HISTORY, exist_ok=True)

        if os.path.exists(OUTPUT_FILE):
            df_existing = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str)
            df = pd.concat([df_existing, df], ignore_index=True)

        df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
        copy2(OUTPUT_FILE, OUTPUT_FILE_HIST)

        print(f"Cleaned data written to {OUTPUT_FILE}")
        print(f"Copied to history folder: {OUTPUT_FILE_HIST}")
    else:
        print("No new data to clean.")

    if new_hashes:
        with open(PROCESSED_LINES_TRACKER, "a") as f:
            for h in new_hashes:
                f.write(h + "\n")

if __name__ == "__main__":
    main()
```

### 6. Silver Data Processor - Rinse Data

This script processes the "Bronze" data into the "Silver" format for the Rinse logs.

**File: `process_silver_rinse.py`**

```python
import os
import pandas as pd
from datetime import datetime
import hashlib
from shutil import copy2

# === CONFIG ===
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))
INPUT_FILE = os.path.join(BASE_DIR, "BronzeRawData", "Rinse", "current", "Rinse.dat")

SILVER_BASE = os.path.join(BASE_DIR, "SilverRawData", "Rinse")
SILVER_CURRENT = os.path.join(SILVER_BASE, "current")
SILVER_HISTORY = os.path.join(SILVER_BASE, datetime.now().strftime("%Y/%m/%d"))

FILENAME_OUT = "Silver_Rinse.dat"
OUTPUT_FILE = os.path.join(SILVER_CURRENT, FILENAME_OUT)
OUTPUT_FILE_HIST = os.path.join(SILVER_HISTORY, FILENAME_OUT)

PROCESSED_LINES_TRACKER = os.path.join(BASE_DIR, "BronzeRawData", "Rinse", "current", "cleaned_lines.txt")

# === HELPERS ===
def hash_line(line):
    return hashlib.md5(line.encode("utf-8")).hexdigest()

def clean_value(val):
    if not isinstance(val, str):
        val = str(val)
    return val.replace("\n", "").replace("\r", "").replace('"', "").replace("'", "").strip()

def validate_int(val, min_val, max_val, col_name):
    try:
        val = clean_value(val)
        val = int(float(val))
        if min_val <= val <= max_val:
            return val
        print(f"Warning: {col_name} out of range ({val})")
    except:
        if str(val).strip() != "":
            print(f"Warning: Invalid value in {col_name} -> {val}")
    return None

def validate_machine_id(val):
    try:
        v = int(val)
        if 0 <= v <= 32767:
            return v
        else:
            print(f"Skipping line due to invalid machine_id: {val}")
    except:
        print(f"Skipping line due to invalid machine_id: {val}")
    return None

def validate_nullable(val, null_marker, col_name):
    try:
        val = clean_value(val)
        val = int(float(val))
        return None if val == null_marker else val
    except:
        if str(val).strip() != "":
            print(f"Warning: Invalid value in {col_name} -> {val}")
        return None

def format_date(val, col_name):
    val = clean_value(val)
    try:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            return datetime.strptime(val, "%m/%d/%Y %H:%M:%S")
        except:
            if val.strip() != "":
                print(f"Warning: Invalid datetime in {col_name} -> {val}")
            return None

# === MAIN PROCESS ===
def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Input file not found: {INPUT_FILE}")
        return

    processed_hashes = set()
    if os.path.exists(PROCESSED_LINES_TRACKER):
        with open(PROCESSED_LINES_TRACKER, "r") as f:
            processed_hashes = set(f.read().splitlines())

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]

    if not raw_lines:
        print("Input file is empty or only contains blank lines.")
        return

    header = raw_lines[0].split(";")
    data_lines = raw_lines[1:]

    cleaned_rows = []
    new_hashes = []

    for line in data_lines:
        line_hash = hash_line(line)
        if line_hash in processed_hashes:
            continue

        parts = line.split(";")
        if len(parts) < len(header):
            print(f"Warning: Line skipped due to missing fields -> {line}")
            continue

        row = dict(zip(header, parts))

        machine_id = validate_machine_id(row["machine_id"])
        if machine_id is None:
            continue

        ts = format_date(row["timestamp"], "timestamp")

        cleaned = {
            "machine_id": machine_id,
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "",
            "rinse_type": validate_int(row["rinse_type"], 0, 255, "rinse_type"),
            "flow_rate_left": validate_nullable(row["flow_rate_left"], 65535, "flow_rate_left"),
            "flow_rate_right": validate_nullable(row["flow_rate_right"], 65535, "flow_rate_right"),
            "status_left": validate_int(row["status_left"], 0, 6, "status_left"),
            "status_right": validate_int(row["status_right"], 0, 6, "status_right"),
            "pump_pressure": validate_int(row["pump_pressure"], 0, 1000, "pump_pressure"),
            "nozzle_flow_rate_left": validate_nullable(row["nozzle_flow_rate_left"], 65535, "nozzle_flow_rate_left"),
            "nozzle_flow_rate_right": validate_nullable(row["nozzle_flow_rate_right"], 65535, "nozzle_flow_rate_right"),
            "nozzle_status_left": validate_int(row["nozzle_status_left"], 0, 255, "nozzle_status_left"),
            "nozzle_status_right": validate_int(row["nozzle_status_right"], 0, 255, "nozzle_status_right")
        }

        cleaned_rows.append(cleaned)
        new_hashes.append(line_hash)

    if cleaned_rows:
        df = pd.DataFrame(cleaned_rows)

        os.makedirs(SILVER_CURRENT, exist_ok=True)
        os.makedirs(SILVER_HISTORY, exist_ok=True)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else str(x))

        if os.path.exists(OUTPUT_FILE):
            df_existing = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str)
            df = pd.concat([df_existing, df], ignore_index=True)

        df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
        copy2(OUTPUT_FILE, OUTPUT_FILE_HIST)

        print(f"Cleaned data written to {OUTPUT_FILE}")
        print(f"Copied to history folder: {OUTPUT_FILE_HIST}")
    else:
        print("No new data to clean.")

    if new_hashes:
        with open(PROCESSED_LINES_TRACKER, "a") as f:
            for h in new_hashes:
                f.write(h + "\n")

if __name__ == "__main__":
    main()
```

## GitHub Actions Workflows

The ETL process is automated through GitHub Actions workflows that run on a self-hosted runner on the VM.

### ETL Process Workflow

This workflow runs every 30 minutes to download and process the data files:

**File: `.github/workflows/execute-eversys-scripts.yml`**

```yaml
name: Execute Eversys Scripts
on:
  schedule:
    - cron: "*/30 * * * *"  # Runs every 30 minutes
  workflow_dispatch:
jobs:
  run-eversys-scripts:
    runs-on: [self-hosted]
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4
      - name: Run Download Script
        run: python downloadeversysfiles.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Extract Script
        run: python extracteversysdata.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Clean Script
        run: python silvercleaningscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Info Script
        run: python silverinfoscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Rinse Script
        run: python silverrinsescript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Product Script
        run: python silverproductscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
```

### Deployment Workflow

This workflow updates the code on the VM whenever changes are pushed to the main branch:

**File: `.github/workflows/deploy-on-vm.yml`**

```yaml
name: Deploy on VM
on:
  push:
    branches: [ "main" ]
jobs:
  deploy:
    runs-on: self-hosted
    steps:
      - name: Check out repository
        uses: actions/checkout@v3
      - name: Pull latest changes from GitHub
        run: |
          cd C:/Users/Administrator/DataCycleProject/DataCycleProject_Grp10 
          git pull origin main
```

## End-to-End Process Flow

The complete ETL process flows as follows:

1. **GitHub Actions workflow** triggers based on a schedule or manual execution
2. **Download script** retrieves raw DAT files from the SMB server
3. **Bronze processing script** consolidates raw files into the Bronze format
4. **Silver processing scripts** clean and validate the data into the Silver format
5. **SQL Server stored procedures** are executed to import the data into the database
6. **SQL Server Agent jobs** also run on their schedule to perform the same import processes

## Monitoring and Troubleshooting

### Log Files

Each Python script generates console output that can be captured and reviewed in the GitHub Actions workflow logs. Additionally, the SQL Server Agent job history provides information about the execution of the import procedures.

### Common Issues

1. **SMB Connection Failures**:
   - Check network connectivity to the SMB server
   - Verify credentials are correct
   - Ensure proper permissions are set on the shared folder

2. **Data Processing Errors**:
   - Review the console output for warnings about invalid data
   - Check the processed_files.txt and cleaned_lines.txt files to see what has been processed
   - Examine the raw data files for format issues

3. **Database Import Failures**:
   - Check SQL Server error logs
   - Review SQL Server Agent job history
   - Verify that the SQL Server service account has proper permissions
