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
