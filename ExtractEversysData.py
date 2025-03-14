import os
import pandas as pd
from openpyxl import load_workbook
from io import StringIO
from shutil import copy2
from datetime import datetime

# Configuration
BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))  # Move one level up

DAT_FILES_FOLDER = os.path.join(BASE_DIR, "EversysDatFiles")  # Folder containing DAT files
BRONZE_FOLDER = os.path.join(BASE_DIR, "BronzeRawData")  # Main storage folder

OUTPUT_EXCEL_FILE = os.path.join(BRONZE_FOLDER, "Eversys_data.xlsx")  # Main Excel file
PROCESSED_FILES_TRACKER = os.path.join(BRONZE_FOLDER, "processed_files.txt")  # Processed files tracker

EXCEL_MAX_ROWS = 1_048_576
BATCH_SIZE = 100  # Process files in batches of 100

# Ensure directories exist
os.makedirs(BRONZE_FOLDER, exist_ok=True)

# Track processed files
def load_processed_files():
    if os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_processed_file(filename):
    with open(PROCESSED_FILES_TRACKER, "a") as f:
        f.write(f"{filename}\n")

# List DAT files in local folder
def list_local_files():
    return [f for f in os.listdir(DAT_FILES_FOLDER) if f.endswith(".dat")]

# Process DAT file (No Filtering)
def process_file(file_path, filename):
    file_mapping = {
        "Cleaning_History": "Cleaning",
        "Rinse_History": "Rinse",
        "Info_Message_History": "Info msg",
        "Product_History": "Product"
    }

    file_type = filename.split("-")[-1].replace(".dat", "").replace(".DAT", "")
    sheet_name = file_mapping.get(file_type, file_type)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        df = pd.read_csv(StringIO(content), delimiter=';', dtype=str)
        df["file_type"] = sheet_name  # Add a column for file type

        return df if not df.empty else None, sheet_name
    except Exception as e:
        print(f"Error processing file {filename}: {e}")
        return None, None

# Append batch to existing Excel file
def append_to_excel(file_name, data_frames):
    """Append data to an existing Excel file while handling Excel row limits."""
    
    if all(len(df) == 0 for df_list in data_frames.values() for df in df_list):
        print("No valid data to save. Skipping Excel write.")
        return

    mode = 'a' if os.path.exists(file_name) else 'w'
    writer_args = {"engine": "openpyxl", "mode": mode}

    if mode == 'a':
        writer_args["if_sheet_exists"] = "replace"

    with pd.ExcelWriter(file_name, **writer_args) as writer:
        for sheet_name, dfs in data_frames.items():
            if not dfs:
                continue

            new_data = pd.concat(dfs, ignore_index=True)

            try:
                # Read existing sheet data if appending
                existing_data = pd.read_excel(file_name, sheet_name=sheet_name, dtype=str)
                combined_df = pd.concat([existing_data, new_data], ignore_index=True)
            except Exception:
                # Create a new sheet if it does not exist
                combined_df = new_data  

            sheet_count = 1
            while len(combined_df) > 0:
                sheet_chunk = combined_df.iloc[:EXCEL_MAX_ROWS]
                combined_df = combined_df.iloc[EXCEL_MAX_ROWS:]

                sheet_name_chunk = f"{sheet_name}_{sheet_count}" if sheet_count > 1 else sheet_name
                sheet_chunk.to_excel(writer, sheet_name=sheet_name_chunk, index=False)

                sheet_count += 1

    print(f"Data successfully appended to {file_name}")

# Copy the final Excel file and processed tracker to the history folder
def save_files_to_history():
    """Copies the processed Excel file and processed files tracker into a structured year/month/day history folder."""
    now = datetime.now()
    history_folder = os.path.join(BRONZE_FOLDER, str(now.year), f"{now.month:02d}", f"{now.day:02d}")
    
    # Ensure directory exists
    os.makedirs(history_folder, exist_ok=True)
    
    history_excel = os.path.join(history_folder, "Eversys_data.xlsx")
    history_tracker = os.path.join(history_folder, "processed_files.txt")

    # Copy files (overwrite if they exist)
    copy2(OUTPUT_EXCEL_FILE, history_excel)
    copy2(PROCESSED_FILES_TRACKER, history_tracker)

    print(f"Excel file copied to history: {history_excel}")
    print(f"Processed files tracker copied to history: {history_tracker}")

# Main process
def main():
    processed_files = load_processed_files()
    all_files = list_local_files()
    new_files = [f for f in all_files if f not in processed_files]

    if not new_files:
        print("No new files to process.")
        return

    print(f"Found {len(new_files)} new DAT files to process.")

    batch_count = 0

    while batch_count * BATCH_SIZE < len(new_files):
        batch_files = new_files[batch_count * BATCH_SIZE:(batch_count + 1) * BATCH_SIZE]
        print(f"Processing batch {batch_count + 1}: {len(batch_files)} files")

        data_frames = {name: [] for name in ["Cleaning", "Rinse", "Info msg", "Product"]}
        successful_files = []

        for filename in batch_files:
            file_path = os.path.join(DAT_FILES_FOLDER, filename)
            df, sheet_name = process_file(file_path, filename)
            if df is None:
                continue

            data_frames[sheet_name].append(df)
            successful_files.append(filename)

        append_to_excel(OUTPUT_EXCEL_FILE, data_frames)
        print(f"Batch {batch_count + 1} appended to {OUTPUT_EXCEL_FILE}")

        for filename in batch_files:
            save_processed_file(filename)

        batch_count += 1

    save_files_to_history()
    print("Processing complete.")

if __name__ == "__main__":
    print("Starting new execution cycle...")
    main()
