import os
import pandas as pd
from openpyxl import load_workbook
from io import StringIO

# Configuration
DAT_FILES_FOLDER = "EversysDatFiles"  # Local folder with downloaded DAT files
LOCAL_CSV = "known_machines.csv"
OUTPUT_EXCEL_FILE = "Eversys_data.xlsx"
PROCESSED_FILES_TRACKER = "processed_files.txt"
EXCEL_MAX_ROWS = 1_048_576
BATCH_SIZE = 100  # Process files in batches of 100

# Load known machines
def load_known_machines():
    return pd.read_csv(LOCAL_CSV, dtype=str)

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

# Append data to Excel file (handles max row limits)
def append_to_excel(file_name, data_frames):
    """Append data to an existing Excel file while handling Excel row limits."""
    
    if all(len(df) == 0 for df_list in data_frames.values() for df in df_list):
        print("No valid data to save. Skipping Excel write.")
        return

    mode = 'a' if os.path.exists(file_name) else 'w'
    
    writer_args = {"engine": "openpyxl", "mode": mode}
    
    # Use "if_sheet_exists" only when appending to an existing file
    if mode == 'a':
        writer_args["if_sheet_exists"] = "overlay"

    with pd.ExcelWriter(file_name, **writer_args) as writer:
        for sheet_name, dfs in data_frames.items():
            if not dfs:
                continue
            
            new_data = pd.concat(dfs, ignore_index=True)
            try:
                existing_data = pd.read_excel(file_name, sheet_name=sheet_name, dtype=str)
                combined_df = pd.concat([existing_data, new_data], ignore_index=True)
            except Exception:
                combined_df = new_data  # Create a new sheet if missing

            sheet_count = 1
            while len(combined_df) > 0:
                sheet_chunk = combined_df.iloc[:EXCEL_MAX_ROWS]
                combined_df = combined_df.iloc[EXCEL_MAX_ROWS:]

                sheet_name_chunk = f"{sheet_name}_{sheet_count}" if sheet_count > 1 else sheet_name
                sheet_chunk.to_excel(writer, sheet_name=sheet_name_chunk, index=False)

                sheet_count += 1

    print(f"Data successfully appended to {file_name}")


# Main process
def main():
    known_machines = load_known_machines()
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

            df = df.merge(known_machines, on="machine_id", how="left", suffixes=("_original", "_known")) if "machine_id" in df.columns else df

            data_frames[sheet_name].append(df)
            successful_files.append(filename)

        append_to_excel(OUTPUT_EXCEL_FILE, data_frames)
        print(f"Batch {batch_count + 1} appended to {OUTPUT_EXCEL_FILE}")

        for filename in batch_files:
            save_processed_file(filename)

        batch_count += 1

    print("Processing complete.")

if __name__ == "__main__":
    print("Starting new execution cycle...")
    main()
