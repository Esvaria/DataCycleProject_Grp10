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