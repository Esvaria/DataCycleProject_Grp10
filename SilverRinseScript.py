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

        ts = format_date(row["timestamp"], "timestamp")

        cleaned = {
            "machine_id": clean_value(row["machine_id"]),
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
