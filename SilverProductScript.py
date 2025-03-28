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
        val = clean_value(val)
        return int(float(val))
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
        ts = format_date(row["timestamp"], "timestamp")

        cleaned = {
            "machine_id": validate_int(row["machine_id"], "machine_id"),
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

        # Load previous file if it exists
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
