# Technical Documentation - Group 10

## Project Overview

This project implements a multi-stage data processing pipeline for coffee machine operational data. The pipeline follows the medallion architecture pattern with:

1. **Bronze Layer**: Raw data ingestion and initial consolidation from .dat files
2. **Silver Layer**: Data validation, cleaning, and standardization
3. **Gold Layer**: OLAP destination

The system processes various types of machine operational data including cleaning cycles, rinse operations, product usage, and information messages.

## Architecture

```
Project Directory Structure
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

## Data Flow

1. Raw `.dat` files are collected in `EversysDatFiles/` directory
2. Bronze processor script consolidates files by category (Cleaning, Rinse, Info, Product)
3. Silver layer scripts validate, clean, and standardize each data category
4. Data is stored in both current and historical (date-based) directories
5. Processed data is tracked to prevent reprocessing

## Processing Components

### Bronze Layer (bronze_processor.py)

This script provides the initial data ingestion and consolidation:

- Reads raw `.dat` files from the source directory
- Categorizes files by type (Cleaning, Rinse, Info, Product)
- Consolidates data into category-specific files
- Maintains a history of processed files
- Creates backup copies in date-organized folders

Key functions:
- `process_files()`: Processes and merges new .dat files
- `ensure_current_files()`: Ensures that required output files exist
- `backup_file()`: Creates historical copies of processed files

### Silver Layer

The silver layer consists of three specialized scripts for different data categories:

#### 1. silver_cleaning.py

Processes cleaning cycle data with the following validations:
- Machine ID validation
- Timestamp standardization
- Status code validation for various components
- Temperature and RPM validation
- Two-value field parsing

#### 2. silver_info.py 

Processes information messages with validations for:
- Machine ID validation (smallint range)
- Timestamp standardization
- Message type number validation

#### 3. silver_rinse.py

Processes rinsing operation data with validations for:
- Machine ID validation
- Timestamp standardization
- Flow rate validation
- Status code validation
- Pressure validation

### Common Features Across Silver Layer Scripts

All silver layer scripts share common functionality:
- Line-level hash tracking to prevent duplicate processing
- Data type validation and standardization
- Error logging for invalid values
- Historical data preservation
- Appending to existing files with proper header handling

## Processing Safeguards

The pipeline implements several safeguards:

1. **Idempotent Processing**: Each file and line is processed exactly once
2. **Hash-based Tracking**: MD5 hashes are used to track processed data
3. **Temporary Files**: Processing uses temp files before committing changes
4. **Historical Preservation**: All stages maintain dated historical copies
5. **Error Handling**: Validation failures are logged but don't stop processing
6. **Directory Creation**: Automatic creation of required directories

## Data Validation Rules

The system applies specific validation rules for each data type:

### Cleaning Data Validation
- Machine ID: 0-32767
- Powder status: 0-4
- Tabs status: 0-7
- Detergent status: 0-9
- Milk pump error: 0-1 (binary)
- Temperature and RPM: Numeric validation

### Info Data Validation
- Machine ID: -32768 to 32767 (smallint range)
- Timestamp: Standard date format
- Type number: Integer validation

### Rinse Data Validation
- Machine ID: 0-32767
- Rinse type: 0-255
- Flow rates: Nullable with 65535 as null marker
- Status values: 0-6
- Pump pressure: 0-1000
- Nozzle status: 0-255

## Running the Pipeline

The scripts appear to be designed to run in sequence:

1. Run `bronze_processor.py` to consolidate raw data files
2. Run each silver processing script to clean respective data categories

These may be executed on a schedule via GitHub Actions or other CI/CD systems.

## Database Integration

The processed data is ultimately loaded into:
1. A relational database (for operational queries)
2. An OLAP database (for analytical workloads)

The loading procedures for these destinations are not included in the provided scripts.

## Best Practices and Notes

1. **Incremental Processing**: The system only processes new files/lines, making it efficient for frequent runs
2. **Data Lineage**: Historical copies at all processing stages enable data tracing
3. **Data Quality**: Extensive validation ensures data integrity
4. **Idempotence**: Safe to re-run without causing duplication
5. **Error Handling**: Validation failures are logged but don't halt overall processing


--- TODO
+ add database tables and relationships
+ add knime workflow
+ mention database backups
+ mention mssql jobs and stocked procedures
