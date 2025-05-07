# Machine Data Import System Setup Guide

This document outlines the setup process for the machine data import system, which processes data files from coffee machines and imports them into a SQL Server database.

## Summary

This document provides a complete guide for setting up the machine data import system in a new environment. The system consists of:

1. **Database Components**:
   - SQL Server database (machine_data)
   - Tables for various log types (cleaning_logs, rinse_logs, product_logs, info_logs)
   - Stored procedures for data import
   - Debug procedures for troubleshooting

2. **File System Components**:
   - Directory structure for data files
   - Semicolon-delimited data files with specific formats

3. **Automation Components**:
   - SQL Server Agent jobs for scheduled execution
   - Master job that runs all import procedures in sequence
   - Automated database backup job
   - GitHub Actions workflows for the ETL process


To set up the system from scratch:

1. Create the database and tables
2. Create the directory structure
3. Enable xp_cmdshell
4. Create all stored procedures
5. Set up SQL Server Agent jobs
6. Populate the initial data files
7. Monitor job execution to ensure proper functioning

With this setup, the system will automatically import data from the specified files into the SQL Server database at scheduled intervals, making the data available for reporting and analysis.


## System Overview

The system consists of SQL Server stored procedures that import various types of machine data logs:
- Cleaning logs
- Rinse logs
- Product logs 
- Info logs

Each procedure imports data from a specific file in the file system into its corresponding table in the `machine_data` database.

## Prerequisites

- SQL Server 2019 or newer
- SQL Server Agent for scheduled jobs
- `xp_cmdshell` extended stored procedure enabled
- Appropriate file system permissions

## Database Setup

### 1. Create the Database

```sql
-- Create the database
CREATE DATABASE [machine_data]
GO

USE [machine_data]
GO
```

### 2. Create Tables

Create the required tables in the database:

```sql
-- Create machine_names table
CREATE TABLE [dbo].[machine_names] (
    [machine_id] INT NOT NULL PRIMARY KEY,
    [name] NVARCHAR(100) NOT NULL
);

-- Create cleaning_logs table
CREATE TABLE [dbo].[cleaning_logs] (
    [log_id] INT IDENTITY(1,1) PRIMARY KEY,
    [machine_id] INT NOT NULL,
    [timestamp_start] DATETIME NOT NULL,
    [timestamp_end] DATETIME NOT NULL,
    [powder_clean_status] SMALLINT NULL,
    [tabs_status_left] SMALLINT NULL,
    [tabs_status_right] SMALLINT NULL,
    [detergent_status_left] SMALLINT NULL,
    [detergent_status_right] SMALLINT NULL,
    [milk_pump_error_left] TINYINT NULL,
    [milk_pump_error_right] TINYINT NULL,
    [milk_temp_left_1] SMALLINT NULL,
    [milk_temp_left_2] SMALLINT NULL,
    [milk_temp_right_1] SMALLINT NULL,
    [milk_temp_right_2] SMALLINT NULL,
    [milk_rpm_left_1] SMALLINT NULL,
    [milk_rpm_left_2] SMALLINT NULL,
    [milk_rpm_right_1] SMALLINT NULL,
    [milk_rpm_right_2] SMALLINT NULL,
    [milk_clean_temp_left_1] SMALLINT NULL,
    [milk_clean_temp_left_2] SMALLINT NULL,
    [milk_clean_temp_right_1] SMALLINT NULL,
    [milk_clean_temp_right_2] SMALLINT NULL,
    [milk_clean_rpm_left_1] SMALLINT NULL,
    [milk_clean_rpm_left_2] SMALLINT NULL,
    [milk_clean_rpm_right_1] SMALLINT NULL,
    [milk_clean_rpm_right_2] SMALLINT NULL,
    [milk_seq_cycle_left_1] SMALLINT NULL,
    [milk_seq_cycle_left_2] SMALLINT NULL,
    [milk_seq_cycle_right_1] SMALLINT NULL,
    [milk_seq_cycle_right_2] SMALLINT NULL,
    FOREIGN KEY ([machine_id]) REFERENCES [dbo].[machine_names]([machine_id])
);

-- Create rinse_logs table
CREATE TABLE [dbo].[rinse_logs] (
    [log_id] INT IDENTITY(1,1) PRIMARY KEY,
    [machine_id] INT NOT NULL,
    [timestamp] DATETIME NOT NULL,
    [rinse_type] INT NULL,
    [flow_rate_left] INT NULL,
    [flow_rate_right] INT NULL,
    [status_left] INT NULL,
    [status_right] INT NULL,
    [pump_pressure] INT NULL,
    [nozzle_flow_rate_left] INT NULL,
    [nozzle_flow_rate_right] INT NULL,
    [nozzle_status_left] INT NULL,
    [nozzle_status_right] INT NULL,
    FOREIGN KEY ([machine_id]) REFERENCES [dbo].[machine_names]([machine_id])
);

-- Create product_logs table
CREATE TABLE [dbo].[product_logs] (
    [log_id] INT IDENTITY(1,1) PRIMARY KEY,
    [machine_id] INT NOT NULL,
    [timestamp] DATETIME NOT NULL,
    [press_before] FLOAT NULL,
    [press_after] FLOAT NULL,
    [press_final] FLOAT NULL,
    [grind_time] FLOAT NULL,
    [ext_time] FLOAT NULL,
    [water_qnty] INT NULL,
    [water_temp] INT NULL,
    [prod_type] INT NULL,
    [double_prod] FLOAT NULL,
    [bean_hopper] INT NULL,
    [outlet_side] FLOAT NULL,
    [stopped] FLOAT NULL,
    [milk_temp] INT NULL,
    [steam_pressure] FLOAT NULL,
    [grind_adjust_left] INT NULL,
    [grind_adjust_right] INT NULL,
    [milk_time] FLOAT NULL,
    [boiler_temp] INT NULL,
    FOREIGN KEY ([machine_id]) REFERENCES [dbo].[machine_names]([machine_id])
);

-- Create info_logs table
CREATE TABLE [dbo].[info_logs] (
    [log_id] INT IDENTITY(1,1) PRIMARY KEY,
    [machine_id] INT NOT NULL,
    [timestamp] DATETIME NOT NULL,
    [number] NVARCHAR(50) NULL,
    [typography] NVARCHAR(10) NULL,
    [type_number] FLOAT NULL,
    FOREIGN KEY ([machine_id]) REFERENCES [dbo].[machine_names]([machine_id])
);
```

Ensure the SQL Server service account has read permissions on these directories.

## Enable xp_cmdshell

The stored procedures use `xp_cmdshell` to read the data files. Enable it with:

```sql
-- Enable advanced options
EXEC sp_configure 'show advanced options', 1;
RECONFIGURE;

-- Enable xp_cmdshell
EXEC sp_configure 'xp_cmdshell', 1;
RECONFIGURE;
```

## Create Stored Procedures

Refer to the [Stored Procedures Documentation](./DB-Stored-Procedures.md) for the complete scripts to create all required procedures:
- sp_ImportCleaningLogs
- sp_ImportRinseLogs
- sp_ImportProductLogs
- sp_ImportInfoLogs
- Debug procedures

## Set Up SQL Server Agent Jobs

Once the stored procedures are created, set up SQL Server Agent jobs to run them on a schedule.

Refer to the [SQL Server Agent Jobs Documentation](./DB-SQL-Server-Agent-Jobs.md) for detailed setup instructions for:
- Individual import jobs for each log type
- Master import job to run all procedures in sequence
- Database backup job

## Data File Specifications

The system expects data files in the following format:

### 1. Cleaning Logs (Silver_Cleaning.dat)
- Semicolon-delimited (`;`) text file
- Contains header row with column names
- Contains the following fields:
  - machine_id (INT)
  - timestamp_start (DATETIME)
  - timestamp_end (DATETIME)
  - powder_clean_status (SMALLINT)
  - tabs_status_left (SMALLINT)
  - tabs_status_right (SMALLINT)
  - detergent_status_left (SMALLINT)
  - detergent_status_right (SMALLINT)
  - milk_pump_error_left (TINYINT)
  - milk_pump_error_right (TINYINT)
  - milk_temp_left_1 (SMALLINT)
  - milk_temp_left_2 (SMALLINT)
  - milk_temp_right_1 (SMALLINT)
  - milk_temp_right_2 (SMALLINT)
  - milk_rpm_left_1 (SMALLINT)
  - milk_rpm_left_2 (SMALLINT)
  - milk_rpm_right_1 (SMALLINT)
  - milk_rpm_right_2 (SMALLINT)
  - milk_clean_temp_left_1 (SMALLINT)
  - milk_clean_temp_left_2 (SMALLINT)
  - milk_clean_temp_right_1 (SMALLINT)
  - milk_clean_temp_right_2 (SMALLINT)
  - milk_clean_rpm_left_1 (SMALLINT)
  - milk_clean_rpm_left_2 (SMALLINT)
  - milk_clean_rpm_right_1 (SMALLINT)
  - milk_clean_rpm_right_2 (SMALLINT)
  - milk_seq_cycle_left_1 (SMALLINT)
  - milk_seq_cycle_left_2 (SMALLINT)
  - milk_seq_cycle_right_1 (SMALLINT)
  - milk_seq_cycle_right_2 (SMALLINT)

### 2. Rinse Logs (Silver_Rinse.dat)
- Semicolon-delimited (`;`) text file
- Contains header row with column names
- Contains the following fields:
  - machine_id (INT)
  - timestamp (DATETIME)
  - rinse_type (INT)
  - flow_rate_left (INT)
  - flow_rate_right (INT)
  - status_left (INT)
  - status_right (INT)
  - pump_pressure (INT)
  - nozzle_flow_rate_left (INT)
  - nozzle_flow_rate_right (INT)
  - nozzle_status_left (INT)
  - nozzle_status_right (INT)

### 3. Product Logs (Silver_Product.dat)
- Semicolon-delimited (`;`) text file
- Contains header row with column names
- Contains the following fields:
  - machine_id (INT)
  - timestamp (DATETIME)
  - press_before (FLOAT)
  - press_after (FLOAT)
  - press_final (FLOAT)
  - grind_time (FLOAT)
  - ext_time (FLOAT)
  - water_qnty (INT)
  - water_temp (INT)
  - prod_type (INT)
  - double_prod (FLOAT)
  - bean_hopper (INT)
  - outlet_side (FLOAT)
  - stopped (FLOAT)
  - milk_temp (INT)
  - steam_pressure (FLOAT)
  - grind_adjust_left (INT)
  - grind_adjust_right (INT)
  - milk_time (FLOAT)
  - boiler_temp (INT)

### 4. Info Logs (Silver_Info.dat)
- Semicolon-delimited (`;`) text file
- Contains header row with column names
- Contains the following fields:
  - machine_id (INT)
  - timestamp (DATETIME)
  - number (NVARCHAR(50))
  - typography (NVARCHAR(10))
  - type_number (FLOAT)

## Automated ETL Process

For details on the automated ETL process that populates the data files, refer to the [ETL Process Documentation](./DB-ETL-Process.md) which describes:
- The Python scripts that download data from the SMB server
- The scripts that process the raw files into the Silver format
- The GitHub Actions workflows that automate these processes

## Troubleshooting

### Common Issues and Solutions

1. **Permission Errors**:
   - Ensure SQL Server service account has read permissions on the data file directories
   - Check logs for error messages related to file access

2. **xp_cmdshell Errors**:
   - Verify that xp_cmdshell is enabled
   - Check SQL Server configuration for security restrictions

3. **Parsing Errors**:
   - Use the debug stored procedures to check for data format issues
   - Verify that data files have the correct format and delimiters

4. **Missing Data**:
   - Check for unexpected NULL values in imported data
   - Verify that the machine_names table is being populated correctly

### Running Debug Procedures

To diagnose issues with data imports:

```sql
-- Check Info Logs import
EXEC [machine_data].[dbo].[sp_ImportInfoLogs_Debug];

-- Check Rinse Logs import
EXEC [machine_data].[dbo].[sp_ImportRinseLogs_Debug];
```

## Monitoring and Backup

### Monitoring Job Execution

Monitor SQL Server Agent jobs to ensure they run successfully:

```sql
-- Check job history
SELECT 
    j.name AS 'Job Name',
    h.run_date,
    h.run_time,
    h.run_status,
    h.run_duration,
    h.message
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
WHERE j.name LIKE '%Import%'
ORDER BY h.run_date DESC, h.run_time DESC;
```

### **Database Backup**:
   Automated daily backups are handled by a SQL Server Agent job. See [SQL Server Agent Jobs Documentation](./DB-SQL-Server-Agent-Jobs.md)) for details.
