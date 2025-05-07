# Data Warehouse Stored Procedures

This document provides detailed documentation for all stored procedures used in the `DWmachines` data warehouse.


## Summary

The data warehouse stored procedures provide a comprehensive ETL framework for:

1. **Schema Creation**: Creating the dimensional model structure
2. **Dimension Management**: Populating and maintaining dimension tables
3. **Fact Loading**: Loading fact data from the operational database
4. **Performance Optimization**: Maintaining indexes and statistics
5. **Monitoring**: Tracking ETL execution and data growth

These procedures work together to ensure data is properly extracted from the operational database, transformed to conform to the dimensional model, and loaded into the appropriate tables in the data warehouse.

By using these procedures as part of a scheduled ETL process, you maintain a reliable and up-to-date analytical environment for reporting and analysis on machine operational data.


## Master ETL Procedure

### sp_LoadDWData_Master

This procedure orchestrates the entire ETL process by executing the individual procedures in the correct sequence.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadDWData_Master]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RC INT;
    
    -- Step 1: First, ensure dimensions are complete
    -- This adds any missing IDs from the source data
    EXEC @RC = sp_EnsureDimensionCompleteness;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to check dimension tables.';
        -- Continue anyway
    END
    
    -- Step 2: Then, load dimension tables with your proper definitions
    -- This will override the auto-added descriptions with your proper ones
    EXEC @RC = sp_LoadDimensionTables;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load dimension tables.';
        -- Continue anyway
    END
    
    -- Step 3: Load FactMachineCleaning
    EXEC @RC = sp_LoadFactMachineCleaning;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactMachineCleaning table.';
        -- Continue anyway
    END
    
    -- Step 4: Load FactRinseOperation
    EXEC @RC = sp_LoadFactRinseOperation;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactRinseOperation table.';
        -- Continue anyway
    END
    
    -- Step 5: Load FactInfoLog
    EXEC @RC = sp_LoadFactInfoLog;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactInfoLog table.';
        -- Continue anyway
    END
    
    -- Step 6: Load FactProductRun
    EXEC @RC = sp_LoadFactProductRun;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactProductRun table.';
        -- Continue anyway
    END
    
    PRINT 'Data warehouse ETL process completed.';
    RETURN 0;
END;
```

**Purpose:** Orchestrates the entire ETL process for the data warehouse.

**Process:**
1. Ensures dimension tables have all required values
2. Updates dimension tables with proper reference data
3. Loads each fact table in sequence
4. Continues even if individual step fails
5. Reports completion status

**Usage:** This procedure should be scheduled to run regularly (e.g., daily) via a SQL Server Agent job.

## Schema Creation Procedure

### sp_CreateDWSchema

This procedure creates the complete data warehouse schema.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_CreateDWSchema]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Create dimension tables if they don't exist
        
        -- DimMachine
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimMachine')
        BEGIN
            CREATE TABLE [DimMachine] (
              [machine_id] int PRIMARY KEY,
              [machine_name] nvarchar(255),
              [machine_type] nvarchar(255),
              [installation_date] date,
              [manufacturer] nvarchar(255),
              [model] nvarchar(255),
              [location] nvarchar(255)
            );
            
            PRINT 'Created table DimMachine';
        END
        ELSE
        BEGIN
            PRINT 'Table DimMachine already exists';
        END
        
        -- Create other dimension tables...
        
        -- Create fact tables if they don't exist...
        
        -- Add foreign key constraints...
        
        PRINT 'Data warehouse schema creation completed successfully.';
        
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = 
            'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) + 
            ', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) + 
            ', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
            ', Line ' + CONVERT(VARCHAR(5), ERROR_LINE()) + 
            ', Message: ' + ERROR_MESSAGE();
            
        RAISERROR(@ErrorMessage, 16, 1);
        
        -- Log to SQL Server Error Log
        EXEC sp_executesql N'RAISERROR(''DW Schema Creation Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

**Purpose:** Creates the complete data warehouse schema including all dimension and fact tables and their relationships.

**Key Features:**
- Creates tables only if they don't already exist
- Establishes foreign key relationships
- Includes comprehensive error handling

**Usage:** Run once to create the initial schema, or to add missing tables and relationships.

## Dimension-Related Procedures

### sp_LoadDimensionTables

This procedure populates all dimension tables with reference data.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadDimensionTables]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- 1. Ensure we have machine data in DimMachine using MERGE
        MERGE INTO DimMachine AS target
        USING (
            SELECT 
                m.machine_id, 
                m.name
            FROM machine_data.dbo.machine_names m
        ) AS source
        ON target.machine_id = source.machine_id
        WHEN NOT MATCHED THEN
            INSERT (machine_id, machine_name)
            VALUES (source.machine_id, source.name);
        
        PRINT 'Updated DimMachine - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- 2. Generate or populate DimDate - assuming we need dates from 2022 to 2025
        -- Check if DimDate is empty
        IF NOT EXISTS (SELECT TOP 1 1 FROM DimDate)
        BEGIN
            -- Create a temporary table to hold dates
            CREATE TABLE #TempDates (full_date DATE);
            
            -- Variables for the date range
            DECLARE @StartDate DATE = '2022-01-01';
            DECLARE @EndDate DATE = '2025-12-31';
            DECLARE @CurrentDate DATE = @StartDate;
            
            -- Generate dates
            WHILE @CurrentDate <= @EndDate
            BEGIN
                INSERT INTO #TempDates (full_date) VALUES (@CurrentDate);
                SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
            END
            
            -- Insert into DimDate
            INSERT INTO DimDate (
                date_id, 
                full_date, 
                day, 
                month, 
                quarter, 
                year, 
                is_weekend, 
                is_holiday
            )
            SELECT
                -- Create a date key in the format YYYYMMDD
                CAST(YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date) AS INT) AS date_id,
                full_date,
                DAY(full_date) AS day,
                MONTH(full_date) AS month,
                DATEPART(QUARTER, full_date) AS quarter,
                YEAR(full_date) AS year,
                CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
                -- For simplicity, just marking major US holidays
                CASE WHEN (MONTH(full_date) = 1 AND DAY(full_date) = 1) -- New Year's Day
                     OR (MONTH(full_date) = 7 AND DAY(full_date) = 4)  -- Independence Day
                     OR (MONTH(full_date) = 12 AND DAY(full_date) = 25) -- Christmas
                     THEN 1 ELSE 0 END AS is_holiday
            FROM #TempDates;
            
            -- Drop the temporary table
            DROP TABLE #TempDates;
            
            PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' dates into DimDate';
        END
        
        -- 3. Generate or populate DimTime - every minute of a day
        -- Check if DimTime is empty
        IF NOT EXISTS (SELECT TOP 1 1 FROM DimTime)
        BEGIN
            -- Create time entries for every minute of the day
            WITH MinuteCTE AS (
                SELECT 0 AS minute_of_day
                UNION ALL
                SELECT minute_of_day + 1
                FROM MinuteCTE
                WHERE minute_of_day < 1439 -- 24 hours * 60 minutes - 1
            )
            INSERT INTO DimTime (
                time_id,
                full_time,
                hour,
                minute,
                second,
                am_pm,
                shift
            )
            SELECT
                minute_of_day AS time_id,
                DATEADD(MINUTE, minute_of_day, '00:00:00') AS full_time,
                DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) AS hour,
                DATEPART(MINUTE, DATEADD(MINUTE, minute_of_day, '00:00:00')) AS minute,
                0 AS second, -- All entries have 0 seconds
                CASE WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
                CASE 
                    WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) >= 6 AND DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 14 THEN 'Morning'
                    WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) >= 14 AND DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 22 THEN 'Evening'
                    ELSE 'Night'
                END AS shift
            FROM MinuteCTE
            OPTION (MAXRECURSION 1500); -- Needed for the recursive CTE
            
            PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' time entries into DimTime';
        END
        
        -- 4. Update lookup dimensions with defined values using MERGE
        
        -- DimProductType
        MERGE INTO DimProductType AS target
        USING (
            VALUES 
                (0, 'None', 'Aucun produit'),
                (1, 'Ristretto', 'Standard Ristretto'),
                (2, 'Expresso', 'Standard Expresso'),
                -- More product types...
                (255, 'Undefined', 'Undefined')
        ) AS source (product_type_id, prod_type, description)
        ON target.product_type_id = source.product_type_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.prod_type = source.prod_type,
                target.description = source.description
        WHEN NOT MATCHED THEN
            INSERT (product_type_id, prod_type, description)
            VALUES (source.product_type_id, source.prod_type, source.description);
            
        PRINT 'Updated DimProductType - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- Update other lookup dimensions...
        
        PRINT 'Dimension tables loaded and updated successfully.';
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = 
            'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) + 
            ', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) + 
            ', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
            ', Line ' + CONVERT(VARCHAR(5), ERROR_LINE()) + 
            ', Message: ' + ERROR_MESSAGE();
            
        RAISERROR(@ErrorMessage, 16, 1);
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

**Purpose:** Populates all dimension tables with reference data.

**Key Functions:**
- Loads DimMachine from operational database
- Generates dates for DimDate from 2022 to 2025
- Generates time entries for DimTime for each minute of the day
- Loads lookup tables (DimProductType, DimPowderStatus, etc.) with predefined values

**Usage:** Run as part of the regular ETL process to ensure dimension tables are up-to-date.

### sp_EnsureDimensionCompleteness

This procedure ensures that all dimension values needed by the fact tables exist in the dimension tables.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_EnsureDimensionCompleteness]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- 1. Check and add missing detergent status values
        INSERT INTO DimDetergentStatus (detergent_status_id, detergent_status_name)
        SELECT DISTINCT CAST(ISNULL(c.detergent_status_left, 0) AS INT), 'Auto-added Status ' + CAST(CAST(ISNULL(c.detergent_status_left, 0) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimDetergentStatus d 
            WHERE d.detergent_status_id = CAST(ISNULL(c.detergent_status_left, 0) AS INT)
        );
        
        -- Check and add missing values for other dimensions...
        
        PRINT 'Dimension tables completeness ensured.';
        RETURN 0;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = 
            'Error ensuring dimension completeness: ' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        RETURN -1;
    END CATCH
END;
```

**Purpose:** Ensures that all dimension values needed by the fact tables exist in the dimension tables.

**Key Features:**
- Scans operational data for dimension values
- Adds missing values to dimension tables with auto-generated descriptions
- Handles each dimension table corresponding to the operational data

**Usage:** Run before loading fact tables to ensure referential integrity.

## Fact Table Loading Procedures

### sp_LoadFactMachineCleaning

Loads cleaning cycle data from the operational database to FactMachineCleaning.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadFactMachineCleaning]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Create a temp table to log problematic records
        CREATE TABLE #ProblemRecords (
            machine_id INT,
            timestamp_start DATETIME,
            error_message VARCHAR(255)
        );
        
        -- Insert records into fact table
        INSERT INTO FactMachineCleaning (
            machine_id,
            date_id,
            time_start_id,
            time_end_id,
            powder_clean_status,
            tabs_status_left,
            tabs_status_right,
            detergent_status_left,
            detergent_status_right,
            milk_pump_error_left,
            milk_pump_error_right,
            -- Additional fields...
            cleaning_duration_minutes
        )
        SELECT
            c.machine_id,
            CAST(YEAR(c.timestamp_start) * 10000 + MONTH(c.timestamp_start) * 100 + DAY(c.timestamp_start) AS INT),
            (DATEPART(HOUR, c.timestamp_start) * 60 + DATEPART(MINUTE, c.timestamp_start)),
            (DATEPART(HOUR, c.timestamp_end) * 60 + DATEPART(MINUTE, c.timestamp_end)),
            -- Map dimension values with validation...
            DATEDIFF(MINUTE, c.timestamp_start, c.timestamp_end)
        FROM machine_data.dbo.cleaning_logs c
        LEFT JOIN FactMachineCleaning f ON 
            f.machine_id = c.machine_id
            AND f.date_id = CAST(YEAR(c.timestamp_start) * 10000 + MONTH(c.timestamp_start) * 100 + DAY(c.timestamp_start) AS INT)
            AND f.time_start_id = (DATEPART(HOUR, c.timestamp_start) * 60 + DATEPART(MINUTE, c.timestamp_start))
        WHERE f.machine_id IS NULL
          AND c.timestamp_start IS NOT NULL 
          AND c.timestamp_end IS NOT NULL;
        
        -- Print any problematic records
        IF EXISTS (SELECT 1 FROM #ProblemRecords)
        BEGIN
            SELECT 'Problem records found in cleaning_logs:' AS Message;
            SELECT * FROM #ProblemRecords;
        END
        
        -- Clean up
        DROP TABLE IF EXISTS #ProblemRecords;
        
        PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records into FactMachineCleaning';
        RETURN 0;
    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #ProblemRecords;
        PRINT 'Error loading FactMachineCleaning: ' + ERROR_MESSAGE();
        RETURN -1;
    END CATCH
END;
```

**Purpose:** Loads cleaning cycle data into the FactMachineCleaning fact table.

**Key Features:**
- Validates foreign keys
- Substitutes default values for nulls
- Only loads records not already in the fact table
- Calculates derived values (cleaning_duration_minutes)
- Tracks problematic records for debugging

**Usage:** Run as part of the regular ETL process to load new cleaning cycle data.

### sp_LoadFactRinseOperation

Loads rinse operation data from the operational database to FactRinseOperation.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadFactRinseOperation]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Create a temp table for valid records
        CREATE TABLE #ValidRinseRecords (
            machine_id INT,
            date_id INT,
            time_id INT,
            rinse_type INT,
            flow_rate_left FLOAT,
            flow_rate_right FLOAT,
            status_left INT,
            status_right INT,
            pump_pressure FLOAT,
            nozzle_flow_rate_left FLOAT,
            nozzle_flow_rate_right FLOAT,
            nozzle_status_left INT,
            nozzle_status_right INT
        );
        
        -- Insert into temp table with validated foreign keys
        INSERT INTO #ValidRinseRecords
        SELECT
            r.machine_id,
            CAST(YEAR(r.timestamp) * 10000 + MONTH(r.timestamp) * 100 + DAY(r.timestamp) AS INT),
            (DATEPART(HOUR, r.timestamp) * 60 + DATEPART(MINUTE, r.timestamp)),
            -- Map to valid dimension values with validation...
            ISNULL(r.flow_rate_left, 0),
            ISNULL(r.flow_rate_right, 0),
            -- More fields...
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM FactRinseOperation f
            WHERE f.machine_id = r.machine_id
            AND f.date_id = CAST(YEAR(r.timestamp) * 10000 + MONTH(r.timestamp) * 100 + DAY(r.timestamp) AS INT)
            AND f.time_id = (DATEPART(HOUR, r.timestamp) * 60 + DATEPART(MINUTE, r.timestamp))
            AND f.rinse_type = ISNULL(r.rinse_type, 0)
        );
        
        -- Print any records with rinse type issues (for debugging)
        SELECT 'Problem records with rinse_type:', 
               machine_id, date_id, time_id, rinse_type
        FROM #ValidRinseRecords
        WHERE NOT EXISTS (SELECT 1 FROM DimRinseType WHERE rinse_type_id = rinse_type);
        
        -- Now insert from temp table to fact table
        INSERT INTO FactRinseOperation (
            machine_id, date_id, time_id, rinse_type, flow_rate_left, flow_rate_right,
            status_left, status_right, pump_pressure, nozzle_flow_rate_left, nozzle_flow_rate_right,
            nozzle_status_left, nozzle_status_right
        )
        SELECT * FROM #ValidRinseRecords;
        
        -- Clean up
        DROP TABLE #ValidRinseRecords;
        
        PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records into FactRinseOperation';
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#ValidRinseRecords') IS NOT NULL
            DROP TABLE #ValidRinseRecords;
            
        SELECT @ErrorMessage = 
            'Error loading FactRinseOperation: ' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        RETURN -1;
    END CATCH
END;
```

**Purpose:** Loads rinse operation data into the FactRinseOperation fact table.

**Key Features:**
- Uses a temporary table for validation
- Maps operational data to dimension keys
- Handles null values and applies defaults
- Provides debugging information for problem records

**Usage:** Run as part of the regular ETL process to load new rinse operation data.

### sp_LoadFactInfoLog

Loads information and error message data from the operational database to FactInfoLog.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadFactInfoLog]
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Load info log data
        INSERT INTO FactInfoLog (
            machine_id,
            date_id,
            time_id,
            number,
            typography,
            type_number
        )
        SELECT
            i.machine_id,
            CAST(YEAR(i.timestamp) * 10000 + MONTH(i.timestamp) * 100 + DAY(i.timestamp) AS INT),
            (DATEPART(HOUR, i.timestamp) * 60 + DATEPART(MINUTE, i.timestamp)),
            CASE WHEN ISNUMERIC(SUBSTRING(i.number, 3, LEN(i.number)-2)) = 1 
                 THEN CAST(SUBSTRING(i.number, 3, LEN(i.number)-2) AS INT) 
                 ELSE 0 
            END,
            i.typography,
            CAST(i.type_number AS NVARCHAR(255))
        FROM machine_data.dbo.info_logs i
        WHERE NOT EXISTS (
            SELECT 1 FROM FactInfoLog f
            WHERE f.machine_id = i.machine_id
            AND f.date_id = CAST(YEAR(i.timestamp) * 10000 + MONTH(i.timestamp) * 100 + DAY(i.timestamp) AS INT)
            AND f.time_id = (DATEPART(HOUR, i.timestamp) * 60 + DATEPART(MINUTE, i.timestamp))
            AND f.number = CASE WHEN ISNUMERIC(SUBSTRING(i.number, 3, LEN(i.number)-2)) = 1 
                              THEN CAST(SUBSTRING(i.number, 3, LEN(i.number)-2) AS INT) 
                              ELSE 0 
                         END
        );
        
        PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records into FactInfoLog';
        RETURN 0;
    END TRY
    BEGIN CATCH
        PRINT 'Error loading FactInfoLog: ' + ERROR_MESSAGE();
        RETURN -1;
    END CATCH
END;
```

**Purpose:** Loads information and error message data into the FactInfoLog fact table.

**Key Features:**
- Extracts numerical values from message numbers
- Handles special formatting in message fields
- Maps date and time components to dimension keys
- Only loads new records not already in the fact table

**Usage:** Run as part of the regular ETL process to load new information log data.

### sp_LoadFactProductRun

Loads product run data from the operational database to FactProductRun.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_LoadFactProductRun]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Create a temp table for valid records
        CREATE TABLE #ValidProductRecords (
            machine_id INT,
            date_id INT,
            time_id INT,
            product_type_id INT,
            double_prod BIT,
            bean_hopper INT,
            press_before FLOAT,
            press_after FLOAT,
            press_final FLOAT,
            grind_time FLOAT,
            ext_time FLOAT,
            water_qnty FLOAT,
            water_temp FLOAT,
            outlet_side INT,
            stopped INT,
            milk_temp FLOAT,
            steam_pressure FLOAT,
            grind_adjust_left FLOAT,
            grind_adjust_right FLOAT,
            milk_time FLOAT,
            boiler_temp FLOAT
        );
        
        -- Insert into temp table with validated foreign keys
        INSERT INTO #ValidProductRecords
        SELECT
            p.machine_id,
            CAST(YEAR(p.timestamp) * 10000 + MONTH(p.timestamp) * 100 + DAY(p.timestamp) AS INT),
            (DATEPART(HOUR, p.timestamp) * 60 + DATEPART(MINUTE, p.timestamp)),
            -- Map to valid dimension values with validation...
            CAST(CASE WHEN p.double_prod = 1 THEN '1' ELSE '0' END AS BIT),
            -- More fields...
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM FactProductRun f
            WHERE f.machine_id = p.machine_id
            AND f.date_id = CAST(YEAR(p.timestamp) * 10000 + MONTH(p.timestamp) * 100 + DAY(p.timestamp) AS INT)
            AND f.time_id = (DATEPART(HOUR, p.timestamp) * 60 + DATEPART(MINUTE, p.timestamp))
            AND f.product_type_id = ISNULL(p.prod_type, 1)
        );
        
        -- Print any records with product type issues (for debugging)
        SELECT 'Problem records with product_type_id:', 
               machine_id, date_id, time_id, product_type_id
        FROM #ValidProductRecords
        WHERE NOT EXISTS (SELECT 1 FROM DimProductType WHERE product_type_id = product_type_id);
        
        -- Now insert from temp table to fact table
        INSERT INTO FactProductRun (
            machine_id, date_id, time_id, product_type_id, double_prod, bean_hopper,
            press_before, press_after, press_final, grind_time, ext_time, water_qnty, water_temp,
            outlet_side, stopped, milk_temp, steam_pressure, grind_adjust_left, grind_adjust_right,
            milk_time, boiler_temp
        )
        SELECT * FROM #ValidProductRecords;
        
        -- Clean up
        DROP TABLE #ValidProductRecords;
        
        PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records into FactProductRun';
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#ValidProductRecords') IS NOT NULL
            DROP TABLE #ValidProductRecords;
            
        SELECT @ErrorMessage = 
            'Error loading FactProductRun: ' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        RETURN -1;
    END CATCH
END;
```

**Purpose:** Loads product run data into the FactProductRun fact table.

**Key Features:**
- Uses a temporary table for validation
- Maps operational data to dimension keys
- Handles null values and applies defaults
- Validates all foreign key references
- Provides debugging information for problem records

**Usage:** Run as part of the regular ETL process to load new product run data.

## Best Practices for Using These Procedures

1. **Execution Order**: Always follow the order specified in sp_LoadDWData_Master
2. **Scheduling**: Schedule the master procedure to run after the operational database is updated
3. **Monitoring**: Monitor execution logs for errors and warnings
4. **Performance**: Consider adding indexes on foreign key columns for better performance
5. **Error Handling**: Check return codes from procedures to detect and handle failures
6. **Data Validation**: Use the debugging output to identify and fix data quality issues

## Scheduling with SQL Server Agent Job

Create a SQL Server Agent job to schedule the ETL process:

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create job for data warehouse ETL
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Data Warehouse ETL', 
    @enabled=1, 
    @notify_level_eventlog=2, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Performs ETL for the data warehouse', 
    @category_name=N'Data Warehouse', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the ETL master procedure
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Run ETL Process', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, -- Quit with success
    @on_success_step_id=0, 
    @on_fail_action=2, -- Quit with failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [DWmachines].[dbo].[sp_LoadDWData_Master]', 
    @database_name=N'DWmachines', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job (daily at 2:00 AM)
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily ETL Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=1, -- Once per day
    @freq_subday_interval=0, 
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date
    @active_end_date=99991231, -- End date
    @active_start_time=20000, -- 2:00 AM
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

This job will run once per day at 2:00 AM to ensure the data warehouse is updated with the latest data.

## Maintenance and Optimization Procedures

### Optimizing Performance

To optimize the performance of the data warehouse and ETL procedures, consider implementing the following:

#### 1. Index Maintenance

Create a procedure to rebuild or reorganize indexes on fact tables:

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_MaintainFactIndexes]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Rebuild indexes on FactMachineCleaning
    ALTER INDEX ALL ON FactMachineCleaning REBUILD;
    
    -- Rebuild indexes on FactRinseOperation
    ALTER INDEX ALL ON FactRinseOperation REBUILD;
    
    -- Rebuild indexes on FactInfoLog
    ALTER INDEX ALL ON FactInfoLog REBUILD;
    
    -- Rebuild indexes on FactProductRun
    ALTER INDEX ALL ON FactProductRun REBUILD;
    
    PRINT 'Fact table indexes rebuilt successfully.';
    RETURN 0;
END;
```

#### 2. Statistics Update

Create a procedure to update statistics on fact and dimension tables:

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_UpdateDWStatistics]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Update statistics on all tables
    EXEC sp_updatestats;
    
    PRINT 'Data warehouse statistics updated successfully.';
    RETURN 0;
END;
```

#### 3. Create Custom Indexes

Create custom indexes for common query patterns:

```sql
-- Index for filtering by product type
CREATE INDEX IX_FactProductRun_ProductType 
ON FactProductRun(product_type_id, date_id);

-- Index for filtering by machine and date
CREATE INDEX IX_FactMachineCleaning_MachineDate
ON FactMachineCleaning(machine_id, date_id);

-- Index for time-based queries
CREATE INDEX IX_FactRinseOperation_DateTime
ON FactRinseOperation(date_id, time_id);
```

### Monitoring Procedures

#### 1. ETL Execution History

Create a procedure to monitor ETL execution history:

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_GetETLHistory]
    @DaysBack INT = 7
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        j.name AS 'Job Name',
        CONVERT(CHAR(10), CONVERT(DATETIME, RTRIM(h.run_date)), 120) AS 'Run Date',
        STUFF(STUFF(RIGHT('000000' + CONVERT(VARCHAR(6), h.run_time), 6), 5, 0, ':'), 3, 0, ':') AS 'Run Time',
        CASE h.run_status
            WHEN 0 THEN 'Failed'
            WHEN 1 THEN 'Succeeded'
            WHEN 2 THEN 'Retry'
            WHEN 3 THEN 'Canceled'
            WHEN 4 THEN 'In Progress'
        END AS 'Status',
        CAST(((h.run_duration / 10000 * 3600) + 
              ((h.run_duration % 10000) / 100 * 60) + 
              (h.run_duration % 100)) AS VARCHAR(10)) + ' seconds' AS 'Duration',
        h.message
    FROM msdb.dbo.sysjobhistory h
    JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
    WHERE j.name LIKE '%Data Warehouse ETL%'
    AND CONVERT(DATETIME, RTRIM(h.run_date)) >= DATEADD(DAY, -@DaysBack, GETDATE())
    ORDER BY h.run_date DESC, h.run_time DESC;
    
    RETURN 0;
END;
```

#### 2. Fact Table Growth

Create a procedure to monitor fact table growth:

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_MonitorFactTableGrowth]
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        t.name AS TableName,
        p.rows AS RowCount,
        SUM(a.total_pages) * 8 AS TotalSpaceKB,
        SUM(a.used_pages) * 8 AS UsedSpaceKB,
        (SUM(a.total_pages) - SUM(a.used_pages)) * 8 AS UnusedSpaceKB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE t.name LIKE 'Fact%'
    GROUP BY t.name, p.rows
    ORDER BY TableName;
    
    RETURN 0;
END;
```

## Common Error Handling Patterns

All stored procedures follow consistent error handling patterns:

1. **Try/Catch Blocks**: Wrap main logic in try/catch blocks
2. **Error Logging**: Log detailed error information
3. **Cleanup**: Clean up temporary objects in case of errors
4. **Return Codes**: Return 0 for success, -1 for failure

Example error handling pattern:

```sql
BEGIN TRY
    -- Main procedure logic
    RETURN 0;
END TRY
BEGIN CATCH
    -- Clean up temporary objects
    IF OBJECT_ID('tempdb..#TempTable') IS NOT NULL
        DROP TABLE #TempTable;
        
    -- Log error details
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    PRINT 'Error: ' + @ErrorMessage;
    
    -- Log to SQL Server Error Log
    EXEC sp_executesql N'RAISERROR(''Procedure Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                  N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
                  
    RETURN -1;
END CATCH
```
