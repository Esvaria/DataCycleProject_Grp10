# Data Warehouse Fact Tables Documentation

## Overview

The fact tables in the data warehouse are the central components of the star schema design, containing the measurements and metrics for analysis, along with foreign keys to the dimension tables. This document details each fact table and its associated ETL procedure.

## FactMachineCleaning

This fact table records cleaning cycle events and their associated measurements.

### Table Structure

| Column | Data Type | Description |
|--------|-----------|-------------|
| cleaning_id | int | Primary key (identity) |
| machine_id | int | Foreign key to DimMachine |
| date_id | int | Foreign key to DimDate |
| time_start_id | int | Foreign key to DimTime (start time) |
| time_end_id | int | Foreign key to DimTime (end time) |
| powder_clean_status | int | Foreign key to DimPowderStatus |
| tabs_status_left | int | Foreign key to DimTabsStatus |
| tabs_status_right | int | Foreign key to DimTabsStatus |
| detergent_status_left | int | Foreign key to DimDetergentStatus |
| detergent_status_right | int | Foreign key to DimDetergentStatus |
| milk_pump_error_left | bit | Error flag for left milk pump |
| milk_pump_error_right | bit | Error flag for right milk pump |
| milk_clean_temp_left_1 | decimal(10,2) | Left side cleaning temperature 1 |
| milk_clean_temp_left_2 | decimal(10,2) | Left side cleaning temperature 2 |
| milk_clean_temp_right_1 | decimal(10,2) | Right side cleaning temperature 1 |
| milk_clean_temp_right_2 | decimal(10,2) | Right side cleaning temperature 2 |
| milk_clean_rpm_left_1 | int | Left side cleaning RPM 1 |
| milk_clean_rpm_left_2 | int | Left side cleaning RPM 2 |
| milk_clean_rpm_right_1 | int | Right side cleaning RPM 1 |
| milk_clean_rpm_right_2 | int | Right side cleaning RPM 2 |
| milk_seq_cycle_left_1 | decimal(10,2) | Left side sequence cycle 1 |
| milk_seq_cycle_left_2 | decimal(10,2) | Left side sequence cycle 2 |
| milk_seq_cycle_right_1 | decimal(10,2) | Right side sequence cycle 1 |
| milk_seq_cycle_right_2 | decimal(10,2) | Right side sequence cycle 2 |
| milk_temp_left_1 | decimal(10,2) | Left side milk temperature 1 |
| milk_temp_left_2 | decimal(10,2) | Left side milk temperature 2 |
| milk_temp_right_1 | decimal(10,2) | Right side milk temperature 1 |
| milk_temp_right_2 | decimal(10,2) | Right side milk temperature 2 |
| milk_rpm_left_1 | int | Left side milk RPM 1 |
| milk_rpm_left_2 | int | Left side milk RPM 2 |
| milk_rpm_right_1 | int | Right side milk RPM 1 |
| milk_rpm_right_2 | int | Right side milk RPM 2 |
| cleaning_duration_minutes | int | Total duration in minutes |

### ETL Procedure: sp_LoadFactMachineCleaning

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
            milk_clean_temp_left_1,
            milk_clean_temp_left_2,
            milk_clean_temp_right_1,
            milk_clean_temp_right_2,
            milk_clean_rpm_left_1,
            milk_clean_rpm_left_2,
            milk_clean_rpm_right_1,
            milk_clean_rpm_right_2,
            milk_seq_cycle_left_1,
            milk_seq_cycle_left_2,
            milk_seq_cycle_right_1,
            milk_seq_cycle_right_2,
            milk_temp_left_1,
            milk_temp_left_2,
            milk_temp_right_1,
            milk_temp_right_2,
            milk_rpm_left_1,
            milk_rpm_left_2,
            milk_rpm_right_1,
            milk_rpm_right_2,
            cleaning_duration_minutes
        )
        SELECT
            c.machine_id,
            CAST(YEAR(c.timestamp_start) * 10000 + MONTH(c.timestamp_start) * 100 + DAY(c.timestamp_start) AS INT),
            (DATEPART(HOUR, c.timestamp_start) * 60 + DATEPART(MINUTE, c.timestamp_start)),
            (DATEPART(HOUR, c.timestamp_end) * 60 + DATEPART(MINUTE, c.timestamp_end)),
            -- Ensure powder_clean_status exists in DimPowderStatus
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimPowderStatus WHERE powder_status_id = CAST(ISNULL(c.powder_clean_status, 0) AS INT))
                THEN CAST(ISNULL(c.powder_clean_status, 0) AS INT)
                ELSE 0 -- Default to 'Undefined' if value doesn't exist
            END,
            -- Ensure tabs_status_left exists in DimTabsStatus
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimTabsStatus WHERE tabs_status_id = CAST(ISNULL(c.tabs_status_left, 2) AS INT))
                THEN CAST(ISNULL(c.tabs_status_left, 2) AS INT)
                ELSE 2 -- Default to 'Undefined' if value doesn't exist
            END,
            -- Ensure tabs_status_right exists in DimTabsStatus
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimTabsStatus WHERE tabs_status_id = CAST(ISNULL(c.tabs_status_right, 2) AS INT))
                THEN CAST(ISNULL(c.tabs_status_right, 2) AS INT)
                ELSE 2 -- Default to 'Undefined' if value doesn't exist
            END,
            -- Ensure detergent_status_left exists in DimDetergentStatus
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimDetergentStatus WHERE detergent_status_id = CAST(ISNULL(c.detergent_status_left, 0) AS INT))
                THEN CAST(ISNULL(c.detergent_status_left, 0) AS INT)
                ELSE 0 -- Default to 'Not defined' if value doesn't exist
            END,
            -- Ensure detergent_status_right exists in DimDetergentStatus
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimDetergentStatus WHERE detergent_status_id = CAST(ISNULL(c.detergent_status_right, 0) AS INT))
                THEN CAST(ISNULL(c.detergent_status_right, 0) AS INT)
                ELSE 0 -- Default to 'Not defined' if value doesn't exist
            END,
            CAST(CASE WHEN c.milk_pump_error_left = 0 THEN '0' ELSE '1' END AS BIT),
            CAST(CASE WHEN c.milk_pump_error_right = 0 THEN '0' ELSE '1' END AS BIT),
            ISNULL(c.milk_clean_temp_left_1, 0),
            ISNULL(c.milk_clean_temp_left_2, 0),
            ISNULL(c.milk_clean_temp_right_1, 0),
            ISNULL(c.milk_clean_temp_right_2, 0),
            ISNULL(c.milk_clean_rpm_left_1, 0),
            ISNULL(c.milk_clean_rpm_left_2, 0),
            ISNULL(c.milk_clean_rpm_right_1, 0),
            ISNULL(c.milk_clean_rpm_right_2, 0),
            ISNULL(c.milk_seq_cycle_left_1, 0),
            ISNULL(c.milk_seq_cycle_left_2, 0),
            ISNULL(c.milk_seq_cycle_right_1, 0),
            ISNULL(c.milk_seq_cycle_right_2, 0),
            ISNULL(c.milk_temp_left_1, 0),
            ISNULL(c.milk_temp_left_2, 0),
            ISNULL(c.milk_temp_right_1, 0),
            ISNULL(c.milk_temp_right_2, 0),
            ISNULL(c.milk_rpm_left_1, 0),
            ISNULL(c.milk_rpm_left_2, 0),
            ISNULL(c.milk_rpm_right_1, 0),
            ISNULL(c.milk_rpm_right_2, 0),
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

## FactRinseOperation

This fact table records rinsing operations and measurements.

### Table Structure

| Column | Data Type | Description |
|--------|-----------|-------------|
| rinse_id | int | Primary key (identity) |
| machine_id | int | Foreign key to DimMachine |
| date_id | int | Foreign key to DimDate |
| time_id | int | Foreign key to DimTime |
| rinse_type | int | Foreign key to DimRinseType |
| flow_rate_left | decimal(10,2) | Left side flow rate |
| flow_rate_right | decimal(10,2) | Right side flow rate |
| status_left | int | Foreign key to DimRinseStatus |
| status_right | int | Foreign key to DimRinseStatus |
| pump_pressure | decimal(10,2) | Pump pressure measurement |
| nozzle_flow_rate_left | decimal(10,2) | Left nozzle flow rate |
| nozzle_flow_rate_right | decimal(10,2) | Right nozzle flow rate |
| nozzle_status_left | int | Foreign key to DimNozzleStatus |
| nozzle_status_right | int | Foreign key to DimNozzleStatus |

### ETL Procedure: sp_LoadFactRinseOperation

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
            -- Map to valid dimension values
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimRinseType WHERE rinse_type_id = ISNULL(r.rinse_type, 255))
                THEN ISNULL(r.rinse_type, 255)
                ELSE 0 -- Default to first value
            END,
            ISNULL(r.flow_rate_left, 0),
            ISNULL(r.flow_rate_right, 0),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimRinseStatus WHERE rinse_status_id = ISNULL(r.status_left, 0))
                THEN ISNULL(r.status_left, 0)
                ELSE 0 -- Default to first value
            END,
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimRinseStatus WHERE rinse_status_id = ISNULL(r.status_right, 0))
                THEN ISNULL(r.status_right, 0)
                ELSE 0 -- Default to first value
            END,
            ISNULL(r.pump_pressure, 0),
            ISNULL(r.nozzle_flow_rate_left, 0),
            ISNULL(r.nozzle_flow_rate_right, 0),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimNozzleStatus WHERE nozzle_status_id = ISNULL(r.nozzle_status_left, 0))
                THEN ISNULL(r.nozzle_status_left, 0)
                ELSE 0 -- Default to 'Undefined'
            END,
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimNozzleStatus WHERE nozzle_status_id = ISNULL(r.nozzle_status_right, 0))
                THEN ISNULL(r.nozzle_status_right, 0)
                ELSE 0 -- Default to 'Undefined'
            END
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

## FactInfoLog

This fact table records information and error messages logged by machines.

### Table Structure

| Column | Data Type | Description |
|--------|-----------|-------------|
| info_id | int | Primary key (identity) |
| machine_id | int | Foreign key to DimMachine |
| date_id | int | Foreign key to DimDate |
| time_id | int | Foreign key to DimTime |
| number | int | Message number |
| typography | nvarchar(255) | Message typography |
| type_number | nvarchar(255) | Message type |

### ETL Procedure: sp_LoadFactInfoLog

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

## FactProductRun

This fact table records individual beverage production runs.

### Table Structure

| Column | Data Type | Description |
|--------|-----------|-------------|
| production_id | int | Primary key (identity) |
| machine_id | int | Foreign key to DimMachine |
| date_id | int | Foreign key to DimDate |
| time_id | int | Foreign key to DimTime |
| product_type_id | int | Foreign key to DimProductType |
| double_prod | bit | Double product flag |
| bean_hopper | int | Foreign key to DimBeanHopper |
| press_before | decimal(10,2) | Pressure before extraction |
| press_after | decimal(10,2) | Pressure after extraction |
| press_final | decimal(10,2) | Final pressure |
| grind_time | decimal(10,2) | Grinding time |
| ext_time | decimal(10,2) | Extraction time |
| water_qnty | decimal(10,2) | Water quantity |
| water_temp | decimal(10,2) | Water temperature |
| outlet_side | int | Foreign key to DimOutletSide |
| stopped | int | Foreign key to DimStopped |
| milk_temp | decimal(10,2) | Milk temperature |
| steam_pressure | decimal(10,2) | Steam pressure |
| grind_adjust_left | decimal(10,2) | Left grinder adjustment |
| grind_adjust_right | decimal(10,2) | Right grinder adjustment |
| milk_time | decimal(10,2) | Milk processing time |
| boiler_temp | decimal(10,2) | Boiler temperature |

### ETL Procedure: sp_LoadFactProductRun

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
            -- Map to valid dimension values
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimProductType WHERE product_type_id = ISNULL(p.prod_type, 255))
                THEN ISNULL(p.prod_type, 255)
                ELSE 0 -- Default to first value
            END,
            CAST(CASE WHEN p.double_prod = 1 THEN '1' ELSE '0' END AS BIT),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimBeanHopper WHERE bean_hopper_id = ISNULL(p.bean_hopper, 255))
                THEN ISNULL(p.bean_hopper, 255)
                ELSE 255 -- Default to 'None'
            END,
            ISNULL(p.press_before, 0),
            ISNULL(p.press_after, 0),
            ISNULL(p.press_final, 0),
            ISNULL(p.grind_time, 0),
            ISNULL(p.ext_time, 0),
            ISNULL(p.water_qnty, 0),
            ISNULL(p.water_temp, 0),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimOutletSide WHERE outlet_side_id = p.outlet_side)
                THEN p.outlet_side
                ELSE 0 -- Default to 'Left'
            END,
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimStopped WHERE stopped_id = p.stopped)
                THEN p.stopped
                ELSE 0 -- Default to 'Finished'
            END,
            ISNULL(p.milk_temp, 0),
            ISNULL(p.steam_pressure, 0),
            ISNULL(p.grind_adjust_left, 0),
            ISNULL(p.grind_adjust_right, 0),
            ISNULL(p.milk_time, 0),
            ISNULL(p.boiler_temp, 0)
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

## Common Patterns in Fact Table ETL

All fact table ETL procedures follow similar patterns:

1. **Data Validation**: Validating all source data and foreign keys
2. **Default Value Assignment**: Replacing nulls or invalid values with meaningful defaults
3. **Incremental Loading**: Only loading new records to avoid duplication
4. **Error Handling**: Comprehensive error trapping and reporting
5. **Derived Metrics**: Calculating additional metrics where appropriate (e.g., duration)

## Optimizing Fact Table Queries

For optimal performance when querying fact tables:

1. **Create appropriate indexes**:
   ```sql
   -- Example: Index on commonly filtered dimensions
   CREATE INDEX IX_FactProductRun_ProductType 
   ON FactProductRun(product_type_id);
   
   -- Example: Index on date range queries
   CREATE INDEX IX_FactMachineCleaning_DateRange
   ON FactMachineCleaning(date_id, machine_id);
   ```

2. **Create partitioned views** for large fact tables:
   ```sql
   -- Example: Partitioned view by year
   CREATE VIEW vw_FactProductRun_Current
   AS
   SELECT * FROM FactProductRun
   WHERE date_id >= 20240101;
   ```

3. **Create aggregated views** for common analysis patterns:
   ```sql
   -- Example: Daily product counts by type
   CREATE VIEW vw_DailyProductCounts
   AS
   SELECT 
       date_id, 
       product_type_id,
       COUNT(*) as product_count
   FROM FactProductRun
   GROUP BY date_id, product_type_id;
   ```

## Analysis Capabilities

The fact tables enable various analysis scenarios:

1. **Machine Performance**: Track cleaning cycles, rinse operations, and product runs by machine
2. **Product Popularity**: Analyze which product types are most commonly produced
3. **Maintenance Patterns**: Identify machines requiring more frequent cleaning or maintenance
4. **Quality Control**: Monitor temperature, pressure, and other quality indicators
5. **Time-Based Analysis**: Analyze patterns by time of day, day of week, etc.