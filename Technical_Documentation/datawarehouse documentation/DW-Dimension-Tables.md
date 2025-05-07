# Data Warehouse Dimension Tables Documentation

## Overview

Dimension tables in the data warehouse provide the descriptive context for the measurements in fact tables. They represent the "who, what, where, when, why, and how" of business processes. This document details each dimension table and its population process.

## Dimension Table Population

The `sp_LoadDimensionTables` procedure populates all dimension tables with reference data. The procedure also uses the MERGE statement to handle updates to existing entries without creating duplicates.

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
        
        -- Generate dates and times
        -- Load reference data for other dimensions
        
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

## Dimension Completeness

The `sp_EnsureDimensionCompleteness` procedure ensures that all dimension values needed by the fact tables exist in the dimension tables. This is crucial for maintaining referential integrity.

```sql
USE [DWmachines]
GO

CREATE PROCEDURE [dbo].[sp_EnsureDimensionCompleteness]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Scan operational data for dimension values and add missing ones
        
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

## Standard Dimensions

### DimMachine

Stores information about individual coffee machines.

| Column | Data Type | Description |
|--------|-----------|-------------|
| machine_id | int | Primary key, matches source system ID |
| machine_name | nvarchar(255) | Machine name or identifier |
| machine_type | nvarchar(255) | Type of coffee machine |
| installation_date | date | Date machine was installed |
| manufacturer | nvarchar(255) | Machine manufacturer |
| model | nvarchar(255) | Machine model number |
| location | nvarchar(255) | Physical location of the machine |

#### Population

The DimMachine table is populated directly from the machine_names table in the machine_data database, with additional attributes added as they become available.

### DimDate

Contains a record for each calendar date needed for analysis.

| Column | Data Type | Description |
|--------|-----------|-------------|
| date_id | int | Primary key (format: YYYYMMDD) |
| full_date | date | Actual date value |
| day | int | Day of month (1-31) |
| month | int | Month number (1-12) |
| quarter | int | Quarter (1-4) |
| year | int | Four-digit year |
| is_weekend | bit | Flag for weekend days |
| is_holiday | bit | Flag for holidays |

#### Population

The DimDate table is pre-populated with dates from 2022 to 2025 using a date generation algorithm in the `sp_LoadDimensionTables` procedure.

### DimTime

Contains a record for each minute of the day for time-based analysis.

| Column | Data Type | Description |
|--------|-----------|-------------|
| time_id | int | Primary key (minutes since midnight) |
| full_time | time | Time value (HH:MM:SS) |
| hour | int | Hour component (0-23) |
| minute | int | Minute component (0-59) |
| second | int | Second component (always 0) |
| am_pm | nvarchar(255) | AM/PM indicator |
| shift | nvarchar(255) | Work shift designation |

#### Population

The DimTime table is pre-populated with all 1,440 minutes of a day (24 hours × 60 minutes) using a recursive CTE in the `sp_LoadDimensionTables` procedure.

## Reference Dimensions

### DimProductType

Categorizes the different beverage products the machines can produce.

| Column | Data Type | Description |
|--------|-----------|-------------|
| product_type_id | int | Primary key |
| prod_type | nvarchar(255) | Product type name |
| description | nvarchar(255) | Detailed description |

#### Standard Values

| ID | Product Type | Description |
|----|-------------|-------------|
| 0 | None | Aucun produit |
| 1 | Ristretto | Standard Ristretto |
| 2 | Expresso | Standard Expresso |
| 3 | Coffee | Standard Coffee |
| 4 | Filter Coffee | Filtered Coffee |
| 5 | Americano | Standard Americano |
| 6 | Coffee Pot | Standard Coffee Pot |
| 7 | Filter coffee Pot | Filtered Coffee Pot |
| 8 | Hot Water | Hot Water Only |
| 9 | Manual Steam | Steam |
| 10 | Auto Steam | Auto Steam |
| 11 | Everfoam | Everfoam |
| 12 | Milk Coffee | Standard Coffee with milk |
| 13 | Cappuccino | Cappuccino |
| 14 | Expresso Macchiatto | Expresso Macchiatto |
| 15 | Latte Macchiatto | Latte Macchiatto |
| 16 | Milk | Hot Milk |
| 17 | Milk Foam | Milk Foam |
| 18 | Powder | Powder |
| 19 | White Americano | White Americano |
| 20 | Max | Max |
| 255 | Undefined | Undefined |

### DimPowderStatus

Status codes for powder cleaning operations.

| Column | Data Type | Description |
|--------|-----------|-------------|
| powder_status_id | int | Primary key |
| status_name | nvarchar(255) | Status description |

#### Standard Values

| ID | Status Name |
|----|------------|
| 0 | Undefined |
| 1 | Not Necessary |
| 2 | Mixer Cleaned |
| 3 | Without Mixer |
| 4 | Max |

### DimTabsStatus

Status codes for cleaning tabs used in machines.

| Column | Data Type | Description |
|--------|-----------|-------------|
| tabs_status_id | int | Primary key |
| tabs_status_name | nvarchar(255) | Status description |

#### Standard Values

| ID | Status Name |
|----|------------|
| 0 | No |
| 1 | Yes |
| 2 | Undefined |
| 3 | Error |
| 4 | Unknown |
| 5 | Not Necessary |
| 6 | Cycle Error |
| 7 | Max |

### DimDetergentStatus

Status codes for detergent used in cleaning operations.

| Column | Data Type | Description |
|--------|-----------|-------------|
| detergent_status_id | int | Primary key |
| detergent_status_name | nvarchar(255) | Status description |

#### Standard Values

| ID | Status Name |
|----|------------|
| 0 | Not defined |
| 1 | No |
| 2 | Yes |
| 3 | Error |
| 4 | Unknown |
| 5 | Not Necessary |
| 6 | Cycle Abort |
| 7 | Cycle Warning |
| 8 | Detergent Warning |
| 9 | Max |

### DimRinseType

Types of rinsing operations performed by machines.

| Column | Data Type | Description |
|--------|-----------|-------------|
| rinse_type_id | int | Primary key |
| rinse_type_name | nvarchar(255) | Rinse type description |

#### Standard Values

| ID | Rinse Type Name |
|----|----------------|
| 0 | Initial Reboot |
| 1 | Initial Wake Up |
| 2 | Warm Left |
| 3 | Warm Right |
| 4 | After Clean |
| 5 | Flow Rate |
| 6 | Requested ETC |
| 7 | Max |
| 255 | Undefined |

### DimRinseStatus

Status codes for rinse operations.

| Column | Data Type | Description |
|--------|-----------|-------------|
| rinse_status_id | int | Primary key |
| rinse_status_name | nvarchar(255) | Status description |

#### Standard Values

| ID | Status Name |
|----|------------|
| 0 | Undefined |
| 1 | Unknown |
| 2 | Too Low |
| 3 | Too High |
| 4 | Nozzel 05 |
| 5 | Nozzel 07 |
| 6 | System OK |

### DimNozzleStatus

Status codes for nozzle components.

| Column | Data Type | Description |
|--------|-----------|-------------|
| nozzle_status_id | int | Primary key |
| nozzle_status_name | nvarchar(255) | Status description |

#### Standard Values

| ID | Status Name |
|----|------------|
| 0 | Undefined |
| 1 | Unknown |
| 2 | Too Low |
| 3 | Too High |
| 4 | Nuzzle 05 |
| 5 | Nuzle 07 |
| 6 | System OK |
| 255 | Null |

### DimBeanHopper

Different bean hoppers available on machines.

| Column | Data Type | Description |
|--------|-----------|-------------|
| bean_hopper_id | int | Primary key |
| bean_hopper_name | nvarchar(255) | Bean hopper description |

#### Standard Values

| ID | Bean Hopper Name |
|----|-----------------|
| 0 | Front Right |
| 1 | Rear Left |
| 2 | Mix |
| 3 | Powder chute |
| 255 | None |

### DimOutletSide

Left/right side designations for dual outlet machines.

| Column | Data Type | Description |
|--------|-----------|-------------|
| outlet_side_id | int | Primary key |
| outlet_side_name | nvarchar(255) | Side description (Left/Right) |

#### Standard Values

| ID | Outlet Side Name |
|----|-----------------|
| 0 | Left |
| 1 | Right |

### DimStopped

Status codes for stopped production runs.

| Column | Data Type | Description |
|--------|-----------|-------------|
| stopped_id | int | Primary key |
| stopped_name | nvarchar(255) | Stopped reason description |

#### Standard Values

| ID | Stopped Name |
|----|-------------|
| 0 | Finished |
| 1 | Stopped |
| 2 | Machine Abort |
| 3 | User Abort |

## Best Practices for Dimension Management

### 1. Slowly Changing Dimensions (SCD)

While the current implementation doesn't specifically manage slowly changing dimensions, they could be implemented for dimensions like DimMachine:

```sql
-- Example of Type 2 SCD for DimMachine
ALTER TABLE DimMachine ADD
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL;
```

### 2. Default Unknown Member

Each dimension should include a default "Unknown" or "Not Applicable" member (typically ID 0 or -1) to handle cases where the source data doesn't provide a valid value:

```sql
-- Example of ensuring Unknown members exist
IF NOT EXISTS (SELECT 1 FROM DimProductType WHERE product_type_id = 0)
    INSERT INTO DimProductType (product_type_id, prod_type, description)
    VALUES (0, 'Unknown', 'Default value for unknown product types');
```

### 3. Junk Dimensions

For dimensions with low cardinality that often appear together (e.g., flags and indicators), consider creating junk dimensions:

```sql
-- Example of a junk dimension for machine status flags
CREATE TABLE DimMachineStatus (
    status_id INT PRIMARY KEY,
    milk_pump_error_left BIT,
    milk_pump_error_right BIT,
    status_description NVARCHAR(255)
);
```

### 4. Role-Playing Dimensions

The DimTime dimension plays multiple roles in FactMachineCleaning (start time and end time). This is handled through separate foreign key relationships:

```sql
-- Example of role-playing dimension relationships
ALTER TABLE FactMachineCleaning 
    ADD CONSTRAINT FK_FactMachineCleaning_DimTime_Start 
    FOREIGN KEY (time_start_id) REFERENCES DimTime(time_id);

ALTER TABLE FactMachineCleaning 
    ADD CONSTRAINT FK_FactMachineCleaning_DimTime_End 
    FOREIGN KEY (time_end_id) REFERENCES DimTime(time_id);
```

### 5. Dimension Hierarchies

Hierarchical relationships in dimensions can be explicitly modeled. For example, in a more detailed DimDate:

```sql
-- Example of dimension hierarchy attributes
ALTER TABLE DimDate ADD
    week_of_year INT,
    month_name NVARCHAR(10),
    quarter_name NVARCHAR(2),
    year_month NVARCHAR(7); -- Format: YYYY-MM
```

## Dimension Maintenance

Dimensions should be maintained through regular procedures:

1. **Daily Update**: Run `sp_EnsureDimensionCompleteness` to add any missing values
2. **Weekly Review**: Check for any new dimension values that need better descriptions
3. **Monthly Update**: Update any descriptive information in dimensions that may have changed
4. **Yearly Refresh**: Add new dates to DimDate for the upcoming year

By maintaining clean, complete, and descriptive dimension tables, the data warehouse provides a rich context for analyzing the fact data.