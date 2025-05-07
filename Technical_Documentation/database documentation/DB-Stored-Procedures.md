# Stored Procedures Documentation

This document contains the SQL scripts for all stored procedures used in the machine data import system.

## 1. Cleaning Logs Import Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Updated Cleaning Logs Import Procedure with Fixed Parsing
ALTER PROCEDURE [dbo].[sp_ImportCleaningLogs]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Cleaning\current\Silver_Cleaning.dat';
    
    BEGIN TRY
        -- Read the file directly using xp_cmdshell
        CREATE TABLE #RawData (line_data NVARCHAR(MAX));
        
        -- Import the file using xp_cmdshell to ensure we get all lines
        INSERT INTO #RawData
        EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Cleaning\current\Silver_Cleaning.dat"';
        
        -- Remove NULL rows and the header row
        DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempCleaningLogs (
            machine_id INT NULL,
            timestamp_start DATETIME NULL,
            timestamp_end DATETIME NULL,
            powder_clean_status SMALLINT NULL,
            tabs_status_left SMALLINT NULL,
            tabs_status_right SMALLINT NULL,
            detergent_status_left SMALLINT NULL,
            detergent_status_right SMALLINT NULL,
            milk_pump_error_left TINYINT NULL,
            milk_pump_error_right TINYINT NULL,
            milk_temp_left_1 SMALLINT NULL,
            milk_temp_left_2 SMALLINT NULL,
            milk_temp_right_1 SMALLINT NULL,
            milk_temp_right_2 SMALLINT NULL,
            milk_rpm_left_1 SMALLINT NULL,
            milk_rpm_left_2 SMALLINT NULL,
            milk_rpm_right_1 SMALLINT NULL,
            milk_rpm_right_2 SMALLINT NULL,
            milk_clean_temp_left_1 SMALLINT NULL,
            milk_clean_temp_left_2 SMALLINT NULL,
            milk_clean_temp_right_1 SMALLINT NULL,
            milk_clean_temp_right_2 SMALLINT NULL,
            milk_clean_rpm_left_1 SMALLINT NULL,
            milk_clean_rpm_left_2 SMALLINT NULL,
            milk_clean_rpm_right_1 SMALLINT NULL,
            milk_clean_rpm_right_2 SMALLINT NULL,
            milk_seq_cycle_left_1 SMALLINT NULL,
            milk_seq_cycle_left_2 SMALLINT NULL,
            milk_seq_cycle_right_1 SMALLINT NULL,
            milk_seq_cycle_right_2 SMALLINT NULL
        );
        
        -- Parse data using a cursor-based approach with string manipulation
        DECLARE @Delimiter CHAR(1) = ';';
        
        -- Process each row
        DECLARE @Line NVARCHAR(MAX);
        DECLARE @Pos INT;
        DECLARE @NextPos INT;
        DECLARE @FieldNum INT;
        DECLARE @Field NVARCHAR(MAX);
        
        -- Declare variables for each field
        DECLARE @MachineId INT;
        DECLARE @TimestampStart DATETIME;
        DECLARE @TimestampEnd DATETIME;
        DECLARE @PowderCleanStatus SMALLINT;
        DECLARE @TabsStatusLeft SMALLINT;
        DECLARE @TabsStatusRight SMALLINT;
        DECLARE @DetergentStatusLeft SMALLINT;
        DECLARE @DetergentStatusRight SMALLINT;
        DECLARE @MilkPumpErrorLeft TINYINT;
        DECLARE @MilkPumpErrorRight TINYINT;
        DECLARE @MilkTempLeft1 SMALLINT;
        DECLARE @MilkTempLeft2 SMALLINT;
        DECLARE @MilkTempRight1 SMALLINT;
        DECLARE @MilkTempRight2 SMALLINT;
        DECLARE @MilkRpmLeft1 SMALLINT;
        DECLARE @MilkRpmLeft2 SMALLINT;
        DECLARE @MilkRpmRight1 SMALLINT;
        DECLARE @MilkRpmRight2 SMALLINT;
        DECLARE @MilkCleanTempLeft1 SMALLINT;
        DECLARE @MilkCleanTempLeft2 SMALLINT;
        DECLARE @MilkCleanTempRight1 SMALLINT;
        DECLARE @MilkCleanTempRight2 SMALLINT;
        DECLARE @MilkCleanRpmLeft1 SMALLINT;
        DECLARE @MilkCleanRpmLeft2 SMALLINT;
        DECLARE @MilkCleanRpmRight1 SMALLINT;
        DECLARE @MilkCleanRpmRight2 SMALLINT;
        DECLARE @MilkSeqCycleLeft1 SMALLINT;
        DECLARE @MilkSeqCycleLeft2 SMALLINT;
        DECLARE @MilkSeqCycleRight1 SMALLINT;
        DECLARE @MilkSeqCycleRight2 SMALLINT;
        
        DECLARE lineCursor CURSOR FOR SELECT line_data FROM #RawData;
        OPEN lineCursor;
        FETCH NEXT FROM lineCursor INTO @Line;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Pos = 1;
            SET @FieldNum = 1;
            
            -- Initialize all variables to NULL
            SET @MachineId = NULL;
            SET @TimestampStart = NULL;
            SET @TimestampEnd = NULL;
            SET @PowderCleanStatus = NULL;
            SET @TabsStatusLeft = NULL;
            SET @TabsStatusRight = NULL;
            SET @DetergentStatusLeft = NULL;
            SET @DetergentStatusRight = NULL;
            SET @MilkPumpErrorLeft = NULL;
            SET @MilkPumpErrorRight = NULL;
            SET @MilkTempLeft1 = NULL;
            SET @MilkTempLeft2 = NULL;
            SET @MilkTempRight1 = NULL;
            SET @MilkTempRight2 = NULL;
            SET @MilkRpmLeft1 = NULL;
            SET @MilkRpmLeft2 = NULL;
            SET @MilkRpmRight1 = NULL;
            SET @MilkRpmRight2 = NULL;
            SET @MilkCleanTempLeft1 = NULL;
            SET @MilkCleanTempLeft2 = NULL;
            SET @MilkCleanTempRight1 = NULL;
            SET @MilkCleanTempRight2 = NULL;
            SET @MilkCleanRpmLeft1 = NULL;
            SET @MilkCleanRpmLeft2 = NULL;
            SET @MilkCleanRpmRight1 = NULL;
            SET @MilkCleanRpmRight2 = NULL;
            SET @MilkSeqCycleLeft1 = NULL;
            SET @MilkSeqCycleLeft2 = NULL;
            SET @MilkSeqCycleRight1 = NULL;
            SET @MilkSeqCycleRight2 = NULL;
            
            WHILE @Pos <= LEN(@Line) AND @FieldNum <= 30
            BEGIN
                SET @NextPos = CHARINDEX(@Delimiter, @Line + @Delimiter, @Pos);
                IF @NextPos = 0 SET @NextPos = LEN(@Line) + 1;
                
                SET @Field = SUBSTRING(@Line, @Pos, @NextPos - @Pos);
                
                -- Assign the field to the correct variable based on its position
                IF @FieldNum = 1 SET @MachineId = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 2 SET @TimestampStart = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 3 SET @TimestampEnd = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 4 SET @PowderCleanStatus = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 5 SET @TabsStatusLeft = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 6 SET @TabsStatusRight = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 7 SET @DetergentStatusLeft = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 8 SET @DetergentStatusRight = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 9 SET @MilkPumpErrorLeft = TRY_CAST(@Field AS TINYINT);
                ELSE IF @FieldNum = 10 SET @MilkPumpErrorRight = TRY_CAST(@Field AS TINYINT);
                ELSE IF @FieldNum = 11 SET @MilkTempLeft1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 12 SET @MilkTempLeft2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 13 SET @MilkTempRight1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 14 SET @MilkTempRight2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 15 SET @MilkRpmLeft1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 16 SET @MilkRpmLeft2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 17 SET @MilkRpmRight1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 18 SET @MilkRpmRight2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 19 SET @MilkCleanTempLeft1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 20 SET @MilkCleanTempLeft2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 21 SET @MilkCleanTempRight1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 22 SET @MilkCleanTempRight2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 23 SET @MilkCleanRpmLeft1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 24 SET @MilkCleanRpmLeft2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 25 SET @MilkCleanRpmRight1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 26 SET @MilkCleanRpmRight2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 27 SET @MilkSeqCycleLeft1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 28 SET @MilkSeqCycleLeft2 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 29 SET @MilkSeqCycleRight1 = TRY_CAST(@Field AS SMALLINT);
                ELSE IF @FieldNum = 30 SET @MilkSeqCycleRight2 = TRY_CAST(@Field AS SMALLINT);
                
                -- Move to the next field
                SET @Pos = @NextPos + 1;
                SET @FieldNum = @FieldNum + 1;
            END
            
            -- Insert the parsed data into the temp table
            INSERT INTO #TempCleaningLogs (
                machine_id, timestamp_start, timestamp_end, powder_clean_status, 
                tabs_status_left, tabs_status_right, detergent_status_left, detergent_status_right,
                milk_pump_error_left, milk_pump_error_right, milk_temp_left_1, milk_temp_left_2,
                milk_temp_right_1, milk_temp_right_2, milk_rpm_left_1, milk_rpm_left_2,
                milk_rpm_right_1, milk_rpm_right_2, milk_clean_temp_left_1, milk_clean_temp_left_2,
                milk_clean_temp_right_1, milk_clean_temp_right_2, milk_clean_rpm_left_1, milk_clean_rpm_left_2,
                milk_clean_rpm_right_1, milk_clean_rpm_right_2, milk_seq_cycle_left_1, milk_seq_cycle_left_2,
                milk_seq_cycle_right_1, milk_seq_cycle_right_2
            )
            VALUES (
                @MachineId, @TimestampStart, @TimestampEnd, @PowderCleanStatus, 
                @TabsStatusLeft, @TabsStatusRight, @DetergentStatusLeft, @DetergentStatusRight,
                @MilkPumpErrorLeft, @MilkPumpErrorRight, @MilkTempLeft1, @MilkTempLeft2,
                @MilkTempRight1, @MilkTempRight2, @MilkRpmLeft1, @MilkRpmLeft2,
                @MilkRpmRight1, @MilkRpmRight2, @MilkCleanTempLeft1, @MilkCleanTempLeft2,
                @MilkCleanTempRight1, @MilkCleanTempRight2, @MilkCleanRpmLeft1, @MilkCleanRpmLeft2,
                @MilkCleanRpmRight1, @MilkCleanRpmRight2, @MilkSeqCycleLeft1, @MilkSeqCycleLeft2,
                @MilkSeqCycleRight1, @MilkSeqCycleRight2
            );
            
            FETCH NEXT FROM lineCursor INTO @Line;
        END
        
        CLOSE lineCursor;
        DEALLOCATE lineCursor;
        
        -- Auto-add missing machine_ids to the machine_names table
        INSERT INTO machine_names (machine_id, name)
        SELECT DISTINCT t.machine_id, 'Machine ' + CAST(t.machine_id AS NVARCHAR(10))
        FROM #TempCleaningLogs t
        LEFT JOIN machine_names m ON t.machine_id = m.machine_id
        WHERE m.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many machines were added
        PRINT 'New machines added: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Log the import operation
        DECLARE @RowsImported INT;
        SELECT @RowsImported = COUNT(*) FROM #TempCleaningLogs;
        
        PRINT 'Successfully imported data from ' + @FilePath;
        PRINT 'Rows imported: ' + CAST(@RowsImported AS VARCHAR(20));
        
        -- Insert only new records (now all machine_ids should exist)
        INSERT INTO cleaning_logs (
            machine_id, timestamp_start, timestamp_end, powder_clean_status, 
            tabs_status_left, tabs_status_right, detergent_status_left, detergent_status_right,
            milk_pump_error_left, milk_pump_error_right, milk_temp_left_1, milk_temp_left_2,
            milk_temp_right_1, milk_temp_right_2, milk_rpm_left_1, milk_rpm_left_2,
            milk_rpm_right_1, milk_rpm_right_2, milk_clean_temp_left_1, milk_clean_temp_left_2,
            milk_clean_temp_right_1, milk_clean_temp_right_2, milk_clean_rpm_left_1, milk_clean_rpm_left_2,
            milk_clean_rpm_right_1, milk_clean_rpm_right_2, milk_seq_cycle_left_1, milk_seq_cycle_left_2,
            milk_seq_cycle_right_1, milk_seq_cycle_right_2
        )
        SELECT 
            t.machine_id, t.timestamp_start, t.timestamp_end, t.powder_clean_status, 
            t.tabs_status_left, t.tabs_status_right, t.detergent_status_left, t.detergent_status_right,
            t.milk_pump_error_left, t.milk_pump_error_right, t.milk_temp_left_1, t.milk_temp_left_2,
            t.milk_temp_right_1, t.milk_temp_right_2, t.milk_rpm_left_1, t.milk_rpm_left_2,
            t.milk_rpm_right_1, t.milk_rpm_right_2, t.milk_clean_temp_left_1, t.milk_clean_temp_left_2,
            t.milk_clean_temp_right_1, t.milk_clean_temp_right_2, t.milk_clean_rpm_left_1, t.milk_clean_rpm_left_2,
            t.milk_clean_rpm_right_1, t.milk_clean_rpm_right_2, t.milk_seq_cycle_left_1, t.milk_seq_cycle_left_2,
            t.milk_seq_cycle_right_1, t.milk_seq_cycle_right_2
        FROM #TempCleaningLogs t
        LEFT JOIN cleaning_logs c ON 
            t.machine_id = c.machine_id AND
            t.timestamp_start = c.timestamp_start AND
            t.timestamp_end = c.timestamp_end
        WHERE c.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many new records were inserted
        PRINT 'New records inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Drop the temporary tables
        DROP TABLE #RawData;
        DROP TABLE #TempCleaningLogs;
        
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
        EXEC sp_executesql N'RAISERROR(''Cleaning Import Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
        
        -- Clean up temporary tables and cursor if they exist
        IF CURSOR_STATUS('local', 'lineCursor') = 1
        BEGIN
            CLOSE lineCursor;
            DEALLOCATE lineCursor;
        END
        
        IF OBJECT_ID('tempdb..#RawData') IS NOT NULL
            DROP TABLE #RawData;
            
        IF OBJECT_ID('tempdb..#TempCleaningLogs') IS NOT NULL
            DROP TABLE #TempCleaningLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```
## 2. Rinse Logs Import Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Updated Rinse Logs Import Procedure with Fixed Parsing
ALTER PROCEDURE [dbo].[sp_ImportRinseLogs]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Rinse\current\Silver_Rinse.dat';
    
    BEGIN TRY
        -- Read the file directly using xp_cmdshell
        CREATE TABLE #RawData (line_data NVARCHAR(MAX));
        
        -- Import the file using xp_cmdshell to ensure we get all lines
        INSERT INTO #RawData
        EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Rinse\current\Silver_Rinse.dat"';
        
        -- Remove NULL rows and the header row
        DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempRinseLogs (
            machine_id INT NULL,
            timestamp DATETIME NULL,
            rinse_type INT NULL,
            flow_rate_left INT NULL,
            flow_rate_right INT NULL,
            status_left INT NULL,
            status_right INT NULL,
            pump_pressure INT NULL,
            nozzle_flow_rate_left INT NULL,
            nozzle_flow_rate_right INT NULL,
            nozzle_status_left INT NULL,
            nozzle_status_right INT NULL
        );
        
        -- Parse data using a cursor-based approach with string manipulation
        DECLARE @Delimiter CHAR(1) = ';';
        
        -- Process each row
        DECLARE @Line NVARCHAR(MAX);
        DECLARE @Pos INT;
        DECLARE @NextPos INT;
        DECLARE @FieldNum INT;
        DECLARE @Field NVARCHAR(MAX);
        
        -- Declare variables for each field
        DECLARE @MachineId INT;
        DECLARE @Timestamp DATETIME;
        DECLARE @RinseType INT;
        DECLARE @FlowRateLeft INT;
        DECLARE @FlowRateRight INT;
        DECLARE @StatusLeft INT;
        DECLARE @StatusRight INT;
        DECLARE @PumpPressure INT;
        DECLARE @NozzleFlowRateLeft INT;
        DECLARE @NozzleFlowRateRight INT;
        DECLARE @NozzleStatusLeft INT;
        DECLARE @NozzleStatusRight INT;
        
        DECLARE lineCursor CURSOR FOR SELECT line_data FROM #RawData;
        OPEN lineCursor;
        FETCH NEXT FROM lineCursor INTO @Line;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Pos = 1;
            SET @FieldNum = 1;
            
            -- Initialize all variables to NULL
            SET @MachineId = NULL;
            SET @Timestamp = NULL;
            SET @RinseType = NULL;
            SET @FlowRateLeft = NULL;
            SET @FlowRateRight = NULL;
            SET @StatusLeft = NULL;
            SET @StatusRight = NULL;
            SET @PumpPressure = NULL;
            SET @NozzleFlowRateLeft = NULL;
            SET @NozzleFlowRateRight = NULL;
            SET @NozzleStatusLeft = NULL;
            SET @NozzleStatusRight = NULL;
            
            WHILE @Pos <= LEN(@Line) AND @FieldNum <= 12
            BEGIN
                SET @NextPos = CHARINDEX(@Delimiter, @Line + @Delimiter, @Pos);
                IF @NextPos = 0 SET @NextPos = LEN(@Line) + 1;
                
                SET @Field = SUBSTRING(@Line, @Pos, @NextPos - @Pos);
                
                -- Assign the field to the correct variable based on its position
                IF @FieldNum = 1 SET @MachineId = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 2 SET @Timestamp = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 3 SET @RinseType = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 4 SET @FlowRateLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 5 SET @FlowRateRight = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 6 SET @StatusLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 7 SET @StatusRight = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 8 SET @PumpPressure = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 9 SET @NozzleFlowRateLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 10 SET @NozzleFlowRateRight = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 11 SET @NozzleStatusLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 12 SET @NozzleStatusRight = TRY_CAST(@Field AS INT);
                
                -- Move to the next field
                SET @Pos = @NextPos + 1;
                SET @FieldNum = @FieldNum + 1;
            END
            
            -- Insert the parsed data into the temp table
            INSERT INTO #TempRinseLogs (
                machine_id, timestamp, rinse_type, flow_rate_left, flow_rate_right,
                status_left, status_right, pump_pressure, nozzle_flow_rate_left,
                nozzle_flow_rate_right, nozzle_status_left, nozzle_status_right
            )
            VALUES (
                @MachineId, @Timestamp, @RinseType, @FlowRateLeft, @FlowRateRight,
                @StatusLeft, @StatusRight, @PumpPressure, @NozzleFlowRateLeft,
                @NozzleFlowRateRight, @NozzleStatusLeft, @NozzleStatusRight
            );
            
            FETCH NEXT FROM lineCursor INTO @Line;
        END
        
        CLOSE lineCursor;
        DEALLOCATE lineCursor;
        
        -- Auto-add missing machine_ids to the machine_names table
        INSERT INTO machine_names (machine_id, name)
        SELECT DISTINCT t.machine_id, 'Machine ' + CAST(t.machine_id AS NVARCHAR(10))
        FROM #TempRinseLogs t
        LEFT JOIN machine_names m ON t.machine_id = m.machine_id
        WHERE m.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many machines were added
        PRINT 'New machines added: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Log the import operation
        DECLARE @RowsImported INT;
        SELECT @RowsImported = COUNT(*) FROM #TempRinseLogs;
        
        PRINT 'Successfully imported data from ' + @FilePath;
        PRINT 'Rows imported: ' + CAST(@RowsImported AS VARCHAR(20));
        
        -- Truncate the table if it already exists
        TRUNCATE TABLE rinse_logs;
        
        -- Insert all records with valid machine_id
        INSERT INTO rinse_logs (
            machine_id, timestamp, rinse_type, flow_rate_left, flow_rate_right,
            status_left, status_right, pump_pressure, nozzle_flow_rate_left,
            nozzle_flow_rate_right, nozzle_status_left, nozzle_status_right
        )
        SELECT 
            t.machine_id, t.timestamp, t.rinse_type, t.flow_rate_left, t.flow_rate_right,
            t.status_left, t.status_right, t.pump_pressure, t.nozzle_flow_rate_left,
            t.nozzle_flow_rate_right, t.nozzle_status_left, t.nozzle_status_right
        FROM #TempRinseLogs t
        WHERE t.machine_id IS NOT NULL;
        
        -- Log how many new records were inserted
        PRINT 'New records inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Drop the temporary tables
        DROP TABLE #RawData;
        DROP TABLE #TempRinseLogs;
        
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
        EXEC sp_executesql N'RAISERROR(''Rinse Import Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
        
        -- Clean up temporary tables and cursor if they exist
        IF CURSOR_STATUS('local', 'lineCursor') = 1
        BEGIN
            CLOSE lineCursor;
            DEALLOCATE lineCursor;
        END
        
        IF OBJECT_ID('tempdb..#RawData') IS NOT NULL
            DROP TABLE #RawData;
            
        IF OBJECT_ID('tempdb..#TempRinseLogs') IS NOT NULL
            DROP TABLE #TempRinseLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

## 3. Product Logs Import Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Updated Product Logs Import Procedure with Fixed Parsing
ALTER PROCEDURE [dbo].[sp_ImportProductLogs]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Product\current\Silver_Product.dat';
    
    BEGIN TRY
        -- Read the file directly using xp_cmdshell
        CREATE TABLE #RawData (line_data NVARCHAR(MAX));
        
        -- Import the file using xp_cmdshell to ensure we get all lines
        INSERT INTO #RawData
        EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Product\current\Silver_Product.dat"';
        
        -- Remove NULL rows and the header row
        DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempProductLogs (
            machine_id INT NULL,
            timestamp DATETIME NULL,
            press_before FLOAT NULL,
            press_after FLOAT NULL,
            press_final FLOAT NULL,
            grind_time FLOAT NULL,
            ext_time FLOAT NULL,
            water_qnty INT NULL,
            water_temp INT NULL,
            prod_type INT NULL,
            double_prod FLOAT NULL,
            bean_hopper INT NULL,
            outlet_side FLOAT NULL,
            stopped FLOAT NULL,
            milk_temp INT NULL,
            steam_pressure FLOAT NULL,
            grind_adjust_left INT NULL,
            grind_adjust_right INT NULL,
            milk_time FLOAT NULL,
            boiler_temp INT NULL
        );
        
        -- Parse data using a cursor-based approach with string manipulation
        DECLARE @Delimiter CHAR(1) = ';';
        
        -- Process each row
        DECLARE @Line NVARCHAR(MAX);
        DECLARE @Pos INT;
        DECLARE @NextPos INT;
        DECLARE @FieldNum INT;
        DECLARE @Field NVARCHAR(MAX);
        
        -- Declare variables for each field
        DECLARE @MachineId INT;
        DECLARE @Timestamp DATETIME;
        DECLARE @PressBefore FLOAT;
        DECLARE @PressAfter FLOAT;
        DECLARE @PressFinal FLOAT;
        DECLARE @GrindTime FLOAT;
        DECLARE @ExtTime FLOAT;
        DECLARE @WaterQnty INT;
        DECLARE @WaterTemp INT;
        DECLARE @ProdType INT;
        DECLARE @DoubleProd FLOAT;
        DECLARE @BeanHopper INT;
        DECLARE @OutletSide FLOAT;
        DECLARE @Stopped FLOAT;
        DECLARE @MilkTemp INT;
        DECLARE @SteamPressure FLOAT;
        DECLARE @GrindAdjustLeft INT;
        DECLARE @GrindAdjustRight INT;
        DECLARE @MilkTime FLOAT;
        DECLARE @BoilerTemp INT;
        
        DECLARE lineCursor CURSOR FOR SELECT line_data FROM #RawData;
        OPEN lineCursor;
        FETCH NEXT FROM lineCursor INTO @Line;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Pos = 1;
            SET @FieldNum = 1;
            
            -- Initialize all variables to NULL
            SET @MachineId = NULL;
            SET @Timestamp = NULL;
            SET @PressBefore = NULL;
            SET @PressAfter = NULL;
            SET @PressFinal = NULL;
            SET @GrindTime = NULL;
            SET @ExtTime = NULL;
            SET @WaterQnty = NULL;
            SET @WaterTemp = NULL;
            SET @ProdType = NULL;
            SET @DoubleProd = NULL;
            SET @BeanHopper = NULL;
            SET @OutletSide = NULL;
            SET @Stopped = NULL;
            SET @MilkTemp = NULL;
            SET @SteamPressure = NULL;
            SET @GrindAdjustLeft = NULL;
            SET @GrindAdjustRight = NULL;
            SET @MilkTime = NULL;
            SET @BoilerTemp = NULL;
            
            WHILE @Pos <= LEN(@Line) AND @FieldNum <= 20
            BEGIN
                SET @NextPos = CHARINDEX(@Delimiter, @Line + @Delimiter, @Pos);
                IF @NextPos = 0 SET @NextPos = LEN(@Line) + 1;
                
                SET @Field = SUBSTRING(@Line, @Pos, @NextPos - @Pos);
                
                -- Assign the field to the correct variable based on its position
                IF @FieldNum = 1 SET @MachineId = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 2 SET @Timestamp = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 3 SET @PressBefore = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 4 SET @PressAfter = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 5 SET @PressFinal = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 6 SET @GrindTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 7 SET @ExtTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 8 SET @WaterQnty = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 9 SET @WaterTemp = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 10 SET @ProdType = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 11 SET @DoubleProd = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 12 SET @BeanHopper = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 13 SET @OutletSide = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 14 SET @Stopped = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 15 SET @MilkTemp = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 16 SET @SteamPressure = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 17 SET @GrindAdjustLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 18 SET @GrindAdjustRight = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 19 SET @MilkTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 20 SET @BoilerTemp = TRY_CAST(@Field AS INT);
                
                -- Move to the next field
                SET @Pos = @NextPos + 1;
                SET @FieldNum = @FieldNum + 1;
            END
            
            -- Insert the parsed data into the temp table
            INSERT INTO #TempProductLogs (
                machine_id, timestamp, press_before, press_after, press_final, 
                grind_time, ext_time, water_qnty, water_temp, prod_type, 
                double_prod, bean_hopper, outlet_side, stopped, milk_temp, 
                steam_pressure, grind_adjust_left, grind_adjust_right, milk_time, boiler_temp
            )
            VALUES (
                @MachineId, @Timestamp, @PressBefore, @PressAfter, @PressFinal,
                @GrindTime, @ExtTime, @WaterQnty, @WaterTemp, @ProdType,
                @DoubleProd, @BeanHopper, @OutletSide, @Stopped, @MilkTemp,
                @SteamPressure, @GrindAdjustLeft, @GrindAdjustRight, @MilkTime, @BoilerTemp
            );
            
            FETCH NEXT FROM lineCursor INTO @Line;
        END
        
        CLOSE lineCursor;
        DEALLOCATE lineCursor;
        
        -- Auto-add missing machine_ids to the machine_names table
        INSERT INTO machine_names (machine_id, name)
        SELECT DISTINCT t.machine_id, 'Machine ' + CAST(t.machine_id AS NVARCHAR(10))
        FROM #TempProductLogs t
        LEFT JOIN machine_names m ON t.machine_id = m.machine_id
        WHERE m.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many machines were added
        PRINT 'New machines added: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Log the import operation
        DECLARE @RowsImported INT;
        SELECT @RowsImported = COUNT(*) FROM #TempProductLogs;
        
        PRINT 'Successfully imported data from ' + @FilePath;
        PRINT 'Rows imported: ' + CAST(@RowsImported AS VARCHAR(20));
        
        -- Insert only new records (now all machine_ids should exist)
        INSERT INTO product_logs (
            machine_id, timestamp, press_before, press_after, press_final, 
            grind_time, ext_time, water_qnty, water_temp, prod_type, 
            double_prod, bean_hopper, outlet_side, stopped, milk_temp, 
            steam_pressure, grind_adjust_left, grind_adjust_right, milk_time, boiler_temp
        )
        SELECT 
            t.machine_id, t.timestamp, t.press_before, t.press_after, t.press_final, 
            t.grind_time, t.ext_time, t.water_qnty, t.water_temp, t.prod_type, 
            t.double_prod, t.bean_hopper, t.outlet_side, t.stopped, t.milk_temp, 
            t.steam_pressure, t.grind_adjust_left, t.grind_adjust_right, t.milk_time, t.boiler_temp
        FROM #TempProductLogs t
        LEFT JOIN product_logs p ON 
            t.machine_id = p.machine_id AND
            t.timestamp = p.timestamp AND
            t.prod_type = p.prod_type AND
            t.water_qnty = p.water_qnty
        WHERE p.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many new records were inserted
        PRINT 'New records inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Drop the temporary tables
        DROP TABLE #RawData;
        DROP TABLE #TempProductLogs;
        
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
        EXEC sp_executesql N'RAISERROR(''Product Import Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
        
        -- Clean up temporary tables and cursor if they exist
        IF CURSOR_STATUS('local', 'lineCursor') = 1
        BEGIN
            CLOSE lineCursor;
            DEALLOCATE lineCursor;
        END
        
        IF OBJECT_ID('tempdb..#RawData') IS NOT NULL
            DROP TABLE #RawData;
            
        IF OBJECT_ID('tempdb..#TempProductLogs') IS NOT NULL
            DROP TABLE #TempProductLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

## 4. Info Logs Import Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Updated Info Logs Import Procedure with Fixed Parsing
ALTER PROCEDURE [dbo].[sp_ImportInfoLogs]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Info\current\Silver_Info.dat';
    
    BEGIN TRY
        -- Read the file directly using xp_cmdshell
        CREATE TABLE #RawData (line_data NVARCHAR(MAX));
        
        -- Import the file using xp_cmdshell to ensure we get all lines
        INSERT INTO #RawData
        EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Info\current\Silver_Info.dat"';
        
        -- Remove NULL rows and the header row
        DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempInfoLogs (
            machine_id INT NULL,
            timestamp DATETIME NULL,
            number NVARCHAR(50) NULL,
            typography NVARCHAR(10) NULL,
            type_number FLOAT NULL
        );
        
        -- Parse data using a cursor-based approach with string manipulation
        DECLARE @Delimiter CHAR(1) = ';';
        
        -- Process each row
        DECLARE @Line NVARCHAR(MAX);
        DECLARE @Pos INT;
        DECLARE @NextPos INT;
        DECLARE @FieldNum INT;
        DECLARE @Field NVARCHAR(MAX);
        
        -- Declare variables for each field
        DECLARE @MachineId INT;
        DECLARE @Timestamp DATETIME;
        DECLARE @Number NVARCHAR(50);
        DECLARE @Typography NVARCHAR(10);
        DECLARE @TypeNumber FLOAT;
        
        DECLARE lineCursor CURSOR FOR SELECT line_data FROM #RawData;
        OPEN lineCursor;
        FETCH NEXT FROM lineCursor INTO @Line;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @Pos = 1;
            SET @FieldNum = 1;
            
            -- Initialize all variables to NULL
            SET @MachineId = NULL;
            SET @Timestamp = NULL;
            SET @Number = NULL;
            SET @Typography = NULL;
            SET @TypeNumber = NULL;
            
            WHILE @Pos <= LEN(@Line) AND @FieldNum <= 5
            BEGIN
                SET @NextPos = CHARINDEX(@Delimiter, @Line + @Delimiter, @Pos);
                IF @NextPos = 0 SET @NextPos = LEN(@Line) + 1;
                
                SET @Field = SUBSTRING(@Line, @Pos, @NextPos - @Pos);
                
                -- Assign the field to the correct variable based on its position
                IF @FieldNum = 1 SET @MachineId = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 2 SET @Timestamp = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 3 SET @Number = @Field;
                ELSE IF @FieldNum = 4 SET @Typography = @Field;
                ELSE IF @FieldNum = 5 SET @TypeNumber = TRY_CAST(@Field AS FLOAT);
                
                -- Move to the next field
                SET @Pos = @NextPos + 1;
                SET @FieldNum = @FieldNum + 1;
            END
            
            -- Insert the parsed data into the temp table
            INSERT INTO #TempInfoLogs (
                machine_id, timestamp, number, typography, type_number
            )
            VALUES (
                @MachineId, @Timestamp, @Number, @Typography, @TypeNumber
            );
            
            FETCH NEXT FROM lineCursor INTO @Line;
        END
        
        CLOSE lineCursor;
        DEALLOCATE lineCursor;
        
        -- Auto-add missing machine_ids to the machine_names table
        INSERT INTO machine_names (machine_id, name)
        SELECT DISTINCT t.machine_id, 'Machine ' + CAST(t.machine_id AS NVARCHAR(10))
        FROM #TempInfoLogs t
        LEFT JOIN machine_names m ON t.machine_id = m.machine_id
        WHERE m.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many machines were added
        PRINT 'New machines added: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Log the import operation
        DECLARE @RowsImported INT;
        SELECT @RowsImported = COUNT(*) FROM #TempInfoLogs;
        
        PRINT 'Successfully imported data from ' + @FilePath;
        PRINT 'Rows imported: ' + CAST(@RowsImported AS VARCHAR(20));
        
        -- Insert only new records (now all machine_ids should exist)
        INSERT INTO info_logs (
            machine_id, timestamp, number, typography, type_number
        )
        SELECT 
            t.machine_id, t.timestamp, t.number, t.typography, t.type_number
        FROM #TempInfoLogs t
        LEFT JOIN info_logs i ON 
            t.machine_id = i.machine_id AND
            t.timestamp = i.timestamp AND
            t.number = i.number AND
            t.typography = i.typography
        WHERE i.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many new records were inserted
        PRINT 'New records inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Drop the temporary tables
        DROP TABLE #RawData;
        DROP TABLE #TempInfoLogs;
        
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
        EXEC sp_executesql N'RAISERROR(''Info Import Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
        
        -- Clean up temporary tables and cursor if they exist
        IF CURSOR_STATUS('local', 'lineCursor') = 1
        BEGIN
            CLOSE lineCursor;
            DEALLOCATE lineCursor;
        END
        
        IF OBJECT_ID('tempdb..#RawData') IS NOT NULL
            DROP TABLE #RawData;
            
        IF OBJECT_ID('tempdb..#TempInfoLogs') IS NOT NULL
            DROP TABLE #TempInfoLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

## 5. Rinse Logs Debug Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_ImportRinseLogs_Debug]
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Read the file directly using xp_cmdshell
    CREATE TABLE #RawData (line_data NVARCHAR(MAX));
    
    -- Import the file using xp_cmdshell to ensure we get all lines
    INSERT INTO #RawData
    EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Rinse\current\Silver_Rinse.dat"';
    
    -- Remove NULL rows and the header row
    DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
    
    -- Show sample data
    SELECT TOP 5 line_data FROM #RawData;
    
    -- Take one row for testing
    DECLARE @TestRow NVARCHAR(MAX);
    SELECT TOP 1 @TestRow = line_data FROM #RawData;
    
    -- Test the parsing of this row
    SELECT 
        @TestRow AS original_row,
        LEFT(@TestRow, CHARINDEX(';', @TestRow) - 1) AS first_field,
        PARSENAME(REPLACE(@TestRow, ';', '.'), 12) AS machine_id_field,
        PARSENAME(REPLACE(@TestRow, ';', '.'), 11) AS timestamp_field,
        PARSENAME(REPLACE(@TestRow, ';', '.'), 1) AS last_field;
    
    -- Count records with non-NULL machine_id after parsing
    CREATE TABLE #TestParsing (
        machine_id INT NULL,
        other_field VARCHAR(10)
    );
    
    INSERT INTO #TestParsing
    SELECT 
        TRY_CAST(PARSENAME(REPLACE(line_data, ';', '.'), 12) AS INT),
        'test'
    FROM #RawData
    WHERE line_data IS NOT NULL AND line_data <> '';
    
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN machine_id IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_machine_id,
        SUM(CASE WHEN machine_id IS NULL THEN 1 ELSE 0 END) AS rows_with_null_machine_id
    FROM #TestParsing;
    
    -- Clean up
    DROP TABLE #RawData;
    DROP TABLE #TestParsing;
END;


```
## 6. Info Logs Debug Procedure

```sql
USE [machine_data]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Create a debug version of the procedure
ALTER PROCEDURE [dbo].[sp_ImportInfoLogs_Debug]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Info\current\Silver_Info.dat';
    
    BEGIN TRY
        -- Check if file exists
        DECLARE @FileExists INT = 0;
        EXEC master.dbo.xp_fileexist @FilePath, @FileExists OUTPUT;
        SELECT 'File exists check:', @FileExists;
        
        -- Create a staging table
        CREATE TABLE #StagingInfoLogs (
            line_data NVARCHAR(MAX)
        );
        
        -- Try different line terminators
        PRINT 'Trying with \n terminator:';
        DECLARE @BulkCommand NVARCHAR(4000);
        SET @BulkCommand = 'BULK INSERT #StagingInfoLogs FROM ''' + @FilePath + ''' WITH (ROWTERMINATOR = ''\n'', CODEPAGE = ''65001'')';
        
        EXEC sp_executesql @BulkCommand;
        
        -- Check what was loaded
        SELECT 'Rows loaded (LF):', COUNT(*) FROM #StagingInfoLogs;
        SELECT TOP 5 'Raw data sample:', line_data FROM #StagingInfoLogs;
        
        -- Check for empty rows
        SELECT 'Empty rows:', COUNT(*) FROM #StagingInfoLogs WHERE RTRIM(LTRIM(line_data)) = '';
        
        -- Clean and try again with CRLF
        TRUNCATE TABLE #StagingInfoLogs;
        
        PRINT 'Trying with \r\n terminator:';
        SET @BulkCommand = 'BULK INSERT #StagingInfoLogs FROM ''' + @FilePath + ''' WITH (ROWTERMINATOR = ''\r\n'', CODEPAGE = ''65001'')';
        EXEC sp_executesql @BulkCommand;
        
        -- Check what was loaded
        SELECT 'Rows loaded (CRLF):', COUNT(*) FROM #StagingInfoLogs;
        
        -- Verify header detection
        SELECT 'Header rows found:', COUNT(*) 
        FROM #StagingInfoLogs 
        WHERE line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempInfoLogs (
            machine_id INT NULL,
            timestamp DATETIME NULL,
            number NVARCHAR(50) NULL,
            typography NVARCHAR(10) NULL,
            type_number FLOAT NULL
        );
        
        -- Test the PARSENAME operation on a sample row
        SELECT TOP 1
            'Original data:', line_data,
            'After REPLACE:', REPLACE(line_data, ';', '.'),
            'PARSENAME 5:', PARSENAME(REPLACE(line_data, ';', '.'), 5),
            'PARSENAME 4:', PARSENAME(REPLACE(line_data, ';', '.'), 4),
            'PARSENAME 3:', PARSENAME(REPLACE(line_data, ';', '.'), 3),
            'PARSENAME 2:', PARSENAME(REPLACE(line_data, ';', '.'), 2),
            'PARSENAME 1:', PARSENAME(REPLACE(line_data, ';', '.'), 1)
        FROM #StagingInfoLogs
        WHERE line_data NOT LIKE 'machine_id%';
        
        -- Parse data from the staging table, skipping header row
        INSERT INTO #TempInfoLogs
        SELECT 
            CAST(PARSENAME(REPLACE(t.cols, ';', '.'), 5) AS INT),
            CAST(PARSENAME(REPLACE(t.cols, ';', '.'), 4) AS DATETIME),
            PARSENAME(REPLACE(t.cols, ';', '.'), 3),
            PARSENAME(REPLACE(t.cols, ';', '.'), 2),
            CAST(PARSENAME(REPLACE(t.cols, ';', '.'), 1) AS FLOAT)
        FROM (
            SELECT line_data as cols FROM #StagingInfoLogs 
            WHERE line_data NOT LIKE 'machine_id%' -- Skip header row
        ) t;
        
        -- Check what was parsed
        SELECT 'Rows successfully parsed:', COUNT(*) FROM #TempInfoLogs;
        SELECT TOP 5 * FROM #TempInfoLogs;
        
        -- Check for NULLs in important fields
        SELECT 'Rows with NULL machine_id:', COUNT(*) FROM #TempInfoLogs WHERE machine_id IS NULL;
        SELECT 'Rows with NULL timestamp:', COUNT(*) FROM #TempInfoLogs WHERE timestamp IS NULL;
        
        -- Drop the temporary tables
        DROP TABLE #StagingInfoLogs;
        DROP TABLE #TempInfoLogs;
        
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = 
            'Error ' + CONVERT(VARCHAR(50), ERROR_NUMBER()) + 
            ', Severity ' + CONVERT(VARCHAR(5), ERROR_SEVERITY()) + 
            ', State ' + CONVERT(VARCHAR(5), ERROR_STATE()) + 
            ', Line ' + CONVERT(VARCHAR(5), ERROR_LINE()) + 
            ', Message: ' + ERROR_MESSAGE();
            
        SELECT 'ERROR:', @ErrorMessage;
        
        -- Clean up temporary tables if they exist
        IF OBJECT_ID('tempdb..#StagingInfoLogs') IS NOT NULL
            DROP TABLE #StagingInfoLogs;
            
        IF OBJECT_ID('tempdb..#TempInfoLogs') IS NOT NULL
            DROP TABLE #TempInfoLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
```

