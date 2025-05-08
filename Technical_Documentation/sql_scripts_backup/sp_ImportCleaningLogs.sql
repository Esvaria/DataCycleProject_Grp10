USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportCleaningLogs]    Script Date: 5/4/2025 8:29:41 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Updated Cleaning Logs Import Procedure with Fixed Parsing
ALTER   PROCEDURE [dbo].[sp_ImportCleaningLogs]
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
