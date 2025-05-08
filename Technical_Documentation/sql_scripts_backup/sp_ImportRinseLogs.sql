USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportRinseLogs]    Script Date: 5/4/2025 8:31:04 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- Updated Rinse Logs Import Procedure with Fixed Parsing
ALTER   PROCEDURE [dbo].[sp_ImportRinseLogs]
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
