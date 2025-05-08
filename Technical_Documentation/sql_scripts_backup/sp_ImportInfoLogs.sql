USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportInfoLogs]    Script Date: 5/4/2025 8:30:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- Updated Info Logs Import Procedure with Fixed Parsing
ALTER   PROCEDURE [dbo].[sp_ImportInfoLogs]
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
