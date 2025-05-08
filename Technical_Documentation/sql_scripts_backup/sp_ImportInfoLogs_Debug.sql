USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportInfoLogs_Debug]    Script Date: 5/4/2025 8:30:29 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Create a debug version of the procedure
ALTER   PROCEDURE [dbo].[sp_ImportInfoLogs_Debug]
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
