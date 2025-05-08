USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportRinseLogs_Debug]    Script Date: 5/4/2025 8:31:21 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[sp_ImportRinseLogs_Debug]
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
