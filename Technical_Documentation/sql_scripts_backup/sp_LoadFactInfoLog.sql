USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_LoadFactInfoLog]    Script Date: 5/4/2025 8:36:53 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Create a procedure to load the info log fact table
ALTER   PROCEDURE [dbo].[sp_LoadFactInfoLog]
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
        SELECT-- Limit to prevent issues
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
