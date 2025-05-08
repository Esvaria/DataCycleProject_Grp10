USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_LoadFactRinseOperation]    Script Date: 5/4/2025 8:37:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_LoadFactRinseOperation]
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