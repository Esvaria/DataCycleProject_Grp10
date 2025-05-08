USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_LoadFactMachineCleaning]    Script Date: 5/4/2025 8:37:13 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_LoadFactMachineCleaning]
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