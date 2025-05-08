ALTER PROCEDURE [dbo].[sp_LoadDimensionTables]
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
        
        -- 2. Generate or populate DimDate - assuming we need dates from 2022 to 2025
        -- Check if DimDate is empty
        IF NOT EXISTS (SELECT TOP 1 1 FROM DimDate)
        BEGIN
            -- Create a temporary table to hold dates
            CREATE TABLE #TempDates (full_date DATE);
            
            -- Variables for the date range
            DECLARE @StartDate DATE = '2022-01-01';
            DECLARE @EndDate DATE = '2025-12-31';
            DECLARE @CurrentDate DATE = @StartDate;
            
            -- Generate dates
            WHILE @CurrentDate <= @EndDate
            BEGIN
                INSERT INTO #TempDates (full_date) VALUES (@CurrentDate);
                SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
            END
            
            -- Insert into DimDate
            INSERT INTO DimDate (
                date_id, 
                full_date, 
                day, 
                month, 
                quarter, 
                year, 
                is_weekend, 
                is_holiday
            )
            SELECT
                -- Create a date key in the format YYYYMMDD
                CAST(YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date) AS INT) AS date_id,
                full_date,
                DAY(full_date) AS day,
                MONTH(full_date) AS month,
                DATEPART(QUARTER, full_date) AS quarter,
                YEAR(full_date) AS year,
                CASE WHEN DATEPART(WEEKDAY, full_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
                -- For simplicity, just marking major US holidays
                CASE WHEN (MONTH(full_date) = 1 AND DAY(full_date) = 1) -- New Year's Day
                     OR (MONTH(full_date) = 7 AND DAY(full_date) = 4)  -- Independence Day
                     OR (MONTH(full_date) = 12 AND DAY(full_date) = 25) -- Christmas
                     THEN 1 ELSE 0 END AS is_holiday
            FROM #TempDates;
            
            -- Drop the temporary table
            DROP TABLE #TempDates;
            
            PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' dates into DimDate';
        END
        
        -- 3. Generate or populate DimTime - every minute of a day
        -- Check if DimTime is empty
        IF NOT EXISTS (SELECT TOP 1 1 FROM DimTime)
        BEGIN
            -- Create time entries for every minute of the day
            WITH MinuteCTE AS (
                SELECT 0 AS minute_of_day
                UNION ALL
                SELECT minute_of_day + 1
                FROM MinuteCTE
                WHERE minute_of_day < 1439 -- 24 hours * 60 minutes - 1
            )
            INSERT INTO DimTime (
                time_id,
                full_time,
                hour,
                minute,
                second,
                am_pm,
                shift
            )
            SELECT
                minute_of_day AS time_id,
                DATEADD(MINUTE, minute_of_day, '00:00:00') AS full_time,
                DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) AS hour,
                DATEPART(MINUTE, DATEADD(MINUTE, minute_of_day, '00:00:00')) AS minute,
                0 AS second, -- All entries have 0 seconds
                CASE WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
                CASE 
                    WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) >= 6 AND DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 14 THEN 'Morning'
                    WHEN DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) >= 14 AND DATEPART(HOUR, DATEADD(MINUTE, minute_of_day, '00:00:00')) < 22 THEN 'Evening'
                    ELSE 'Night'
                END AS shift
            FROM MinuteCTE
            OPTION (MAXRECURSION 1500); -- Needed for the recursive CTE
            
            PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' time entries into DimTime';
        END
        
        -- 4. Update lookup dimensions with defined values using MERGE
        
        -- DimProductType
        MERGE INTO DimProductType AS target
        USING (
            VALUES 
                (0, 'None', 'Aucun produit'),
                (1, 'Ristretto', 'Standard Ristretto'),
                (2, 'Expresso', 'Standard Expresso'),
                (3, 'Coffee', 'Standard Coffee'),
                (4, 'Filter Coffee', 'Filtered Coffee'),
                (5, 'Americano', 'Standard Americano'),
                (6, 'Coffee Pot', 'Standard Coffee Pot'),
                (7, 'Filter coffee Pot', 'Filtered Coffee Pot'),
                (8, 'Hot Water', 'Hot Water Only'),
                (9, 'Manual Steam', 'Steam'),
                (10, 'Auto Steam', 'Auto Steam'),
                (11, 'Everfoam', 'Everfoam'),
                (12, 'Milk Coffee', 'Standard Coffee with milk'),
                (13, 'Cappuccino', 'Cappuccino'),
                (14, 'Expresso Macchiatto', 'Expresso Macchiatto'),
                (15, 'Latte Macchiatto', 'Latte Macchiatto'),
                (16, 'Milk', 'Hot Milk'),
                (17, 'Milk Foam', 'Milk Foam'),
                (18, 'Powder', 'Powder'),
                (19, 'White Americano', 'White Americano'),
                (20, 'Max', 'Max'),
                (255, 'Undefined', 'Undefined')
        ) AS source (product_type_id, prod_type, description)
        ON target.product_type_id = source.product_type_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.prod_type = source.prod_type,
                target.description = source.description
        WHEN NOT MATCHED THEN
            INSERT (product_type_id, prod_type, description)
            VALUES (source.product_type_id, source.prod_type, source.description);
            
        PRINT 'Updated DimProductType - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimPowderStatus
        MERGE INTO DimPowderStatus AS target
        USING (
            VALUES 
                (0, 'Undefined'),
                (1, 'Not Necessary'),
                (2, 'Mixer Cleaned'),
                (3, 'Without Mixer'),
                (4, 'Max')
        ) AS source (powder_status_id, status_name)
        ON target.powder_status_id = source.powder_status_id
        WHEN MATCHED THEN
            UPDATE SET target.status_name = source.status_name
        WHEN NOT MATCHED THEN
            INSERT (powder_status_id, status_name)
            VALUES (source.powder_status_id, source.status_name);
                
        PRINT 'Updated DimPowderStatus - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimTabsStatus
        MERGE INTO DimTabsStatus AS target
        USING (
            VALUES 
                (0, 'No'),
                (1, 'Yes'),
                (2, 'Undefined'),
                (3, 'Error'),
                (4, 'Unknown'),
                (5, 'Not Necessary'),
                (6, 'Cycle Error'),
                (7, 'Max')
        ) AS source (tabs_status_id, tabs_status_name)
        ON target.tabs_status_id = source.tabs_status_id
        WHEN MATCHED THEN
            UPDATE SET target.tabs_status_name = source.tabs_status_name
        WHEN NOT MATCHED THEN
            INSERT (tabs_status_id, tabs_status_name)
            VALUES (source.tabs_status_id, source.tabs_status_name);
                
        PRINT 'Updated DimTabsStatus - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimDetergentStatus
        MERGE INTO DimDetergentStatus AS target
        USING (
            VALUES 
                (0, 'Not defined'),
                (1, 'No'),
                (2, 'Yes'),
                (3, 'Error'),
                (4, 'Unknown'),
                (5, 'Not Necessary'),
                (6, 'Cycle Abort'),
                (7, 'Cycle Warning'),
                (8, 'Detergent Warning'),
                (9, 'Max')
        ) AS source (detergent_status_id, detergent_status_name)
        ON target.detergent_status_id = source.detergent_status_id
        WHEN MATCHED THEN
            UPDATE SET target.detergent_status_name = source.detergent_status_name
        WHEN NOT MATCHED THEN
            INSERT (detergent_status_id, detergent_status_name)
            VALUES (source.detergent_status_id, source.detergent_status_name);
                
        PRINT 'Updated DimDetergentStatus - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimRinseType
        MERGE INTO DimRinseType AS target
        USING (
            VALUES 
                (0, 'Initial Reboot'),
                (1, 'Initial Wake Up'),
                (2, 'Warm Left'),
                (3, 'Warm Right'),
                (4, 'After Clean'),
                (5, 'Flow Rate'),
                (6, 'Requested ETC'),
                (7, 'Max'),
                (255, 'Undefined')
        ) AS source (rinse_type_id, rinse_type_name)
        ON target.rinse_type_id = source.rinse_type_id
        WHEN MATCHED THEN
            UPDATE SET target.rinse_type_name = source.rinse_type_name
        WHEN NOT MATCHED THEN
            INSERT (rinse_type_id, rinse_type_name)
            VALUES (source.rinse_type_id, source.rinse_type_name);
                
        PRINT 'Updated DimRinseType - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimRinseStatus
        MERGE INTO DimRinseStatus AS target
        USING (
            VALUES 
                (0, 'Undefined'),
                (1, 'Unknown'),
                (2, 'Too Low'),
                (3, 'Too High'),
                (4, 'Nozzel 05'),
                (5, 'Nozzel 07'),
                (6, 'System OK')
        ) AS source (rinse_status_id, rinse_status_name)
        ON target.rinse_status_id = source.rinse_status_id
        WHEN MATCHED THEN
            UPDATE SET target.rinse_status_name = source.rinse_status_name
        WHEN NOT MATCHED THEN
            INSERT (rinse_status_id, rinse_status_name)
            VALUES (source.rinse_status_id, source.rinse_status_name);
                
        PRINT 'Updated DimRinseStatus - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimNozzleStatus
        MERGE INTO DimNozzleStatus AS target
        USING (
            VALUES 
                (0, 'Undefined'),
                (1, 'Unknown'),
                (2, 'Too Low'),
                (3, 'Too High'),
                (4, 'Nuzzle 05'),
                (5, 'Nuzle 07'),
                (6, 'System OK'),
                (255, 'Null')
        ) AS source (nozzle_status_id, nozzle_status_name)
        ON target.nozzle_status_id = source.nozzle_status_id
        WHEN MATCHED THEN
            UPDATE SET target.nozzle_status_name = source.nozzle_status_name
        WHEN NOT MATCHED THEN
            INSERT (nozzle_status_id, nozzle_status_name)
            VALUES (source.nozzle_status_id, source.nozzle_status_name);
                
        PRINT 'Updated DimNozzleStatus - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimBeanHopper
        MERGE INTO DimBeanHopper AS target
        USING (
            VALUES 
                (0, 'Front Right'),
                (1, 'Rear Left'),
                (2, 'Mix'),
                (3, 'Powder chute'),
                (255, 'None')
        ) AS source (bean_hopper_id, bean_hopper_name)
        ON target.bean_hopper_id = source.bean_hopper_id
        WHEN MATCHED THEN
            UPDATE SET target.bean_hopper_name = source.bean_hopper_name
        WHEN NOT MATCHED THEN
            INSERT (bean_hopper_id, bean_hopper_name)
            VALUES (source.bean_hopper_id, source.bean_hopper_name);
                
        PRINT 'Updated DimBeanHopper - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimOutletSide
        MERGE INTO DimOutletSide AS target
        USING (
            VALUES 
                (0, 'Left'),
                (1, 'Right')
        ) AS source (outlet_side_id, outlet_side_name)
        ON target.outlet_side_id = source.outlet_side_id
        WHEN MATCHED THEN
            UPDATE SET target.outlet_side_name = source.outlet_side_name
        WHEN NOT MATCHED THEN
            INSERT (outlet_side_id, outlet_side_name)
            VALUES (source.outlet_side_id, source.outlet_side_name);
                
        PRINT 'Updated DimOutletSide - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
        -- DimStopped
        MERGE INTO DimStopped AS target
        USING (
            VALUES 
                (0, 'Finished'),
                (1, 'Stopped'),
                (2, 'Machine Abort'),
                (3, 'User Abort')
        ) AS source (stopped_id, stopped_name)
        ON target.stopped_id = source.stopped_id
        WHEN MATCHED THEN
            UPDATE SET target.stopped_name = source.stopped_name
        WHEN NOT MATCHED THEN
            INSERT (stopped_id, stopped_name)
            VALUES (source.stopped_id, source.stopped_name);
                
        PRINT 'Updated DimStopped - ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' rows affected';
        
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