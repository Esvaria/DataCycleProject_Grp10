USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_LoadFactProductRun]    Script Date: 5/4/2025 8:37:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_LoadFactProductRun]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Create a temp table for valid records
        CREATE TABLE #ValidProductRecords (
            machine_id INT,
            date_id INT,
            time_id INT,
            product_type_id INT,
            double_prod BIT,
            bean_hopper INT,
            press_before FLOAT,
            press_after FLOAT,
            press_final FLOAT,
            grind_time FLOAT,
            ext_time FLOAT,
            water_qnty FLOAT,
            water_temp FLOAT,
            outlet_side INT,
            stopped INT,
            milk_temp FLOAT,
            steam_pressure FLOAT,
            grind_adjust_left FLOAT,
            grind_adjust_right FLOAT,
            milk_time FLOAT,
            boiler_temp FLOAT
        );
        
        -- Insert into temp table with validated foreign keys
        INSERT INTO #ValidProductRecords
        SELECT
            p.machine_id,
            CAST(YEAR(p.timestamp) * 10000 + MONTH(p.timestamp) * 100 + DAY(p.timestamp) AS INT),
            (DATEPART(HOUR, p.timestamp) * 60 + DATEPART(MINUTE, p.timestamp)),
            -- Map to valid dimension values
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimProductType WHERE product_type_id = ISNULL(p.prod_type, 255))
                THEN ISNULL(p.prod_type, 255)
                ELSE 0 -- Default to first value
            END,
            CAST(CASE WHEN p.double_prod = 1 THEN '1' ELSE '0' END AS BIT),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimBeanHopper WHERE bean_hopper_id = ISNULL(p.bean_hopper, 255))
                THEN ISNULL(p.bean_hopper, 255)
                ELSE 255 -- Default to 'None'
            END,
            ISNULL(p.press_before, 0),
            ISNULL(p.press_after, 0),
            ISNULL(p.press_final, 0),
            ISNULL(p.grind_time, 0),
            ISNULL(p.ext_time, 0),
            ISNULL(p.water_qnty, 0),
            ISNULL(p.water_temp, 0),
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimOutletSide WHERE outlet_side_id = p.outlet_side)
                THEN p.outlet_side
                ELSE 0 -- Default to 'Left'
            END,
            CASE 
                WHEN EXISTS (SELECT 1 FROM DimStopped WHERE stopped_id = p.stopped)
                THEN p.stopped
                ELSE 0 -- Default to 'Finished'
            END,
            ISNULL(p.milk_temp, 0),
            ISNULL(p.steam_pressure, 0),
            ISNULL(p.grind_adjust_left, 0),
            ISNULL(p.grind_adjust_right, 0),
            ISNULL(p.milk_time, 0),
            ISNULL(p.boiler_temp, 0)
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM FactProductRun f
            WHERE f.machine_id = p.machine_id
            AND f.date_id = CAST(YEAR(p.timestamp) * 10000 + MONTH(p.timestamp) * 100 + DAY(p.timestamp) AS INT)
            AND f.time_id = (DATEPART(HOUR, p.timestamp) * 60 + DATEPART(MINUTE, p.timestamp))
            AND f.product_type_id = ISNULL(p.prod_type, 1)
        );
        
        -- Print any records with product type issues (for debugging)
        SELECT 'Problem records with product_type_id:', 
               machine_id, date_id, time_id, product_type_id
        FROM #ValidProductRecords
        WHERE NOT EXISTS (SELECT 1 FROM DimProductType WHERE product_type_id = product_type_id);
        
        -- Now insert from temp table to fact table
        INSERT INTO FactProductRun (
            machine_id, date_id, time_id, product_type_id, double_prod, bean_hopper,
            press_before, press_after, press_final, grind_time, ext_time, water_qnty, water_temp,
            outlet_side, stopped, milk_temp, steam_pressure, grind_adjust_left, grind_adjust_right,
            milk_time, boiler_temp
        )
        SELECT * FROM #ValidProductRecords;
        
        -- Clean up
        DROP TABLE #ValidProductRecords;
        
        PRINT 'Loaded ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' records into FactProductRun';
        RETURN 0;
    END TRY
    BEGIN CATCH
        IF OBJECT_ID('tempdb..#ValidProductRecords') IS NOT NULL
            DROP TABLE #ValidProductRecords;
            
        SELECT @ErrorMessage = 
            'Error loading FactProductRun: ' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        RETURN -1;
    END CATCH
END;