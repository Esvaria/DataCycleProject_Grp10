USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_EnsureDimensionCompleteness]    Script Date: 5/4/2025 8:36:06 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[sp_EnsureDimensionCompleteness]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- 1. Check and add missing detergent status values
        INSERT INTO DimDetergentStatus (detergent_status_id, detergent_status_name)
        SELECT DISTINCT CAST(ISNULL(c.detergent_status_left, 0) AS INT), 'Auto-added Status ' + CAST(CAST(ISNULL(c.detergent_status_left, 0) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimDetergentStatus d 
            WHERE d.detergent_status_id = CAST(ISNULL(c.detergent_status_left, 0) AS INT)
        );
        
        INSERT INTO DimDetergentStatus (detergent_status_id, detergent_status_name)
        SELECT DISTINCT CAST(ISNULL(c.detergent_status_right, 0) AS INT), 'Auto-added Status ' + CAST(CAST(ISNULL(c.detergent_status_right, 0) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimDetergentStatus d 
            WHERE d.detergent_status_id = CAST(ISNULL(c.detergent_status_right, 0) AS INT)
        );
        
        -- 2. Check and add missing product type values
        INSERT INTO DimProductType (product_type_id, prod_type, description)
        SELECT DISTINCT ISNULL(p.prod_type, 255), 
               'Auto-added Type ' + CAST(ISNULL(p.prod_type, 255) AS VARCHAR(10)),
               'Automatically added product type ' + CAST(ISNULL(p.prod_type, 255) AS VARCHAR(10))
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM DimProductType d 
            WHERE d.product_type_id = ISNULL(p.prod_type, 255)
        );
        
        -- 3. Check and add missing bean hopper values
        INSERT INTO DimBeanHopper (bean_hopper_id, bean_hopper_name)
        SELECT DISTINCT ISNULL(p.bean_hopper, 255), 
               'Auto-added Hopper ' + CAST(ISNULL(p.bean_hopper, 255) AS VARCHAR(10))
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM DimBeanHopper d 
            WHERE d.bean_hopper_id = ISNULL(p.bean_hopper, 255)
        );
        
        -- 4. Check and add missing outlet side values
        INSERT INTO DimOutletSide (outlet_side_id, outlet_side_name)
        SELECT DISTINCT p.outlet_side, 
               'Auto-added Outlet ' + CAST(p.outlet_side AS VARCHAR(10))
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM DimOutletSide d 
            WHERE d.outlet_side_id = p.outlet_side
        )
        AND p.outlet_side IS NOT NULL;
        
        -- 5. Check and add missing stopped values
        INSERT INTO DimStopped (stopped_id, stopped_name)
        SELECT DISTINCT p.stopped, 
               'Auto-added Stopped ' + CAST(p.stopped AS VARCHAR(10))
        FROM machine_data.dbo.product_logs p
        WHERE NOT EXISTS (
            SELECT 1 FROM DimStopped d 
            WHERE d.stopped_id = p.stopped
        )
        AND p.stopped IS NOT NULL;
        
        -- 6. Check and add missing powder status values
        INSERT INTO DimPowderStatus (powder_status_id, status_name)
        SELECT DISTINCT CAST(ISNULL(c.powder_clean_status, 0) AS INT), 
               'Auto-added Powder Status ' + CAST(CAST(ISNULL(c.powder_clean_status, 0) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimPowderStatus d 
            WHERE d.powder_status_id = CAST(ISNULL(c.powder_clean_status, 0) AS INT)
        );
        
        -- 7. Check and add missing tabs status values
        INSERT INTO DimTabsStatus (tabs_status_id, tabs_status_name)
        SELECT DISTINCT CAST(ISNULL(c.tabs_status_left, 2) AS INT), 
               'Auto-added Tabs Status ' + CAST(CAST(ISNULL(c.tabs_status_left, 2) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimTabsStatus d 
            WHERE d.tabs_status_id = CAST(ISNULL(c.tabs_status_left, 2) AS INT)
        );
        
        INSERT INTO DimTabsStatus (tabs_status_id, tabs_status_name)
        SELECT DISTINCT CAST(ISNULL(c.tabs_status_right, 2) AS INT), 
               'Auto-added Tabs Status ' + CAST(CAST(ISNULL(c.tabs_status_right, 2) AS INT) AS VARCHAR(10))
        FROM machine_data.dbo.cleaning_logs c
        WHERE NOT EXISTS (
            SELECT 1 FROM DimTabsStatus d 
            WHERE d.tabs_status_id = CAST(ISNULL(c.tabs_status_right, 2) AS INT)
        );
        
        -- 8. Check and add missing rinse type values
        INSERT INTO DimRinseType (rinse_type_id, rinse_type_name)
        SELECT DISTINCT ISNULL(r.rinse_type, 255), 
               'Auto-added Rinse Type ' + CAST(ISNULL(r.rinse_type, 255) AS VARCHAR(10))
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM DimRinseType d 
            WHERE d.rinse_type_id = ISNULL(r.rinse_type, 255)
        );
        
        -- 9. Check and add missing rinse status values
        INSERT INTO DimRinseStatus (rinse_status_id, rinse_status_name)
        SELECT DISTINCT ISNULL(r.status_left, 0), 
               'Auto-added Status ' + CAST(ISNULL(r.status_left, 0) AS VARCHAR(10))
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM DimRinseStatus d 
            WHERE d.rinse_status_id = ISNULL(r.status_left, 0)
        );
        
        INSERT INTO DimRinseStatus (rinse_status_id, rinse_status_name)
        SELECT DISTINCT ISNULL(r.status_right, 0), 
               'Auto-added Status ' + CAST(ISNULL(r.status_right, 0) AS VARCHAR(10))
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM DimRinseStatus d 
            WHERE d.rinse_status_id = ISNULL(r.status_right, 0)
        );
        
        -- 10. Check and add missing nozzle status values
        INSERT INTO DimNozzleStatus (nozzle_status_id, nozzle_status_name)
        SELECT DISTINCT ISNULL(r.nozzle_status_left, 0), 
               'Auto-added Nozzle Status ' + CAST(ISNULL(r.nozzle_status_left, 0) AS VARCHAR(10))
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM DimNozzleStatus d 
            WHERE d.nozzle_status_id = ISNULL(r.nozzle_status_left, 0)
        );
        
        INSERT INTO DimNozzleStatus (nozzle_status_id, nozzle_status_name)
        SELECT DISTINCT ISNULL(r.nozzle_status_right, 0), 
               'Auto-added Nozzle Status ' + CAST(ISNULL(r.nozzle_status_right, 0) AS VARCHAR(10))
        FROM machine_data.dbo.rinse_logs r
        WHERE NOT EXISTS (
            SELECT 1 FROM DimNozzleStatus d 
            WHERE d.nozzle_status_id = ISNULL(r.nozzle_status_right, 0)
        );
        
        PRINT 'Dimension tables completeness ensured.';
        RETURN 0;
    END TRY
    BEGIN CATCH
        SELECT @ErrorMessage = 
            'Error ensuring dimension completeness: ' + ERROR_MESSAGE();
        PRINT @ErrorMessage;
        RETURN -1;
    END CATCH
END;