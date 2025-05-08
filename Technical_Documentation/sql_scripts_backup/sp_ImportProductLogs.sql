USE [machine_data]
GO
/****** Object:  StoredProcedure [dbo].[sp_ImportProductLogs]    Script Date: 5/4/2025 8:30:50 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- Updated Product Logs Import Procedure with Fixed Parsing
ALTER   PROCEDURE [dbo].[sp_ImportProductLogs]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @FilePath NVARCHAR(255) = 'C:\Users\Administrator\DataCycleProject\SilverRawData\Product\current\Silver_Product.dat';
    
    BEGIN TRY
        -- Read the file directly using xp_cmdshell
        CREATE TABLE #RawData (line_data NVARCHAR(MAX));
        
        -- Import the file using xp_cmdshell to ensure we get all lines
        INSERT INTO #RawData
        EXEC master.dbo.xp_cmdshell 'type "C:\Users\Administrator\DataCycleProject\SilverRawData\Product\current\Silver_Product.dat"';
        
        -- Remove NULL rows and the header row
        DELETE FROM #RawData WHERE line_data IS NULL OR line_data LIKE 'machine_id%';
        
        -- Create a temporary table for the parsed data
        CREATE TABLE #TempProductLogs (
            machine_id INT NULL,
            timestamp DATETIME NULL,
            press_before FLOAT NULL,
            press_after FLOAT NULL,
            press_final FLOAT NULL,
            grind_time FLOAT NULL,
            ext_time FLOAT NULL,
            water_qnty INT NULL,
            water_temp INT NULL,
            prod_type INT NULL,
            double_prod FLOAT NULL,
            bean_hopper INT NULL,
            outlet_side FLOAT NULL,
            stopped FLOAT NULL,
            milk_temp INT NULL,
            steam_pressure FLOAT NULL,
            grind_adjust_left INT NULL,
            grind_adjust_right INT NULL,
            milk_time FLOAT NULL,
            boiler_temp INT NULL
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
        DECLARE @PressBefore FLOAT;
        DECLARE @PressAfter FLOAT;
        DECLARE @PressFinal FLOAT;
        DECLARE @GrindTime FLOAT;
        DECLARE @ExtTime FLOAT;
        DECLARE @WaterQnty INT;
        DECLARE @WaterTemp INT;
        DECLARE @ProdType INT;
        DECLARE @DoubleProd FLOAT;
        DECLARE @BeanHopper INT;
        DECLARE @OutletSide FLOAT;
        DECLARE @Stopped FLOAT;
        DECLARE @MilkTemp INT;
        DECLARE @SteamPressure FLOAT;
        DECLARE @GrindAdjustLeft INT;
        DECLARE @GrindAdjustRight INT;
        DECLARE @MilkTime FLOAT;
        DECLARE @BoilerTemp INT;
        
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
            SET @PressBefore = NULL;
            SET @PressAfter = NULL;
            SET @PressFinal = NULL;
            SET @GrindTime = NULL;
            SET @ExtTime = NULL;
            SET @WaterQnty = NULL;
            SET @WaterTemp = NULL;
            SET @ProdType = NULL;
            SET @DoubleProd = NULL;
            SET @BeanHopper = NULL;
            SET @OutletSide = NULL;
            SET @Stopped = NULL;
            SET @MilkTemp = NULL;
            SET @SteamPressure = NULL;
            SET @GrindAdjustLeft = NULL;
            SET @GrindAdjustRight = NULL;
            SET @MilkTime = NULL;
            SET @BoilerTemp = NULL;
            
            WHILE @Pos <= LEN(@Line) AND @FieldNum <= 20
            BEGIN
                SET @NextPos = CHARINDEX(@Delimiter, @Line + @Delimiter, @Pos);
                IF @NextPos = 0 SET @NextPos = LEN(@Line) + 1;
                
                SET @Field = SUBSTRING(@Line, @Pos, @NextPos - @Pos);
                
                -- Assign the field to the correct variable based on its position
                IF @FieldNum = 1 SET @MachineId = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 2 SET @Timestamp = TRY_CAST(@Field AS DATETIME);
                ELSE IF @FieldNum = 3 SET @PressBefore = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 4 SET @PressAfter = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 5 SET @PressFinal = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 6 SET @GrindTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 7 SET @ExtTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 8 SET @WaterQnty = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 9 SET @WaterTemp = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 10 SET @ProdType = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 11 SET @DoubleProd = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 12 SET @BeanHopper = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 13 SET @OutletSide = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 14 SET @Stopped = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 15 SET @MilkTemp = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 16 SET @SteamPressure = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 17 SET @GrindAdjustLeft = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 18 SET @GrindAdjustRight = TRY_CAST(@Field AS INT);
                ELSE IF @FieldNum = 19 SET @MilkTime = TRY_CAST(@Field AS FLOAT);
                ELSE IF @FieldNum = 20 SET @BoilerTemp = TRY_CAST(@Field AS INT);
                
                -- Move to the next field
                SET @Pos = @NextPos + 1;
                SET @FieldNum = @FieldNum + 1;
            END
            
            -- Insert the parsed data into the temp table
            INSERT INTO #TempProductLogs (
                machine_id, timestamp, press_before, press_after, press_final, 
                grind_time, ext_time, water_qnty, water_temp, prod_type, 
                double_prod, bean_hopper, outlet_side, stopped, milk_temp, 
                steam_pressure, grind_adjust_left, grind_adjust_right, milk_time, boiler_temp
            )
            VALUES (
                @MachineId, @Timestamp, @PressBefore, @PressAfter, @PressFinal,
                @GrindTime, @ExtTime, @WaterQnty, @WaterTemp, @ProdType,
                @DoubleProd, @BeanHopper, @OutletSide, @Stopped, @MilkTemp,
                @SteamPressure, @GrindAdjustLeft, @GrindAdjustRight, @MilkTime, @BoilerTemp
            );
            
            FETCH NEXT FROM lineCursor INTO @Line;
        END
        
        CLOSE lineCursor;
        DEALLOCATE lineCursor;
        
        -- Auto-add missing machine_ids to the machine_names table
        INSERT INTO machine_names (machine_id, name)
        SELECT DISTINCT t.machine_id, 'Machine ' + CAST(t.machine_id AS NVARCHAR(10))
        FROM #TempProductLogs t
        LEFT JOIN machine_names m ON t.machine_id = m.machine_id
        WHERE m.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many machines were added
        PRINT 'New machines added: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Log the import operation
        DECLARE @RowsImported INT;
        SELECT @RowsImported = COUNT(*) FROM #TempProductLogs;
        
        PRINT 'Successfully imported data from ' + @FilePath;
        PRINT 'Rows imported: ' + CAST(@RowsImported AS VARCHAR(20));
        
        -- Insert only new records (now all machine_ids should exist)
        INSERT INTO product_logs (
            machine_id, timestamp, press_before, press_after, press_final, 
            grind_time, ext_time, water_qnty, water_temp, prod_type, 
            double_prod, bean_hopper, outlet_side, stopped, milk_temp, 
            steam_pressure, grind_adjust_left, grind_adjust_right, milk_time, boiler_temp
        )
        SELECT 
            t.machine_id, t.timestamp, t.press_before, t.press_after, t.press_final, 
            t.grind_time, t.ext_time, t.water_qnty, t.water_temp, t.prod_type, 
            t.double_prod, t.bean_hopper, t.outlet_side, t.stopped, t.milk_temp, 
            t.steam_pressure, t.grind_adjust_left, t.grind_adjust_right, t.milk_time, t.boiler_temp
        FROM #TempProductLogs t
        LEFT JOIN product_logs p ON 
            t.machine_id = p.machine_id AND
            t.timestamp = p.timestamp AND
            t.prod_type = p.prod_type AND
            t.water_qnty = p.water_qnty
        WHERE p.machine_id IS NULL
        AND t.machine_id IS NOT NULL;
        
        -- Log how many new records were inserted
        PRINT 'New records inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(20));
        
        -- Drop the temporary tables
        DROP TABLE #RawData;
        DROP TABLE #TempProductLogs;
        
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
        EXEC sp_executesql N'RAISERROR(''Product Import Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
        
        -- Clean up temporary tables and cursor if they exist
        IF CURSOR_STATUS('local', 'lineCursor') = 1
        BEGIN
            CLOSE lineCursor;
            DEALLOCATE lineCursor;
        END
        
        IF OBJECT_ID('tempdb..#RawData') IS NOT NULL
            DROP TABLE #RawData;
            
        IF OBJECT_ID('tempdb..#TempProductLogs') IS NOT NULL
            DROP TABLE #TempProductLogs;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
