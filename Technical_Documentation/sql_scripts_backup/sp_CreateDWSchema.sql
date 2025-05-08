USE [DWmachines]
GO
/****** Object:  StoredProcedure [dbo].[sp_CreateDWSchema]    Script Date: 5/4/2025 8:35:45 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- Update the schema creation procedure to match actual column names
ALTER   PROCEDURE [dbo].[sp_CreateDWSchema]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Create dimension tables if they don't exist
        
        -- DimMachine
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimMachine')
        BEGIN
            CREATE TABLE [DimMachine] (
              [machine_id] int PRIMARY KEY,
              [machine_name] nvarchar(255),
              [machine_type] nvarchar(255),
              [installation_date] date,
              [manufacturer] nvarchar(255),
              [model] nvarchar(255),
              [location] nvarchar(255)
            );
            
            PRINT 'Created table DimMachine';
        END
        ELSE
        BEGIN
            PRINT 'Table DimMachine already exists';
        END
        
        -- DimDate
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimDate')
        BEGIN
            CREATE TABLE [DimDate] (
              [date_id] int PRIMARY KEY,
              [full_date] date,
              [day] int,
              [month] int,
              [quarter] int,
              [year] int,
              [is_weekend] bit, 
              [is_holiday] bit 
            );
            
            PRINT 'Created table DimDate';
        END
        ELSE
        BEGIN
            PRINT 'Table DimDate already exists';
        END
        
        -- DimTime
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimTime')
        BEGIN
            CREATE TABLE [DimTime] (
              [time_id] int PRIMARY KEY,
              [full_time] time,
              [hour] int,
              [minute] int,
              [second] int,
              [am_pm] nvarchar(255),
              [shift] nvarchar(255)
            );
            
            PRINT 'Created table DimTime';
        END
        ELSE
        BEGIN
            PRINT 'Table DimTime already exists';
        END
        
        -- DimProductType - Adjusted to match actual columns
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimProductType')
        BEGIN
            CREATE TABLE [DimProductType] (
              [product_type_id] int PRIMARY KEY,
              [prod_type] nvarchar(255),
              [description] nvarchar(255)
            );
            
            PRINT 'Created table DimProductType';
        END
        ELSE
        BEGIN
            PRINT 'Table DimProductType already exists';
        END
        
        -- DimPowderStatus
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimPowderStatus')
        BEGIN
            CREATE TABLE [DimPowderStatus] (
              [powder_status_id] int PRIMARY KEY,
              [status_name] nvarchar(255)
            );
            
            PRINT 'Created table DimPowderStatus';
        END
        ELSE
        BEGIN
            PRINT 'Table DimPowderStatus already exists';
        END
        
        -- DimTabsStatus
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimTabsStatus')
        BEGIN
            CREATE TABLE [DimTabsStatus] (
              [tabs_status_id] int PRIMARY KEY,
              [tabs_status_name] nvarchar(255)
            );
            
            PRINT 'Created table DimTabsStatus';
        END
        ELSE
        BEGIN
            PRINT 'Table DimTabsStatus already exists';
        END
        
        -- DimDetergentStatus
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimDetergentStatus')
        BEGIN
            CREATE TABLE [DimDetergentStatus] (
              [detergent_status_id] int PRIMARY KEY,
              [detergent_status_name] nvarchar(255)
            );
            
            PRINT 'Created table DimDetergentStatus';
        END
        ELSE
        BEGIN
            PRINT 'Table DimDetergentStatus already exists';
        END
        
        -- DimRinseType
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimRinseType')
        BEGIN
            CREATE TABLE [DimRinseType] (
              [rinse_type_id] int PRIMARY KEY,
              [rinse_type_name] nvarchar(255)
            );
            
            PRINT 'Created table DimRinseType';
        END
        ELSE
        BEGIN
            PRINT 'Table DimRinseType already exists';
        END
        
        -- DimRinseStatus
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimRinseStatus')
        BEGIN
            CREATE TABLE [DimRinseStatus] (
              [rinse_status_id] int PRIMARY KEY,
              [rinse_status_name] nvarchar(255)
            );
            
            PRINT 'Created table DimRinseStatus';
        END
        ELSE
        BEGIN
            PRINT 'Table DimRinseStatus already exists';
        END
        
        -- DimNozzleStatus
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimNozzleStatus')
        BEGIN
            CREATE TABLE [DimNozzleStatus] (
              [nozzle_status_id] int PRIMARY KEY,
              [nozzle_status_name] nvarchar(255)
            );
            
            PRINT 'Created table DimNozzleStatus';
        END
        ELSE
        BEGIN
            PRINT 'Table DimNozzleStatus already exists';
        END
        
        -- DimBeanHopper
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimBeanHopper')
        BEGIN
            CREATE TABLE [DimBeanHopper] (
              [bean_hopper_id] int PRIMARY KEY,
              [bean_hopper_name] nvarchar(255)
            );
            
            PRINT 'Created table DimBeanHopper';
        END
        ELSE
        BEGIN
            PRINT 'Table DimBeanHopper already exists';
        END
        
        -- DimOutletSide
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimOutletSide')
        BEGIN
            CREATE TABLE [DimOutletSide] (
              [outlet_side_id] int PRIMARY KEY,
              [outlet_side_name] nvarchar(255)
            );
            
            PRINT 'Created table DimOutletSide';
        END
        ELSE
        BEGIN
            PRINT 'Table DimOutletSide already exists';
        END
        
        -- DimStopped
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'DimStopped')
        BEGIN
            CREATE TABLE [DimStopped] (
              [stopped_id] int PRIMARY KEY,
              [stopped_name] nvarchar(255)
            );
            
            PRINT 'Created table DimStopped';
        END
        ELSE
        BEGIN
            PRINT 'Table DimStopped already exists';
        END
        
        -- Create fact tables if they don't exist
        
        -- FactMachineCleaning - CORRECTED column names to match what was reported
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactMachineCleaning')
        BEGIN
            CREATE TABLE [FactMachineCleaning] (
              [cleaning_id] int IDENTITY(1,1) PRIMARY KEY,
              [machine_id] int,
              [date_id] int,
              [time_start_id] int,
              [time_end_id] int,
              [powder_clean_status] int,
              [tabs_status_left] int,
              [tabs_status_right] int,
              [detergent_status_left] int,
              [detergent_status_right] int,
              [milk_pump_error_left] BIT,
              [milk_pump_error_right] BIT,
              [milk_clean_temp_left_1] decimal(10, 2),
			  [milk_clean_temp_left_2] decimal(10, 2),
              [milk_clean_temp_right_1] decimal(10, 2),
			  [milk_clean_temp_right_2] decimal(10, 2),
              [milk_clean_rpm_left_1] int,
			  [milk_clean_rpm_left_2] int,
              [milk_clean_rpm_right_1] int,
			  [milk_clean_rpm_right_2] int,
              [milk_seq_cycle_left_1] decimal(10, 2),
              [milk_seq_cycle_left_2] decimal(10, 2),
              [milk_seq_cycle_right_1] decimal(10, 2),
              [milk_seq_cycle_right_2] decimal(10, 2),
              [milk_temp_left_1] decimal(10, 2),
              [milk_temp_left_2] decimal(10, 2),
              [milk_temp_right_1] decimal(10, 2),
              [milk_temp_right_2] decimal(10, 2),
              [milk_rpm_left_1] int,
              [milk_rpm_left_2] int,
              [milk_rpm_right_1] int,
              [milk_rpm_right_2] int,
              [cleaning_duration_minutes] int
            );
            
            PRINT 'Created table FactMachineCleaning';
        END
        ELSE
        BEGIN
            PRINT 'Table FactMachineCleaning already exists';
        END
        
        -- FactRinseOperation - CORRECTED column names to match what was reported
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactRinseOperation')
        BEGIN
            CREATE TABLE [FactRinseOperation] (
              [rinse_id] int IDENTITY(1,1) PRIMARY KEY,
              [machine_id] int,
              [date_id] int,
              [time_id] int,
              [rinse_type] int,
              [flow_rate_left] decimal(10, 2),
              [flow_rate_right] decimal(10, 2),
              [status_left] int,
              [status_right] int,
              [pump_pressure] decimal(10, 2),
              [nozzle_flow_rate_left] decimal(10, 2),
              [nozzle_flow_rate_right] decimal(10, 2),
              [nozzle_status_left] int,
              [nozzle_status_right] int
            );
            
            PRINT 'Created table FactRinseOperation';
        END
        ELSE
        BEGIN
            PRINT 'Table FactRinseOperation already exists';
        END
        
        -- FactInfoLog
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactInfoLog')
        BEGIN
            CREATE TABLE [FactInfoLog] (
              [info_id] int IDENTITY(1,1) PRIMARY KEY,
              [machine_id] int,
              [date_id] int,
              [time_id] int,
              [number] int,
              [typography] nvarchar(255),
              [type_number] nvarchar(255)
            );
            
            PRINT 'Created table FactInfoLog';
        END
        ELSE
        BEGIN
            PRINT 'Table FactInfoLog already exists';
        END
        
        -- FactProductRun - CORRECTED column names to match what was reported
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FactProductRun')
        BEGIN
            CREATE TABLE [FactProductRun] (
              [production_id] int IDENTITY(1,1) PRIMARY KEY,
              [machine_id] int,
              [date_id] int,
              [time_id] int,
              [product_type_id] int,
			  [double_prod] BIT,
			  [bean_hopper] int,
              [press_before] decimal(10, 2),
              [press_after] decimal(10, 2),
              [press_final] decimal(10, 2),
              [grind_time] decimal(10, 2),
              [ext_time] decimal(10, 2),
              [water_qnty] decimal(10, 2),
              [water_temp] decimal(10, 2),
              [outlet_side] int,
              [stopped] int,
              [milk_temp] decimal(10, 2),
              [steam_pressure] decimal(10, 2),
              [grind_adjust_left] decimal(10, 2),
              [grind_adjust_right] decimal(10, 2),
              [milk_time] decimal(10, 2),
              [boiler_temp] decimal(10, 2)
            );
            
            PRINT 'Created table FactProductRun';
        END
        ELSE
        BEGIN
            PRINT 'Table FactProductRun already exists';
        END
        
        -- Add foreign key constraints with CORRECTED column names
        -- Only creating the first few constraints that don't depend on renamed columns
        
        -- Foreign keys for FactMachineCleaning that don't involve renamed columns
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DimMachine')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DimMachine] 
            FOREIGN KEY ([machine_id]) REFERENCES [DimMachine] ([machine_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DimMachine constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DimDate')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DimDate] 
            FOREIGN KEY ([date_id]) REFERENCES [DimDate] ([date_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DimDate constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DimTime_Start')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DimTime_Start] 
            FOREIGN KEY ([time_start_id]) REFERENCES [DimTime] ([time_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DimTime_Start constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DimTime_End')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DimTime_End] 
            FOREIGN KEY ([time_end_id]) REFERENCES [DimTime] ([time_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DimTime_End constraint';
        END
        
        -- Skipping constraints for renamed columns that would cause errors
        
        -- Add constraints for FactRinseOperation that don't involve renamed columns
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_DimMachine')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_DimMachine] 
            FOREIGN KEY ([machine_id]) REFERENCES [DimMachine] ([machine_id]);
            
            PRINT 'Added FK_FactRinseOperation_DimMachine constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_DimDate')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_DimDate] 
            FOREIGN KEY ([date_id]) REFERENCES [DimDate] ([date_id]);
            
            PRINT 'Added FK_FactRinseOperation_DimDate constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_DimTime')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_DimTime] 
            FOREIGN KEY ([time_id]) REFERENCES [DimTime] ([time_id]);
            
            PRINT 'Added FK_FactRinseOperation_DimTime constraint';
        END
        
        -- Skipping constraints for renamed columns that would cause errors
        
        -- Add constraints for FactInfoLog
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactInfoLog_DimMachine')
        BEGIN
            ALTER TABLE [FactInfoLog] ADD CONSTRAINT [FK_FactInfoLog_DimMachine] 
            FOREIGN KEY ([machine_id]) REFERENCES [DimMachine] ([machine_id]);
            
            PRINT 'Added FK_FactInfoLog_DimMachine constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactInfoLog_DimDate')
        BEGIN
            ALTER TABLE [FactInfoLog] ADD CONSTRAINT [FK_FactInfoLog_DimDate] 
            FOREIGN KEY ([date_id]) REFERENCES [DimDate] ([date_id]);
            
            PRINT 'Added FK_FactInfoLog_DimDate constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactInfoLog_DimTime')
        BEGIN
            ALTER TABLE [FactInfoLog] ADD CONSTRAINT [FK_FactInfoLog_DimTime] 
            FOREIGN KEY ([time_id]) REFERENCES [DimTime] ([time_id]);
            
            PRINT 'Added FK_FactInfoLog_DimTime constraint';
        END
        
        -- Add constraints for FactProductRun that don't involve renamed columns
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_DimMachine')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_DimMachine] 
            FOREIGN KEY ([machine_id]) REFERENCES [DimMachine] ([machine_id]);
            
            PRINT 'Added FK_FactProductRun_DimMachine constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_DimDate')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_DimDate] 
            FOREIGN KEY ([date_id]) REFERENCES [DimDate] ([date_id]);
            
            PRINT 'Added FK_FactProductRun_DimDate constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_DimTime')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_DimTime] 
            FOREIGN KEY ([time_id]) REFERENCES [DimTime] ([time_id]);
            
            PRINT 'Added FK_FactProductRun_DimTime constraint';
        END
        
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_DimProductType')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_DimProductType] 
            FOREIGN KEY ([product_type_id]) REFERENCES [DimProductType] ([product_type_id]);
            
            PRINT 'Added FK_FactProductRun_DimProductType constraint';
        END

		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_DimRinseStatus_Left')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_DimRinseStatus_Left] 
            FOREIGN KEY ([status_left]) REFERENCES [DimRinseStatus] ([rinse_status_id]);
            
            PRINT 'Added FK_FactRinseOperation_DimRinseStatus_Left constraint';
        END
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_DimRinseStatus_Right')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_DimRinseStatus_Right] 
            FOREIGN KEY ([status_right]) REFERENCES [DimRinseStatus] ([rinse_status_id]);
            
            PRINT 'Added FK_FactRinseOperation_DimRinseStatus_Right constraint';
        END
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_TabsStatus_Left')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_TabsStatus_Left] 
            FOREIGN KEY ([tabs_status_left]) REFERENCES [DimTabsStatus] ([tabs_status_id]);
            
            PRINT 'Added FK_FactMachineCLeaning_TabsStatus_Left constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_TabsStatus_Right')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_TabsStatus_Right] 
            FOREIGN KEY ([tabs_status_right]) REFERENCES [DimTabsStatus] ([tabs_status_id]);
            
            PRINT 'Added FK_FactMachineCleaning_TabsStatus_Right constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_PowderStatus')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_PowderStatus] 
            FOREIGN KEY ([powder_clean_status]) REFERENCES [DimPowderStatus] ([powder_status_id]);
            
            PRINT 'Added FK_FactMachineCleaning_PowderStatus constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_RinseType')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_RinseType] 
            FOREIGN KEY ([rinse_type]) REFERENCES [DimRinseType] ([rinse_type_id]);
            
            PRINT 'Added FK_FactRinseOperation_RinseType constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_OutletSide')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_OutletSide] 
            FOREIGN KEY ([outlet_side]) REFERENCES [DimOutletSide] ([outlet_side_id]);
            
            PRINT 'Added FK_FactProductRun_OutletSide constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_Stopped')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_Stopped] 
            FOREIGN KEY ([stopped]) REFERENCES [DimStopped] ([stopped_id]);
            
            PRINT 'Added FK_FactProductRun_Stopped constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DetergentStatus_Left')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DetergentStatus_Left] 
            FOREIGN KEY ([detergent_status_left]) REFERENCES [DimDetergentStatus] ([detergent_status_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DetergentStatus_Left constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactMachineCleaning_DetergentStatus_Right')
        BEGIN
            ALTER TABLE [FactMachineCleaning] ADD CONSTRAINT [FK_FactMachineCleaning_DetergentStatus_Right] 
            FOREIGN KEY ([detergent_status_right]) REFERENCES [DimDetergentStatus] ([detergent_status_id]);
            
            PRINT 'Added FK_FactMachineCleaning_DetergentStatus_Right constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactProductRun_BeanHopper')
        BEGIN
            ALTER TABLE [FactProductRun] ADD CONSTRAINT [FK_FactProductRun_BeanHopper] 
            FOREIGN KEY ([bean_hopper]) REFERENCES [DimBeanHopper] ([bean_hopper_id]);
            
            PRINT 'Added FK_FactProductRun_BeanHopper constraint';
        END
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_NozzleStatus_Left')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_NozzleStatus_Left] 
            FOREIGN KEY ([nozzle_status_left]) REFERENCES [DimNozzleStatus] ([nozzle_status_id]);
            
            PRINT 'Added FK_FactRinseOperation_NozzleStatus_Left constraint';
        END	
		IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_FactRinseOperation_NozzleStatus_Right')
        BEGIN
            ALTER TABLE [FactRinseOperation] ADD CONSTRAINT [FK_FactRinseOperation_NozzleStatus_Right] 
            FOREIGN KEY ([nozzle_status_right]) REFERENCES [DimNozzleStatus] ([nozzle_status_id]);
            
            PRINT 'Added FK_FactRinseOperation_NozzleStatus_Right constraint';
        END	
        
        -- Skipping constraints for renamed columns that would cause errors
        
        PRINT 'Data warehouse schema creation with corrected column names completed successfully.';
        
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
        EXEC sp_executesql N'RAISERROR(''DW Schema Creation Error: %s'', 10, 1, @ErrorMessage) WITH LOG', 
                      N'@ErrorMessage NVARCHAR(4000)', @ErrorMessage;
            
        RETURN -1;
    END CATCH
    
    RETURN 0;
END;
