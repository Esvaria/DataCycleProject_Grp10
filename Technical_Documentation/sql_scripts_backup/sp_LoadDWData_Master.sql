ALTER PROCEDURE [dbo].[sp_LoadDWData_Master]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RC INT;
    
    -- Step 1: First, ensure dimensions are complete
    -- This adds any missing IDs from the source data
    EXEC @RC = sp_EnsureDimensionCompleteness;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to check dimension tables.';
        -- Continue anyway
    END
    
    -- Step 2: Then, load dimension tables with your proper definitions
    -- This will override the auto-added descriptions with your proper ones
    EXEC @RC = sp_LoadDimensionTables;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load dimension tables.';
        -- Continue anyway
    END
    
    -- Step 3: Load FactMachineCleaning
    EXEC @RC = sp_LoadFactMachineCleaning;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactMachineCleaning table.';
        -- Continue anyway
    END
    
    -- Step 4: Load FactRinseOperation
    EXEC @RC = sp_LoadFactRinseOperation;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactRinseOperation table.';
        -- Continue anyway
    END
    
    -- Step 5: Load FactInfoLog
    EXEC @RC = sp_LoadFactInfoLog;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactInfoLog table.';
        -- Continue anyway
    END
    
    -- Step 6: Load FactProductRun
    EXEC @RC = sp_LoadFactProductRun;
    IF @RC <> 0
    BEGIN
        PRINT 'Failed to load FactProductRun table.';
        -- Continue anyway
    END
    
    PRINT 'Data warehouse ETL process completed.';
    RETURN 0;
END;