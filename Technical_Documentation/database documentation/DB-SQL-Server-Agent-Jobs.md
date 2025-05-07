# SQL Server Agent Jobs Documentation

This document outlines the SQL Server Agent jobs required for the machine data import system, including the database backup procedure.

## Summary

This document provides the SQL scripts needed to create the following SQL Server Agent jobs:

1. **Individual Import Jobs**:
   - Import Cleaning Logs
   - Import Rinse Logs
   - Import Product Logs
   - Import Info Logs

2. **Master Import Job**: Runs all import procedures in sequence

3. **Database Backup Job**: Performs daily full backups and maintains a 7-day backup history

4. **Database Maintenance Job**: Performs weekly index rebuilds and statistics updates

These jobs ensure that the machine data import system operates automatically and consistently, with proper database maintenance and backup protocols in place.


## Individual Import Jobs

First, create individual jobs for each import procedure:

### 1. Import Cleaning Logs Job

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create job for cleaning logs
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Import Cleaning Logs', 
    @enabled=1, 
    @notify_level_eventlog=0, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Imports cleaning logs from data file', 
    @category_name=N'Data Collector', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the stored procedure
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Import Procedure', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportCleaningLogs]', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Import Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=4, -- Every hour
    @freq_subday_interval=1, -- 1 hour interval
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date 
    @active_end_date=99991231, -- End date 
    @active_start_time=0, -- Start time (midnight)
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

### 2. Import Rinse Logs Job

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create job for rinse logs
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Import Rinse Logs', 
    @enabled=1, 
    @notify_level_eventlog=0, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Imports rinse logs from data file', 
    @category_name=N'Data Collector', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the stored procedure
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Import Procedure', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportRinseLogs]', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Import Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=4, -- Every hour
    @freq_subday_interval=1, -- 1 hour interval
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date 
    @active_end_date=99991231, -- End date 
    @active_start_time=0, -- Start time (midnight)
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

### 3. Import Product Logs Job

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create job for product logs
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Import Product Logs', 
    @enabled=1, 
    @notify_level_eventlog=0, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Imports product logs from data file', 
    @category_name=N'Data Collector', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the stored procedure
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Import Procedure', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportProductLogs]', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Import Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=4, -- Every hour
    @freq_subday_interval=1, -- 1 hour interval
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date 
    @active_end_date=99991231, -- End date 
    @active_start_time=0, -- Start time (midnight)
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

### 4. Import Info Logs Job

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create job for info logs
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Import Info Logs', 
    @enabled=1, 
    @notify_level_eventlog=0, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Imports info logs from data file', 
    @category_name=N'Data Collector', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the stored procedure
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Execute Import Procedure', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, 
    @on_success_step_id=0, 
    @on_fail_action=2, 
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportInfoLogs]', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Import Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=4, -- Every hour
    @freq_subday_interval=1, -- 1 hour interval
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date 
    @active_end_date=99991231, -- End date 
    @active_start_time=0, -- Start time (midnight)
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

## Master Import Job

Create a master job to run all import procedures in sequence:

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create master job
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Master Import Job', 
    @enabled=1, 
    @notify_level_eventlog=0, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Runs all import jobs in sequence', 
    @category_name=N'Data Collector', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job steps to execute each import job
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Import Cleaning Logs', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=3, -- Go to next step
    @on_success_step_id=0, 
    @on_fail_action=3, -- Go to next step even on failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportCleaningLogs]', 
    @database_name=N'machine_data', 
    @flags=0

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Import Rinse Logs', 
    @step_id=2, 
    @cmdexec_success_code=0, 
    @on_success_action=3, -- Go to next step
    @on_success_step_id=0, 
    @on_fail_action=3, -- Go to next step even on failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportRinseLogs]', 
    @database_name=N'machine_data', 
    @flags=0

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Import Product Logs', 
    @step_id=3, 
    @cmdexec_success_code=0, 
    @on_success_action=3, -- Go to next step
    @on_success_step_id=0, 
    @on_fail_action=3, -- Go to next step even on failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportProductLogs]', 
    @database_name=N'machine_data', 
    @flags=0

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Import Info Logs', 
    @step_id=4, 
    @cmdexec_success_code=0, 
    @on_success_action=1, -- Quit with success
    @on_success_step_id=0, 
    @on_fail_action=2, -- Quit with failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC [machine_data].[dbo].[sp_ImportInfoLogs]', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Master Import Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=4, -- Every hour
    @freq_subday_interval=6, -- 6 hour interval (run 4 times per day)
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date 
    @active_end_date=99991231, -- End date 
    @active_start_time=0, -- Start time (midnight)
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

## Database Backup Job

Create a job to perform daily database backups:

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create backup job
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Database Backup - machine_data', 
    @enabled=1, 
    @notify_level_eventlog=2, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Performs daily full backup of the machine_data database', 
    @category_name=N'Database Maintenance', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to execute the backup
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Full Backup', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=1, -- Quit with success
    @on_success_step_id=0, 
    @on_fail_action=2, -- Quit with failure
    @on_fail_step_id=0, 
    @retry_attempts=1, 
    @retry_interval=5, -- Retry after 5 minutes
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'DECLARE @BackupPath NVARCHAR(255)
DECLARE @BackupFile NVARCHAR(255)
DECLARE @DateTime NVARCHAR(20)

-- Format date/time for filename
SET @DateTime = REPLACE(CONVERT(NVARCHAR, GETDATE(), 112) + ''_'' + 
                REPLACE(CONVERT(NVARCHAR, GETDATE(), 108),'':'',''''), '' '', '''')

-- Set backup path and filename
SET @BackupPath = ''C:\SQLBackups\''
SET @BackupFile = @BackupPath + ''machine_data_'' + @DateTime + ''.bak''

-- Create backup directory if it doesn''t exist
EXEC master.dbo.xp_create_subdir @BackupPath

-- Perform full backup
BACKUP DATABASE [machine_data] TO DISK = @BackupFile
WITH COMPRESSION, INIT, NAME = ''machine_data-Full Database Backup'', STATS = 10

-- Delete backups older than 7 days
DECLARE @cmd NVARCHAR(500)
SET @cmd = ''forfiles /p "'' + @BackupPath + ''" /s /m machine_data_*.bak /d -7 /c "cmd /c del @path"''
EXEC master.dbo.xp_cmdshell @cmd, NO_OUTPUT', 
    @database_name=N'master', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job (daily at 1:00 AM)
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Daily Backup Schedule', 
    @enabled=1, 
    @freq_type=4, -- Daily
    @freq_interval=1, -- Every day
    @freq_subday_type=1, -- Once per day
    @freq_subday_interval=0, 
    @freq_relative_interval=0, 
    @freq_recurrence_factor=0, 
    @active_start_date=20250101, -- Start date
    @active_end_date=99991231, -- End date
    @active_start_time=10000, -- 1:00 AM
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

## Database Maintenance Job

Create a job to perform regular database maintenance:

```sql
USE [msdb]
GO

BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobId BINARY(16)

-- Create maintenance job
EXEC @ReturnCode = msdb.dbo.sp_add_job @job_name=N'Database Maintenance - machine_data', 
    @enabled=1, 
    @notify_level_eventlog=2, 
    @notify_level_email=0, 
    @notify_level_netsend=0, 
    @notify_level_page=0, 
    @delete_level=0, 
    @description=N'Performs weekly index and statistics maintenance', 
    @category_name=N'Database Maintenance', 
    @owner_login_name=N'sa', @job_id = @jobId OUTPUT

-- Add job step to rebuild indexes
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Rebuild Indexes', 
    @step_id=1, 
    @cmdexec_success_code=0, 
    @on_success_action=3, -- Go to next step
    @on_success_step_id=0, 
    @on_fail_action=2, -- Quit with failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC sp_MSforeachtable @command1="PRINT ''?'' ; ALTER INDEX ALL ON ? REBUILD"', 
    @database_name=N'machine_data', 
    @flags=0

-- Add job step to update statistics
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Update Statistics', 
    @step_id=2, 
    @cmdexec_success_code=0, 
    @on_success_action=1, -- Quit with success
    @on_success_step_id=0, 
    @on_fail_action=2, -- Quit with failure
    @on_fail_step_id=0, 
    @retry_attempts=0, 
    @retry_interval=0, 
    @os_run_priority=0, @subsystem=N'TSQL', 
    @command=N'EXEC sp_updatestats', 
    @database_name=N'machine_data', 
    @flags=0

-- Set the job server
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1

-- Create a schedule for the job (weekly on Sunday at 3:00 AM)
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Weekly Maintenance Schedule', 
    @enabled=1, 
    @freq_type=8, -- Weekly
    @freq_interval=1, -- Sunday
    @freq_subday_type=1, -- Once per day
    @freq_subday_interval=0, 
    @freq_relative_interval=0, 
    @freq_recurrence_factor=1, -- Every week
    @active_start_date=20250101, -- Start date
    @active_end_date=99991231, -- End date
    @active_start_time=30000, -- 3:00 AM
    @active_end_time=235959 -- End time (11:59:59 PM)

-- Assign the job to the server
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'

COMMIT TRANSACTION
```

## Monitoring Job History

To monitor the execution history of the jobs, you can use the following query:

```sql
-- Check job history for the last 24 hours
SELECT 
    j.name AS 'Job Name',
    h.run_date,
    h.run_time,
    CASE h.run_status
        WHEN 0 THEN 'Failed'
        WHEN 1 THEN 'Succeeded'
        WHEN 2 THEN 'Retry'
        WHEN 3 THEN 'Canceled'
        WHEN 4 THEN 'In Progress'
    END AS 'Status',
    CAST(((h.run_duration / 10000 * 3600) + 
          ((h.run_duration % 10000) / 100 * 60) + 
          (h.run_duration % 100)) AS VARCHAR(10)) + ' seconds' AS 'Duration',
    h.message
FROM msdb.dbo.sysjobhistory h
JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
WHERE j.name LIKE '%machine_data%'
AND h.run_date >= CONVERT(INT, CONVERT(VARCHAR(8), DATEADD(day, -1, GETDATE()), 112))
ORDER BY h.run_date DESC, h.run_time DESC;
```