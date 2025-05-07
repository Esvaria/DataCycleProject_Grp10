# GitHub Actions Workflow Documentation

This document provides detailed information about the GitHub Actions workflow that automates the ETL process for the machine data import system.

## Overview

The GitHub Actions workflow automates the following tasks:

1. Download raw data files from the SMB server
2. Process the raw data into Bronze format
3. Clean and validate the data into Silver format
4. Trigger SQL Server stored procedures to import the data into the database

## Prerequisites

- A self-hosted GitHub Actions runner on the SQL Server machine (see how in [the official Github documentation](https://https://docs.github.com/en/actions/hosting-your-own-runners/managing-self-hosted-runners/adding-self-hosted-runners))
- Python 3.9+ installed on the runner machine
- SQL Server access from the runner
- GitHub repository with access to the runner
- Required Python packages: pandas, smbprotocol
- SQL Server configured with the machine_data database and stored procedures

## Workflow Configuration

The system uses two GitHub Actions workflows:

### 1. Execute Eversys Scripts (Data ETL Process)

This workflow runs the ETL (Extract, Transform, Load) process to process machine data. It executes the Python scripts that handle downloading and processing the data from the source systems through to the final format ready for SQL Server import.

```yaml
name: Execute Eversys Scripts
on:
  schedule:
    - cron: "*/30 * * * *"  # Runs every 30 minutes
  workflow_dispatch:
jobs:
  run-eversys-scripts:
    runs-on: [self-hosted]
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4
      - name: Run Download Script
        run: python downloadeversysfiles.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Extract Script
        run: python extracteversysdata.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Clean Script
        run: python silvercleaningscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Info Script
        run: python silverinfoscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Rinse Script
        run: python silverrinsescript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
      - name: Run Silver Product Script
        run: python silverproductscript.py
        working-directory: C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10
```

### 2. Deploy on VM (Code Deployment Process)

This workflow manages code deployment when changes are pushed to the repository:

```yaml
name: Deploy on VM
on:
  push:
    branches: [ "main" ]
jobs:
  deploy:
    runs-on: self-hosted
    steps:
      - name: Check out repository
        uses: actions/checkout@v3
      - name: Pull latest changes from GitHub
        run: |
          cd C:/Users/Administrator/DataCycleProject/DataCycleProject_Grp10 
          git pull origin main
```

### Script Directory and Workflow Actions

Both workflows operate on scripts in a fixed directory on the server:
- Working directory: `C:\Users\Administrator\DataCycleProject\DataCycleProject_Grp10`

The ETL workflow executes these Python scripts in sequence:
1. `downloadeversysfiles.py` - Downloads data from the SMB server
2. `extracteversysdata.py` - Transforms data to Bronze format
3. `silvercleaningscript.py` - Processes cleaning logs to Silver format
4. `silverinfoscript.py` - Processes info logs to Silver format
5. `silverrinsescript.py` - Processes rinse logs to Silver format
6. `silverproductscript.py` - Processes product logs to Silver format

## Workflow Schedules

The workflows are configured with these schedules:

1. **ETL Workflow (Execute Eversys Scripts)**:
   - Automatically every 30 minutes (via the cron schedule: `*/30 * * * *`)
   - Manually when triggered via the GitHub Actions interface (using `workflow_dispatch`)

2. **Deployment Workflow (Deploy on VM)**:
   - Automatically triggered on every push to the main branch
   - Updates the code on the VM to the latest version from the repository

To adjust the schedule, modify the cron expression in the workflow file. Here are some common examples:

- Every hour: `0 * * * *`
- Every day at midnight: `0 0 * * *`
- Every Monday at 9 AM: `0 9 * * 1`

## Monitoring the Workflows

### Viewing Workflow Runs

1. Go to the **Actions** tab in your GitHub repository
2. Select either the **Execute Eversys Scripts** or **Deploy on VM** workflow
3. View the list of workflow runs
4. Click on a specific run to see detailed logs

### Workflow Run Logs

Each step in the workflows generates logs that can be expanded to see detailed output:

#### ETL Workflow Logs
1. **Checkout step**: Shows repository checkout status
2. **Download script step**: Shows the list of files downloaded from the SMB server
3. **Extract script step**: Shows the processing of raw data into Bronze format
4. **Silver processing steps**: Show validation warnings and counts of processed rows for each data type

#### Deployment Workflow Logs
1. **Checkout step**: Shows repository checkout status
2. **Pull changes step**: Shows the git pull operation results

### Troubleshooting

If a workflow run fails, check the logs for the specific step that failed:

1. **Download failures**: Check SMB connectivity and credentials
2. **Processing failures**: Check for file format issues or data validation errors
3. **Deployment failures**: Check for git repository issues or file permissions

### Email Notifications

Configure email notifications for workflow failures:

1. Go to your GitHub account settings
2. Select **Notifications**
3. Under **Actions**, select your preferred notification settings for workflow runs

## Integration with SQL Server

### Workflow and SQL Server Integration

The ETL workflow prepares the data files in the Silver format that are used by the SQL Server Agent jobs to import data into the database. The workflow itself does not directly execute SQL commands or stored procedures.

The main responsibilities of each system are:

- **GitHub Actions workflows**: 
  - Download raw data from SMB server
  - Process data into Bronze and Silver formats
  - Maintain code deployment with latest versions

- **SQL Server Agent jobs**:
  - Import data from Silver format files into the database
  - Perform database maintenance and backups
  - Provide scheduling for database operations

### Synchronization Considerations

To ensure smooth operation between the GitHub Actions workflows and SQL Server Agent jobs:

1. **Schedule Coordination**: The ETL workflow runs every 30 minutes to provide fresh data, while SQL Server Agent jobs can be scheduled to run hourly or at other intervals that don't conflict.

2. **File Access**: Both systems need proper access to the shared file locations where the Silver data files are stored.

3. **Error Handling**: Each system has its own logging and error handling mechanisms. Check both GitHub Actions logs and SQL Server Agent job history when troubleshooting.

## Security Considerations

### File System Access

Ensure that:

1. The GitHub runner has appropriate permissions to access and modify files in the working directory
2. The SQL Server service account has read access to the Silver data file locations
3. Network share permissions are properly configured for SMB access

### Code Repository Security

For the deployment workflow:

1. Restrict push access to the main branch to authorized team members
2. Use branch protection rules to enforce code review before merging
3. Regularly audit the repository access permissions

## Summary

The GitHub Actions workflows automate both the ETL process and code deployment for the machine data import system. By running on a self-hosted runner, the workflows have direct access to the file system and network resources needed to operate the system effectively.

The workflows provide:

1. **ETL Process (Execute Eversys Scripts)**:
   - Regular execution every 30 minutes to maintain fresh data
   - Manual trigger option for on-demand processing
   - Sequential execution of all processing steps
   - Preparation of data for SQL Server import

2. **Code Deployment (Deploy on VM)**:
   - Automated deployment on code changes
   - Keeps the production scripts up-to-date
   - Simple git-based deployment mechanism

These workflows, combined with the SQL Server Agent jobs that handle the database operations, create a comprehensive automation system that ensures reliable and consistent operation of the machine data import process with minimal manual intervention.