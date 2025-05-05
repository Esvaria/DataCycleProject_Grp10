# DWmachines Data Warehouse Schema Documentation

## Overview

This document describes the `sp_CreateDWSchema_Fixed` stored procedure, which is designed to create and manage a data warehouse schema for coffee machine operational data. The warehouse follows a star schema design with dimension and fact tables.

## Stored Procedure: `sp_CreateDWSchema_Fixed`

### Purpose
Creates a complete data warehouse schema for tracking coffee machine operations, maintenance, and product runs.

### Database
`DWmachines`

### Schema Structure

#### Dimension Tables

| Table Name         | Description                        | Primary Key         |
| ------------------ | ---------------------------------- | ------------------- |
| DimMachine         | Stores machine details             | machine_id          |
| DimDate            | Calendar date dimensions           | date_id             |
| DimTime            | Time of day dimensions             | time_id             |
| DimProductType     | Product types produced by machines | product_type_id     |
| DimPowderStatus    | Powder dispensing status           | powder_status_id    |
| DimTabsStatus      | Cleaning tabs status               | tabs_status_id      |
| DimDetergentStatus | Detergent status                   | detergent_status_id |
| DimRinseType       | Types of rinse operations          | rinse_type_id       |
| DimRinseStatus     | Status of rinse operations         | rinse_status_id     |
| DimNozzleStatus    | Nozzle operational status          | nozzle_status_id    |
| DimBeanHopper      | Bean hopper information            | bean_hopper_id      |
| DimOutletSide      | Outlet side (left/right)           | outlet_side_id      |
| DimStopped         | Operations stopped status          | stopped_id          |

#### Fact Tables

| Table Name          | Description                            | Primary Key   | Foreign Keys                                    |
| ------------------- | -------------------------------------- | ------------- | ----------------------------------------------- |
| FactMachineCleaning | Records of machine cleaning operations | cleaning_id   | machine_id, date_id, time_start_id, time_end_id |
| FactRinseOperation  | Records of rinse operations            | rinse_id      | machine_id, date_id, time_id                    |
| FactInfoLog         | General information logs               | info_id       | machine_id, date_id, time_id                    |
| FactProductRun      | Records of product creation            | production_id | machine_id, date_id, time_id, product_type_id   |

### Key Fields in Fact Tables

#### FactMachineCleaning
- Tracks cleaning start and end times
- Records powder, tabs, and detergent status
- Monitors milk cleaning system parameters
- Measures milk temperature and RPM values

#### FactRinseOperation
- Tracks rinse type and status
- Records flow rates and pressures
- Monitors nozzle status and flow rates

#### FactInfoLog
- Captures machine events and notifications
- Includes type and typography information

#### FactProductRun
- Records product creation details
- Tracks pressure, grind time, extraction time
- Monitors water quantity, temperature
- Records milk parameters and boiler temperature

## Known Issues

1. Data type mismatch in foreign key constraints:
   - `FK_FactMachineCLeaning_TabsStatus_Left` has a type mismatch between `tabs_status_left` (varchar) and `tabs_status_id` (int)
   - Similar mismatches may exist in other foreign key relationships

2. A typo exists in the constraint name: `FK_FactMachineCLeaning_TabsStatus_Left` (uppercase 'L')

3. Incorrect PRINT message when adding the `FK_FactMachineCLeaning_TabsStatus_Left` constraint

## Recommendations

1. Standardize data types for all primary/foreign key relationships
2. Fix typos in constraint names
3. Correct PRINT messages to reflect the actual constraints being added

## Error Handling

The procedure includes comprehensive error handling:
- Uses TRY/CATCH blocks to capture and report errors
- Logs errors to the SQL Server Error Log
- Returns exit code -1 on failure, 0 on success
