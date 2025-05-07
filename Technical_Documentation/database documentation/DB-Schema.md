# Machine Data - Relational Database Schema

The relational database schema captures the operational data from coffee machines, with `machine_names` as the central table connected to various log tables.

## Entity-Relationship Diagram

```mermaid
erDiagram
    machine_names ||--o{ cleaning_logs : has
    machine_names ||--o{ rinse_logs : has
    machine_names ||--o{ product_logs : has
    machine_names ||--o{ info_logs : has
    machine_names ||--o{ import_log : captures

    machine_names {
        int machine_id PK
        nvarchar name
    }

    cleaning_logs {
        int log_id PK
        int machine_id FK
        datetime timestamp_start
        datetime timestamp_end
        smallint powder_clean_status
        smallint tabs_status_left
        smallint tabs_status_right
        smallint detergent_status_left
        smallint detergent_status_right
        tinyint milk_pump_error_left
        tinyint milk_pump_error_right
        smallint milk_temp_left_1
        smallint milk_temp_left_2
        smallint milk_temp_right_1
        smallint milk_temp_right_2
        smallint milk_rpm_left_1
        smallint milk_rpm_left_2
        smallint milk_rpm_right_1
        smallint milk_rpm_right_2
        smallint milk_clean_temp_left_1
        smallint milk_clean_temp_left_2
        smallint milk_clean_temp_right_1
        smallint milk_clean_temp_right_2
        smallint milk_clean_rpm_left_1
        smallint milk_clean_rpm_left_2
        smallint milk_clean_rpm_right_1
        smallint milk_clean_rpm_right_2
        smallint milk_seq_cycle_left_1
        smallint milk_seq_cycle_left_2
        smallint milk_seq_cycle_right_1
        smallint milk_seq_cycle_right_2
    }

    rinse_logs {
        int log_id PK
        int machine_id FK
        datetime timestamp
        int rinse_type
        int flow_rate_left
        int flow_rate_right
        int status_left
        int status_right
        int pump_pressure
        int nozzle_flow_rate_left
        int nozzle_flow_rate_right
        int nozzle_status_left
        int nozzle_status_right
    }

    product_logs {
        int log_id PK
        int machine_id FK
        datetime timestamp
        float press_before
        float press_after
        float press_final
        float grind_time
        float ext_time
        int water_qnty
        int water_temp
        int prod_type
        float double_prod
        int bean_hopper
        float outlet_side
        float stopped
        int milk_temp
        float steam_pressure
        int grind_adjust_left
        int grind_adjust_right
        float milk_time
        int boiler_temp
    }

    info_logs {
        int log_id PK
        int machine_id FK
        datetime timestamp
        nvarchar number
        nvarchar typography
        float type_number
    }

    import_log {
        int id PK
        datetime log_date
        nvarchar message
        int error_number
        int error_line
    }
```

## Table Details

### machine_names
Central table that stores machine identification information.
- **machine_id** (int, PK): Unique identifier for each coffee machine
- **name** (nvarchar(100)): Descriptive name of the machine

### cleaning_logs
Records cleaning cycles performed by machines.
- **log_id** (int, PK): Unique log identifier
- **machine_id** (int, FK): Reference to machine_names
- **timestamp_start** (datetime): When cleaning cycle started
- **timestamp_end** (datetime): When cleaning cycle ended
- Multiple status and measurement fields for cleaning operations

### rinse_logs
Records rinsing operations performed by machines.
- **log_id** (int, PK): Unique log identifier
- **machine_id** (int, FK): Reference to machine_names
- **timestamp** (datetime): When rinsing operation occurred
- Various flow rates, pressures, and status indicators

### product_logs
Records beverage production operations.
- **log_id** (int, PK): Unique log identifier
- **machine_id** (int, FK): Reference to machine_names
- **timestamp** (datetime): When product was made
- Various measurements about pressure, temperature, and grind settings

### info_logs
Records information and error messages from machines.
- **log_id** (int, PK): Unique log identifier
- **machine_id** (int, FK): Reference to machine_names
- **timestamp** (datetime): When message was logged
- Details about the type and content of the message

### import_log
Tracks data import operations.
- **id** (int, PK): Unique log identifier
- **log_date** (datetime): When import operation occurred
- **message** (nvarchar): Operation message
- **error_number** (int): Error code if applicable
- **error_line** (int): Error line if applicable