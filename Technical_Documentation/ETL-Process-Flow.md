# Machine Data ETL Process Flow

This diagram illustrates the complete data processing pipeline from the source coffee machines to the final data warehouse.

```mermaid
flowchart TD
    subgraph "Data Sources"
        A[Coffee Machines] --> B[Raw .dat Files]
    end

    subgraph "ETL Process"
        subgraph "Extract"
            B -->|SMB Connection| C[EversysDatFiles]
            C -->|download_smb_data.py| D[Raw Data Files]
        end

        subgraph "Transform Bronze"
            D -->|process_bronze_data.py| E[Bronze Data Files]
            E -->|BronzeRawData/Cleaning| F1[Cleaning.dat]
            E -->|BronzeRawData/Rinse| F2[Rinse.dat]
            E -->|BronzeRawData/Product| F3[Product.dat]
            E -->|BronzeRawData/Info| F4[Info.dat]
        end

        subgraph "Transform Silver"
            F1 -->|process_silver_cleaning.py| G1[Silver_Cleaning.dat]
            F2 -->|process_silver_rinse.py| G2[Silver_Rinse.dat]
            F3 -->|process_silver_product.py| G3[Silver_Product.dat]
            F4 -->|process_silver_info.py| G4[Silver_Info.dat]
        end
    end

    subgraph "Operational Database (machine_data)"
        G1 -->|sp_ImportCleaningLogs| H1[cleaning_logs]
        G2 -->|sp_ImportRinseLogs| H2[rinse_logs]
        G3 -->|sp_ImportProductLogs| H3[product_logs]
        G4 -->|sp_ImportInfoLogs| H4[info_logs]
        H1 --> I[machine_names]
        H2 --> I
        H3 --> I
        H4 --> I
    end

    subgraph "Data Warehouse (DWmachines)"
        subgraph "Dimension Tables"
            J1[DimMachine]
            J2[DimDate]
            J3[DimTime]
            J4[DimProductType]
            J5[DimPowderStatus]
            J6[DimTabsStatus]
            J7[DimDetergentStatus]
            J8[DimRinseType]
            J9[DimRinseStatus]
            J10[DimNozzleStatus]
            J11[DimBeanHopper]
            J12[DimOutletSide]
            J13[DimStopped]
        end

        subgraph "Fact Tables"
            K1[FactMachineCleaning]
            K2[FactRinseOperation]
            K3[FactInfoLog]
            K4[FactProductRun]
        end

        I -->|sp_LoadDimensionTables| J1
        H1 -->|sp_LoadFactMachineCleaning| K1
        H2 -->|sp_LoadFactRinseOperation| K2
        H4 -->|sp_LoadFactInfoLog| K3
        H3 -->|sp_LoadFactProductRun| K4
        
        J1 --> K1
        J1 --> K2
        J1 --> K3
        J1 --> K4
        
        J2 --> K1
        J2 --> K2
        J2 --> K3
        J2 --> K4
        
        J3 --> K1
        J3 --> K2
        J3 --> K3
        J3 --> K4
    end

    subgraph "Automation"
        L1[GitHub Actions]
        L2[SQL Server Agent]
        
        L1 -->|Execute Eversys Scripts| C
        L1 -->|GitHub Actions| G1
        L1 -->|GitHub Actions| G2
        L1 -->|GitHub Actions| G3
        L1 -->|GitHub Actions| G4
        
        L2 -->|Import Jobs| H1
        L2 -->|Import Jobs| H2
        L2 -->|Import Jobs| H3
        L2 -->|Import Jobs| H4
        L2 -->|DW ETL Job| K1
        L2 -->|DW ETL Job| K2
        L2 -->|DW ETL Job| K3
        L2 -->|DW ETL Job| K4
    end
```

## Process Steps

1. **Data Collection**: Coffee machines generate operational data
2. **Raw Data Extraction**: Python scripts connect to SMB share and download .dat files
3. **Bronze Layer Transformation**: Files are consolidated by category
4. **Silver Layer Transformation**: Data is validated, cleaned, and standardized
5. **Operational Database Loading**: SQL stored procedures import data into relational tables
6. **Data Warehouse ETL**: Dimensional model is populated from operational data
7. **Automation**: GitHub Actions and SQL Server Agent jobs orchestrate the process

## Data Flow Path

1. Coffee Machines → Raw .dat Files → EversysDatFiles
2. EversysDatFiles → BronzeRawData → SilverRawData
3. SilverRawData → machine_data database (Operational)
4. machine_data → DWmachines database (Analytical)

## Automation Components

The process is fully automated through:

1. **GitHub Actions**:
   - "Execute Eversys Scripts" (runs every 30 minutes)
   - "Deploy on VM" (runs on code changes)

2. **SQL Server Agent Jobs**:
   - Individual import jobs for each log type
   - Master import job to run all procedures in sequence
   - Data warehouse ETL job
   - Database backup job

This end-to-end pipeline ensures that data flows from the coffee machines to the analytical data warehouse with minimal manual intervention.