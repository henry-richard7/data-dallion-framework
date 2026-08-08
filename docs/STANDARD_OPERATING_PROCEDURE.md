# Standard Operating Procedure (SOP): DataDallion Framework

This document outlines the standard procedures for configuring, executing, and maintaining data pipelines using the **DataDallion Framework**. It covers the architecture, configuration parameters, metadata table structures, credential management, execution layers, and troubleshooting guides from both a **Business** and **Technical** standpoint.

---

## 1. Business Standpoint & Architecture Overview

### Business Rationale & Alignment
In modern enterprise data environments, hand-coding individual ingestion pipelines leads to high maintenance costs, inconsistent data quality, lack of auditability, and delayed time-to-market. The **DataDallion Framework** solves these issues by providing a **metadata-driven data orchestration engine** aligned with the **Medallion Architecture**.

By configuring dataset properties in a relational metadata store rather than writing custom ingestion scripts, the business achieves:
1. **Accelerated Time-to-Market:** Onboarding a new dataset requires only metadata registration (SQL inserts), reducing development time from days to minutes.
2. **Unified Data Governance:** Centralized management of dataset columns, quality rules, and processing logic.
3. **Regulatory & Compliance Enforcement:** Automated Key Management System (KMS) integration ensures credentials are never stored in plaintext. Strict data quality management (DQM) checks prevent non-compliant data from reaching analytical layers.
4. **Data Auditability & Lineage:** Granular execution logs capture batch status, processed files, processing durations, and data quality failure rates for comprehensive operational oversight.

### Business Medallion Layers
The framework processes datasets sequentially through three standard business layers:
* **Bronze Layer (Raw landing):** Extracts data from external source endpoints (APIs, S3, SFTP, Databases, Salesforce) and lands them in raw format.
* **Silver Layer (Standardization & Validation):** Applies uniform cleaning rules (trimming, padding, substring extraction, lower/upper casing) and enforces strict validation checks to quarantine dirty records.
* **Gold Layer (Business Analytics):** Joins, aggregates, and processes clean tables into business-ready reports, applying **Slowly Changing Dimension (SCD) Type 2** logic to track historical changes.

### Business Data & Metadata Process Flow

```mermaid
graph TD
    A[Business / Data Engineers] -->|1. Configure Metadata SQL| B[Relational Metastore]
    B -->|Provides Schema & Configuration| C[DataDallion Engine]
    
    subgraph Ingestion & Processing
        C -->|2. Parallel Extraction| D[Bronze Layer: Raw Landing Delta]
        D -->|3. Standardize & Cast| E[Silver Layer: Cleansed Data]
        E -->|4. Run DQM Rules| F{Validation Pass?}
        F -->|No: Error Threshold Exceeded| G[Pipeline Halted & Alerted]
        F -->|No: Within Threshold| H[Quarantine: DQM Error Table]
        F -->|Yes| I[Silver Layer: Clean Staging / Publish Tables]
        I -->|5. Aggregate & Join| J[Gold Layer: Business Analytics SCD Type 2]
    end

    subgraph Monitoring & Compliance
        C -->|Write Execution Logs| K[Operational Log Tables]
        H -->|Audit Quality Metrics| L[Compliance & Quality Reports]
        J -->|Query Business Intelligence| M[PowerBI / Tableau / Analytics]
    end
```

---

## 2. Technical Standpoint & Execution Model

The framework runs as a Spark-based orchestration engine. A master runner script reads the relational metadata tables, fetches encrypted credentials from a Key Management System (KMS), and triggers PySpark tasks.

### Core Technical Workflow
1. **Parallel Execution:** Uses `ThreadPoolExecutor` to handle multiple datasets concurrently.
2. **Incremental File Ingestion:** Compares the files in the source folder (`inbound_location`) against previous successful raw logs (`logRawProcessDtl`) using regex pattern matching. It skips files that have already been processed successfully.
3. **Secret Resolution:** Resolves credentials dynamically at runtime using prefixes (e.g. `keyvault:...`, `secretsmanager:...`, `encrypted:...`) so credentials are never exposed in plaintext in the database or execution code.
4. **Schema Caster:** Casts incoming unstructured column types to match the database schemas specified in the metadata.
5. **DQM Engine:** Translates DQM rules configured in the database to dynamic Spark SQL expressions, evaluates them in a single pass to optimize Spark execution plans, and filters failed records.
6. **SCD Type 2 Merge:** Evaluates primary keys, calculates a SHA-256 checksum of business columns, compares it to the active historical records, and closes modified entries while inserting new ones.

### Technical Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant R as Runner Instance (Python)
    participant DB as Relational Metastore (SQLite/PostgreSQL)
    participant KMS as KMS Provider (AWS/Azure/Vault)
    participant Src as External Source System
    participant S as Spark Driver (PySpark)
    participant DL as Delta Lake Storage

    R->>DB: Fetch ctlDatasetMaster & ctlDataAcquisitionDetail configurations
    DB-->>R: Return dataset execution configs
    R->>DB: Fetch Connection Credential details
    DB-->>R: Return encrypted/prefixed credentials
    R->>KMS: Request decryption for prefixed parameters
    KMS-->>R: Return decrypted credentials
    R->>Src: Request data extraction (parallel worker threads)
    Src-->>R: Download source files to inbound landing directory
    R->>S: Invoke PerformBronze (Raw Table Ingest)
    S->>DL: Read raw inbound files, write to Bronze Delta Table
    S->>DB: Insert logs to logRawProcessDtl & logDataAcquisitionDetail
    R->>S: Invoke SilverLayerProcess (Standardization & Quality Validation)
    S->>DL: Read Bronze Delta Table
    S->>S: Apply ctlDataStandardisationDtl transformations
    S->>S: Run ctlDqmMasterDtl validation checks (single Spark plan)
    alt Quality Critical check failed
        S-->>R: Throw exception (halt pipeline run)
    else Quality Warning checks failed
        S->>DL: Write failed rows to dqm_error_location
        S->>DB: Log counts to logDqmDtl
    end
    S->>DL: Write valid records to staging_location & publish_location
    S->>DB: Log standardization metrics to logDataStandardisationDtl
    R->>S: Invoke GoldLayerProcess (Joins & SCD Type 2 Historical Tracking)
    S->>DL: Read Silver staging clean data
    S->>S: Execute joins/aggregations from ctlTransformationDependencyMaster
    S->>DL: Perform Delta Lake SCD Type 2 MERGE into Gold Tables
    S->>DB: Log transformation completion status to logTransformationDtl
```

---

## 3. Relational Metadata Schema (All 13 Tables)

All configuration variables and logs are stored inside a relational metadata database (e.g. SQLite, PostgreSQL, MySQL). The schema consists of 8 Control Tables and 5 Log Tables.

### A. Control Tables

#### 1. `ctlDatasetMaster`
Central registry defining the table locations, partitioning keys, and table names across the Medallion layers.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `process_id` | `INTEGER` | Primary Key | Unique identifier for the data pipeline process. |
| `dataset_id` | `INTEGER` | Primary Key | Unique identifier for the dataset. |
| `dataset_name` | `TEXT` | Nullable | Name of the dataset. |
| `dataset_type` | `VARCHAR(50)`| Nullable | Type of dataset (e.g. `BRONZE`, `SILVER`, `GOLD`). |
| `inbound_location` | `TEXT` | Nullable | File system path where raw data is received. |
| `inbound_file_pattern` | `TEXT` | Nullable | Regex pattern used to match incoming source files. |
| `inbound_static_file_pattern`| `TEXT` | Nullable | Flag indicating if inbound file name is static ('Y'/'N'). |
| `inbound_file_format` | `TEXT` | Nullable | Format of the source file (e.g. `csv`, `json`, `xml`). |
| `inbound_file_delimiter` | `TEXT` | Nullable | Delimiter used in raw text files (e.g. `,`, `\t`). |
| `landing_location` | `TEXT` | Nullable | Path where raw data is stored in the Landing Layer. |
| `landing_table` | `TEXT` | Nullable | Table name for raw Delta storage. |
| `landing_partition_columns` | `TEXT` | Nullable | Partitioning strategy for the Landing Layer. |
| `data_standardisation_location` | `TEXT` | Nullable | Path where standardized dataset version is saved. |
| `data_standardisation_partition_columns` | `TEXT` | Nullable | Partitioning strategy for the Standardized Layer. |
| `dqm_error_location` | `TEXT` | Nullable | Directory path where invalid/failed rows are written. |
| `dqm_partition_columns` | `TEXT` | Nullable | Partitioning columns for DQM outputs. |
| `staging_location` | `TEXT` | Nullable | Path where clean validated data is staged. |
| `staging_table` | `TEXT` | Nullable | Table name for the Staging Layer. |
| `staging_partition_columns` | `TEXT` | Nullable | Partitioning strategy for the Staging Layer. |
| `transformation_location` | `TEXT` | Nullable | Path where the final transformed dataset is stored. |
| `transformation_table` | `TEXT` | Nullable | Table name for the Transformation Layer. |
| `transformation_partition_columns` | `TEXT` | Nullable | Partitioning strategy for the Transformation Layer. |
| `archive_location` | `TEXT` | Nullable | Archive path for historical/cold raw datasets. |
| `publish_location` | `TEXT` | Nullable | Final destination path where dataset is served. |
| `publish_table` | `TEXT` | Nullable | Consumable table name in the serving database. |
| `publish_partition_columns` | `TEXT` | Nullable | Partitioning strategy for the Published Layer. |
| `table_location_type` | `TEXT` | Default: `MANAGED` | Denotes metastore configuration model: `MANAGED` / `EXTERNAL`. |
| `external_storage_prefix` | `TEXT` | Nullable | Storage driver prefix (e.g., `s3a`, `s3`, `adls`). |

#### 2. `ctlDataAcquisitionDetail`
Defines source properties, outbound folders, extraction scripts, and download locations for the Bronze layer.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `process_id` | `INTEGER` | Primary Key | Unique ID representing the data acquisition process. |
| `pre_ingestion_dataset_id`| `INTEGER` | Primary Key | Dataset ID of the Bronze Layer. |
| `pre_ingestion_dataset_name`| `TEXT` | Nullable | Human readable name of the Bronze dataset. |
| `outbound_source_platform`| `TEXT` | Nullable | Outbound server platform (e.g. `S3`, `SFTP`, `API`, `DATABASE`, `SALESFORCE`). |
| `credentials_identifier` | `TEXT` | Nullable | Foreign key referencing credentials to retrieve. |
| `outbound_source_location`| `TEXT` | Nullable | Remote directory path or DB schema name. |
| `outbound_source_file_pattern_static`| `TEXT` | Nullable | Flag indicating static naming schema ('Y'/'N'). |
| `outbound_source_file_pattern`| `TEXT` | Nullable | Remote file matching pattern with date placeholders. |
| `outbound_source_file_format`| `TEXT` | Nullable | Format type of file on source server (e.g. `csv`, `json`). |
| `outbound_file_delimiter` | `TEXT` | Nullable | Delimiter used on outbound files. |
| `query` | `TEXT` | Nullable | SQL query string to run on target source DB (if applicable). |
| `columns` | `TEXT` | Nullable | Comma-separated Salesforce object field list (if applicable). |
| `inbound_location` | `TEXT` | Nullable | Local/S3 storage landing folder path for raw files. |

#### 3. `ctlDataAcquisitionConnectionMaster`
Stores core network paths, endpoints, and credentials for source database instances, S3, SFTP, and API environments.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `outbound_source_platform`| `VARCHAR(4000)`| Primary Key | Code representation of platform (e.g. `SFTP`, `S3`, `DATABASE`). |
| `credentials_identifier` | `VARCHAR(4000)`| Primary Key | Unique identifier string for source connection credentials. |
| `connection_config` | `TEXT` | Nullable | JSON string storing parameters (host, user, port, token, etc.). |
| `ssh_private_key` | `TEXT` | Nullable | RSA private key contents for SFTP SSH authentication. |

#### 4. `ctlApiConnectionsDtl`
Tracks API connections including parameters, HTTP headers, authentication protocols, and OAuth tokens.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq_no` | `INTEGER` | Primary Key, Auto-Inc | Unique identifier sequence. |
| `pre_ingestion_dataset_id`| `INTEGER` | Nullable | Dataset ID of the associated raw/Bronze dataset. |
| `credentials_identifier` | `TEXT` | Nullable | Connection configuration reference credentials. |
| `type` | `TEXT` | Nullable | Type of request (e.g. `TOKEN`, `RESPONSE`, `CUSTOM`). |
| `token_url` | `TEXT` | Nullable | Endpoint URL to fetch OAuth tokens. |
| `auth_type` | `TEXT` | Nullable | Authentication method (e.g. `OAuth`, `JWT`, `Basic Auth`, `CUSTOM`). |
| `token_type` | `TEXT` | Nullable | Token prefix (e.g., `Bearer`). |
| `client_id` | `TEXT` | Nullable | Client API Key ID. |
| `client_secret` | `TEXT` | Nullable | Client API Secret credentials. |
| `username` | `TEXT` | Nullable | Basic authorization login user. |
| `password` | `TEXT` | Nullable | Basic authorization login password. |
| `issuer` | `TEXT` | Nullable | Token issuer URI for JWT credentials. |
| `scope` | `TEXT` | Nullable | API resource access scopes. |
| `private_key` | `TEXT` | Nullable | Private key for token validation or JWT signatures. |
| `token_path` | `TEXT` | Nullable | JSON path to locate token value in responses. |
| `method` | `TEXT` | Nullable | HTTP Method (e.g. `GET`, `POST`). |
| `url` | `TEXT` | Nullable | Main request endpoint URL. |
| `headers` | `TEXT` | Nullable | JSON string storing header dictionary parameters. |
| `params` | `TEXT` | Nullable | JSON string containing URL parameters. |
| `data` | `TEXT` | Nullable | Raw request body string. |
| `json_body` | `TEXT` | Nullable | JSON formatted request payload string. |
| `body_values` | `TEXT` | Nullable | Placeholder keys and values for dynamic injection. |
| `key_to_add_to_data` | `TEXT` | Nullable | Dynamic parameter key appended to output data files. |
| `ssl_verify` | `TEXT` | Default: `Y` | Flag to enforce SSL certificate validation ('Y'/'N'). |

#### 5. `CtlColumnMetadata`
Maps source columns to target columns, defining target data types, description notes, and JSON extraction paths.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `column_id` | `INTEGER` | Primary Key | Ordinal positioning of the field. |
| `table_name` | `VARCHAR(255)` | Primary Key | Logical table name where the field belongs. |
| `dataset_id` | `INTEGER` | Primary Key | Unique ID correlating to the parent dataset. |
| `column_name` | `VARCHAR(255)` | Primary Key | Logical target column name after ingestion. |
| `column_data_type` | `TEXT` | Nullable | Target casting type (e.g., `string`, `int`, `date`, `float`). |
| `column_date_format` | `TEXT` | Nullable | Input date format pattern (e.g. `yyyy-MM-dd HH:mm:ss`). |
| `column_description` | `TEXT` | Nullable | Context/description of the column data. |
| `column_json_mapping` | `TEXT` | Nullable | JSON path mapping used to extract values from raw JSON. |
| `source_column_name` | `TEXT` | Nullable | Original column name in the source dataset. |
| `column_sequence_number` | `INTEGER` | Nullable | Ordering rank position. |
| `column_tag` | `TEXT` | Nullable | Dashboard tags (e.g. `KPI`, `KPI-Filter`). |

#### 6. `ctlDataStandardisationDtl`
Contains columns-level cleaning rules (like regex formatting, casing, padding, etc.).

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `dataset_id` | `INTEGER` | Primary Key | Dataset reference mapping to standardisation target. |
| `column_name` | `VARCHAR(255)` | Primary Key | Target column mapping to standardize. |
| `function_name` | `TEXT` | Nullable | Rule name (e.g. `padding`, `trim`, `blank_conversion`, `replace`, `type_conversion`, `sub_string`). |
| `function_params` | `TEXT` | Nullable | JSON configuration dictionary mapping to functions. |

#### 7. `ctlDqmMasterDtl`
Tracks the data quality checks assigned to each dataset.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `qc_id` | `INTEGER` | Primary Key | Unique rule check identifier. |
| `process_id` | `INTEGER` | Nullable | Process ID linking this validation to a pipeline. |
| `dataset_id` | `INTEGER` | Nullable | Dataset being validated. |
| `column_name` | `TEXT` | Nullable | Target column (or comma-separated columns for `Unique`). |
| `qc_type` | `TEXT` | Nullable | Rule check validation type (e.g., `Null`, `Unique`, `Regex`). |
| `qc_param` | `TEXT` | Nullable | Configuration parameters (regex, bounds, integer). |
| `active_flag` | `TEXT` | Nullable | Flag denoting rule activity status ('Y'/'N'). |
| `qc_filter` | `TEXT` | Nullable | Sub-filtering criteria parameters to evaluate. |
| `criticality` | `TEXT` | Nullable | Execution response logic: `C` (Critical) or `W` (Warning). |
| `criticality_threshold_pct`| `INTEGER` | Nullable | Maximum allowed fail percentage before failure. |

#### 8. `ctlTransformationDependencyMaster`
Defines Gold layer transformations. Maps source staging tables to consolidated target tables.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `process_id` | `INTEGER` | Primary Key | Ingestion execution pipeline process ID. |
| `dataset_id` | `INTEGER` | Primary Key | Destination transformed table ID. |
| `dependent_dataset_id` | `INTEGER` | Primary Key | Target staging table ID used as input. |
| `transformation_sequence`| `INTEGER` | Nullable | Ranking run execution sequence order. |
| `transformation_type` | `TEXT` | Nullable | Type of operation: `JOIN`, `UNION`, `AGGREGATE`, `SINGLE`. |
| `join_how` | `TEXT` | Nullable | Spark join strategies (e.g., `left`, `inner`, `outer`). |
| `left_table_columns` | `TEXT` | Nullable | Comma-separated list of left join condition columns. |
| `right_table_columns` | `TEXT` | Nullable | Comma-separated list of right join condition columns. |
| `primary_keys` | `TEXT` | Nullable | Comma-separated list of primary keys for SCD Type 2 tracking. |
| `group_by_columns` | `TEXT` | Nullable | Group by parameters (for `AGGREGATE`). |
| `measure_columns` | `TEXT` | Nullable | Target JSON defining aggregation functions (for `AGGREGATE`). |
| `extra_values` | `TEXT` | Nullable | JSON string specifying columns to generate with Spark SQL. |
| `custom_transformation_type`| `TEXT` | Default: `SQL` | Standard query execution platform (e.g. `SQL`, `Python`). |
| `custom_transformation_query`| `TEXT` | Nullable | Custom SQL block to execute directly in Spark. |
| `custom_transformation_script_path`| `TEXT`| Nullable| Path to custom Spark script execution notebook. |

---

### B. Audit & Log Tables

#### 9. `logDataAcquisitionDetail`
Audit trail of external source file downloads.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq_no` | `INTEGER` | Primary Key, Identity| Auto-incremented sequence. |
| `batch_id` | `BIGINTEGER` | Nullable | Consolidated running process batch ID. |
| `run_date` | `DATE` | Nullable | Date when the file extraction was executed. |
| `process_id` | `INTEGER` | Nullable | Parent pipeline process ID. |
| `pre_ingestion_dataset_id`| `INTEGER` | Nullable | Database reference dataset ID. |
| `outbound_source_location`| `TEXT` | Nullable | File server directories fetched from. |
| `inbound_file_location` | `TEXT` | Nullable | Local download destination paths directory. |
| `status` | `VARCHAR(50)` | Default: `IN-PROGRESS`| Execution state (e.g., `SUCCEEDED`, `FAILED`). |
| `exception_details` | `TEXT` | Nullable | Error traceback stack trace details on failures. |
| `start_time` | `DATETIME` | Nullable | Ingestion start timestamp. |
| `end_time` | `DATETIME` | Nullable | Ingestion end timestamp. |

#### 10. `logRawProcessDtl`
Tracks the processing of raw files from the inbound folder into the raw Delta table.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `file_id` | `INTEGER` | Primary Key, Identity| Auto-incremented file ID. |
| `run_date` | `DATE` | Default: today | Process run date. |
| `batch_id` | `BIGINTEGER` | Nullable | Consolidated running batch ID. |
| `process_id` | `INTEGER` | Nullable | Associated pipeline process ID. |
| `dataset_id` | `INTEGER` | Nullable | Dataset target reference ID. |
| `source_file` | `VARCHAR(4000)`| Nullable | Name/path of the source file processed. |
| `landing_location` | `TEXT` | Nullable | Target raw Delta folder location. |
| `file_status` | `VARCHAR(50)` | Default: `IN-PROGRESS`| Processing status (e.g., `SUCCEEDED`, `FAILED`). |
| `exception_details` | `TEXT` | Nullable | Error traceback stack trace details on failures. |
| `file_process_start_time` | `DATETIME` | Nullable | Start timestamp. |
| `file_process_end_time` | `DATETIME` | Nullable | End timestamp. |

#### 11. `logDataStandardisationDtl`
Audit logs generated during column standardization formatting.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq_no` | `INTEGER` | Primary Key, Identity| Auto-incremented sequence. |
| `batch_id` | `BIGINTEGER` | Nullable | Consolidated running process batch ID. |
| `process_id` | `INTEGER` | Nullable | Parent pipeline process ID. |
| `dataset_id` | `INTEGER` | Nullable | Target dataset reference ID. |
| `source_file` | `VARCHAR(4000)`| Nullable | Name of file standardized. |
| `data_standardisation_location`| `TEXT` | Nullable| Destination path written to. |
| `status` | `VARCHAR(50)` | Nullable | Output execution status. |
| `exception_details` | `TEXT` | Nullable | Detailed traceback of failure error conditions. |
| `start_datetime` | `DATETIME` | Nullable | Step execution start timestamp. |
| `end_datetime` | `DATETIME` | Nullable | Step execution end timestamp. |

#### 12. `logDqmDtl`
Stores statistics on row-level DQM runs.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq_no` | `INTEGER` | Primary Key, Identity| Auto-incremented sequence. |
| `process_id` | `INTEGER` | Nullable | Parent pipeline process ID. |
| `dataset_id` | `INTEGER` | Nullable | Dataset validated. |
| `batch_id` | `BIGINTEGER` | Nullable | Consolidated running process batch ID. |
| `source_file` | `VARCHAR(4000)`| Nullable | Ingestion file that triggered rules checks. |
| `column_name` | `TEXT` | Nullable | Field validating against. |
| `qc_type` | `TEXT` | Nullable | Check type applied (e.g., `Null`, `Regex`). |
| `qc_param` | `TEXT` | Nullable | Rule configuration parameter checks. |
| `qc_filter` | `TEXT` | Nullable | Subset filter condition checks. |
| `criticality` | `TEXT` | Nullable | Execution criticality code (`C` / `W`). |
| `criticality_threshold_pct`| `INTEGER` | Nullable | Configuration allowed error percentage ceiling. |
| `error_count` | `INTEGER` | Nullable | Number of rows that failed validation checks. |
| `error_pct` | `INTEGER` | Nullable | Calculated percentage of rows that failed checks. |
| `status` | `VARCHAR(50)` | Nullable | Check status output metrics: `SUCCEEDED` / `FAILED`. |
| `dqm_start_time` | `DATETIME` | Nullable | DQM check step start timestamp. |
| `dqm_end_time` | `DATETIME` | Nullable | DQM check step end timestamp. |

#### 13. `logTransformationDtl`
Tracks the execution of Gold transformations and SCD Type 2 merges.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `seq_no` | `INTEGER` | Primary Key, Identity| Auto-incremented sequence. |
| `batch_id` | `BIGINTEGER` | Nullable | Target batch ID. |
| `data_date` | `DATE` | Nullable | Date of the partition processed. |
| `process_id` | `INTEGER` | Nullable | Transformation process ID. |
| `dataset_id` | `INTEGER` | Nullable | Destination transformed Gold table ID. |
| `source_file` | `VARCHAR(4000)`| Nullable | Reference staging file path analyzed. |
| `status` | `VARCHAR(50)` | Nullable | Execution status results. |
| `exception_details` | `TEXT` | Nullable | Detailed traceback of failure error conditions. |
| `transformation_start_time`| `DATETIME` | Nullable | Process start timestamp. |
| `transformation_end_time` | `DATETIME` | Nullable | Process end timestamp. |

---

## 4. Onboarding Step-by-Step SQL Guide

To onboard a new dataset in the framework, you insert metadata configurations into the control tables.

Below is an onboarding example for a **Sales Transactions** dataset.
It extracts data from an API source, validates transaction details, cleans the fields, and tracks changes historically inside a Gold table.

```mermaid
graph LR
    A[API Source: /sales] -->|1. Extract & Land| B[Bronze raw Delta table]
    B -->|2. Standardize & Validate| C[Silver Clean Staging table]
    C -->|3. Merge & Track History| D[Gold Table SCD Type 2]
```

### Step 4.1: Register Bronze, Silver, and Gold Datasets (`ctlDatasetMaster`)
Every dataset across all three Medallion layers must be registered in the central metastore database.

```sql
-- 1. Bronze layer raw Delta registration
INSERT INTO ctlDatasetMaster (
    process_id, dataset_id, dataset_name, dataset_type, 
    inbound_location, inbound_file_pattern, inbound_static_file_pattern, 
    inbound_file_format, inbound_file_delimiter, 
    landing_location, landing_table, table_location_type
) VALUES (
    1, 100, 'Sales_Transactions', 'BRONZE',
    './data/inbound/sales/', 'sales_report_.*\.csv', 'N',
    'csv', ',', 
    './data/bronze/sales_transactions/', 'bronze_sales_transactions', 'external'
);

-- 2. Silver layer standardized & validated staging registration
INSERT INTO ctlDatasetMaster (
    process_id, dataset_id, dataset_name, dataset_type, 
    landing_location, landing_table,
    data_standardisation_location, data_standardisation_partition_columns,
    dqm_error_location, dqm_partition_columns,
    staging_location, staging_table, staging_partition_columns,
    publish_location, publish_table, publish_partition_columns,
    table_location_type
) VALUES (
    1, 200, 'Sales_Transactions_Clean', 'SILVER',
    './data/bronze/sales_transactions/', 'bronze_sales_transactions',
    './data/silver/sales_standardised/', 'batch_id',
    './data/silver/sales_errors/', 'batch_id',
    './data/silver/sales_staging/', 'staging_sales_transactions', 'batch_id',
    './data/silver/sales_publish/', 'publish_sales_transactions', 'batch_id',
    'external'
);

-- 3. Gold layer consolidated dimension/fact registration
INSERT INTO ctlDatasetMaster (
    process_id, dataset_id, dataset_name, dataset_type,
    transformation_location, transformation_table, transformation_partition_columns,
    publish_location, publish_table, publish_partition_columns,
    table_location_type
) VALUES (
    1, 300, 'Sales_Fact', 'GOLD',
    './data/gold/sales_fact/', 'sales_fact', 'data_date',
    './data/gold/sales_fact_publish/', 'publish_sales_fact', 'data_date',
    'external'
);
```

### Step 4.2: Register the Ingestion Schedule & Endpoint (`ctlDataAcquisitionDetail`)
Defines the source format, endpoint, and outbound S3/API/SFTP properties.

```sql
INSERT INTO ctlDataAcquisitionDetail (
    process_id, pre_ingestion_dataset_id, pre_ingestion_dataset_name,
    outbound_source_platform, credentials_identifier, 
    outbound_source_file_pattern, outbound_source_file_format, outbound_file_delimiter, 
    inbound_location
) VALUES (
    1, 100, 'Sales_Transactions',
    'API', 'SALES_API_CREDENTIALS',
    'sales_report_sample', 'csv', ',',
    './data/inbound/sales/'
);
```

### Step 4.3: Register API Credentials & Call Properties (`ctlApiConnectionsDtl` & `ctlDataAcquisitionConnectionMaster`)
Registers security credentials and maps query endpoints for extraction.

```sql
-- Store connection secrets using prefixes for KMS decryption
INSERT INTO ctlDataAcquisitionConnectionMaster (
    outbound_source_platform, credentials_identifier, connection_config
) VALUES (
    'API', 'SALES_API_CREDENTIALS',
    '{
        "client_id": "keyvault:myvault/api-key:client_id",
        "client_secret": "keyvault:myvault/api-key:client_secret"
    }'
);

-- Set API Endpoint Details
INSERT INTO ctlApiConnectionsDtl (
    pre_ingestion_dataset_id, credentials_identifier, type,
    auth_type, method, url, ssl_verify
) VALUES (
    100, 'SALES_API_CREDENTIALS', 'RESPONSE',
    'oauth', 'GET', 'https://api.company.com/v1/sales/export', 'Y'
);
```

### Step 4.4: Map Column Schemas (`CtlColumnMetadata`)
Defines column data types for Schema Casting.

```sql
-- Map incoming fields to target data types
INSERT INTO CtlColumnMetadata (
    column_id, table_name, dataset_id, column_name, source_column_name, 
    column_data_type, column_sequence_number
) VALUES 
(1, 'bronze_sales_transactions', 100, 'transaction_id', 'TxID', 'int', 1),
(2, 'bronze_sales_transactions', 100, 'customer_id', 'CustomerID', 'int', 2),
(3, 'bronze_sales_transactions', 100, 'amount', 'Amount', 'decimal', 3),
(4, 'bronze_sales_transactions', 100, 'transaction_date', 'TxDate', 'date', 4),

-- Map target staging columns
(1, 'staging_sales_transactions', 200, 'transaction_id', 'transaction_id', 'int', 1),
(2, 'staging_sales_transactions', 200, 'customer_id', 'customer_id', 'int', 2),
(3, 'staging_sales_transactions', 200, 'amount', 'amount', 'decimal', 3),
(4, 'staging_sales_transactions', 200, 'transaction_date', 'transaction_date', 'date', 4),

-- Map Gold table columns (SCD Type 2 business columns)
(1, 'sales_fact', 300, 'transaction_id', 'transaction_id', 'int', 1),
(2, 'sales_fact', 300, 'customer_id', 'customer_id', 'int', 2),
(3, 'sales_fact', 300, 'amount', 'amount', 'decimal', 3),
(4, 'sales_fact', 300, 'transaction_date', 'transaction_date', 'date', 4);
```

### Step 4.5: Configure Standardization Rules (`ctlDataStandardisationDtl`)
Adds data cleaning functions to run during the Silver layer standardization phase.

```sql
-- Standardize Customer IDs to be left-padded with zero to 8 digits
INSERT INTO ctlDataStandardisationDtl (
    dataset_id, column_name, function_name, function_params
) VALUES (
    200, 'customer_id', 'padding', '{"type":"left", "length":8, "padding_value":"0"}'
);
```

### Step 4.6: Add Data Quality Management (DQM) Validation Rules (`ctlDqmMasterDtl`)
Sets up assertions to run against columns in the Silver layer.

```sql
-- Enforce that transaction_id cannot be null. Set as Critical (halts pipeline if violated)
INSERT INTO ctlDqmMasterDtl (
    qc_id, process_id, dataset_id, column_name, qc_type, active_flag, criticality, criticality_threshold_pct
) VALUES (
    1, 1, 200, 'transaction_id', 'Null', 'Y', 'C', 0
);

-- Enforce uniqueness of transaction_id. Set as Warning (logs error count but allows pipeline to continue)
INSERT INTO ctlDqmMasterDtl (
    qc_id, process_id, dataset_id, column_name, qc_type, active_flag, criticality, criticality_threshold_pct
) VALUES (
    2, 1, 200, 'transaction_id', 'Unique', 'Y', 'W', 5
);
```

### Step 4.7: Configure Gold Transformations & Joins (`ctlTransformationDependencyMaster`)
Specifies joins, aggregates, or SQL queries for merging staging tables into target Gold tables.

```sql
-- Map staging input (dataset 200) to Gold target table (dataset 300) with SCD 2 tracking
INSERT INTO ctlTransformationDependencyMaster (
    process_id, dataset_id, dependent_dataset_id,
    transformation_sequence, transformation_type, primary_keys
) VALUES (
    1, 300, 200,
    1, 'SINGLE', 'transaction_id'
);
```

---

## 5. KMS & Secret Resolution Configuration

Credentials can be stored inside metadata tables using provider prefixes. At runtime, the framework decrypts the values dynamically.

```
+---------------------------------------------+     +-------------------------------+
| Prefix String Example                       | --> | Decrypted Credentials Value   |
| 'keyvault:myvault/secret-name:json_key'     |     | 'MyDecryptedSuperPassword123' |
+---------------------------------------------+     +-------------------------------+
```

The framework determines which KMS to use either via the prefix in the connection string or through the global `SECRETS_PROVIDER` environment variable (options: `aws`, `azure`, `vault`, `encryption`).

### A. AWS Secrets Manager Integration
* **Decryption Syntax:** `secretsmanager:<secret_id>[:json_field]`
* **Required Environment Credentials on Run Host:**
  ```env
  AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
  AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  AWS_DEFAULT_REGION=us-east-1
  ```

### B. Azure Key Vault Integration
* **Decryption Syntax:** `keyvault:<vault_name>/<secret_name>[:json_field]`
* **Managed Identity Auth:** Auto-detected if executed on an Azure VM or AKS node.
* **Service Principal Fallback:**
  ```env
  AZURE_CLIENT_ID=00000000-0000-0000-0000-000000000000
  AZURE_CLIENT_SECRET=myAzureClientSecretKeyStr~
  AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000
  ```

### C. HashiCorp Vault Integration
* **Decryption Syntax:** `vault:<mount_path>/<secret_path>[:json_field]`
* **Environment variables:**
  ```env
  VAULT_ADDR=https://vault.company.com:8200
  VAULT_TOKEN=hvs.TokenExampleValueTextHere
  ```

### D. Local Symmetric Encryption (Fernet AES-128)
* **Decryption Syntax:** `encrypted:<ciphertext>`
* **Environment variables:**
  ```env
  DATADALLION_ENCRYPTION_KEY=7Nf_7K3K1X5aB6_V1v1f1R_K1v1f1R_K1v1f1R_K1v8=
  ```
* **Encrypting Plaintext Credentials:**
  Execute the helper script to encrypt credentials before inserting them into metadata configuration tables:
  ```python
  from data_dallion_framework.Common.SecretManager import encrypt_value
  
  # Ensure DATADALLION_ENCRYPTION_KEY is set in your terminal environment first!
  ciphertext = encrypt_value("MySecretStringValue")
  print(ciphertext)
  # Output: encrypted:gAAAAABl...
  ```

---

## 6. PySpark Core Execution Orchestration

The pipeline is triggered using a Python orchestrator file.

### Execution Orchestrator Snippet
Create an entry script (e.g. `main.py`) to run all three layers sequentially:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from data_dallion_framework.BronzeScripts import PerformBronze
from data_dallion_framework.SilverScripts import SilverLayerProcess
from data_dallion_framework.GoldScripts import PerformGoldLayerProcess

# Initialize Spark Session with native Delta Lake support
builder = (
    SparkSession.builder
    .appName("DataDallionOrchestration")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Orchestrator parameters
process_id = 1
env_scope = "dev"  # prefix for databases and storage paths

# 1. RUN BRONZE: Ingests raw source data and writes to raw Delta Lake
bronze = PerformBronze.PerformBronze(spark, process_id, env=env_scope)
bronze.start_extraction()

# 2. RUN SILVER: Applies standardization and runs DQM checks
SilverLayerProcess.SilverLayerProcess(spark, process_id, env=env_scope)

# 3. RUN GOLD: Joins staging tables and updates target tables via SCD Type 2 merge
PerformGoldLayerProcess.GoldLayerProcess(spark, process_id, env=env_scope)
```

### Incremental File Loading Mechanism
The framework performs incremental file ingestion using control logs:
1. It lists files in `inbound_location` (`files_in_inbound`).
2. It fetches all successfully processed files from `logRawProcessDtl` (`raw_completed_files`).
3. It finds new files to process using:
   ```text
   new_files = files_in_inbound - raw_completed_files
   ```
4. It filters the remaining files by matching their names against the regex pattern specified in `inbound_file_pattern`.

### Gold Layer Slowly Changing Dimension (SCD) Type 2 Logic
For Gold layer transformations, the framework tracks historical changes using SCD Type 2 merges:
1. Calculates a SHA-256 hash of all mapped target columns and saves it in `sys_checksum`.
2. Compares incoming records with active rows in the destination table (where `eff_end_dt = '9999-12-31'`) using the configured primary keys.
3. If an incoming record matches an active row but has a different `sys_checksum` (signaling data changes):
   * Updates the active record: Sets `eff_end_dt = staging.eff_strt_dt` and `sys_del_flg = 'Y'` (deactivates the historical record).
   * Inserts the new record: Appends a new active row with `eff_strt_dt = current_date()` and `eff_end_dt = '9999-12-31'`.
4. If an incoming record is new (its primary key does not exist under an active high date), it inserts it directly as active.

---

## 7. Data Quality Management (DQM) Validation Framework

DQM rules are evaluated in parallel using PySpark. Checks are categorized by criticality:
* **Critical (`C`):** Violations halt the pipeline execution immediately, raising an exception to prevent downstream pollution.
* **Warning (`W`):** Violations do not stop the pipeline. Failed rows are logged and saved to the `dqm_error_location` folder, while valid rows continue processing.

### Supported DQM Rule Types

| Check Type | Parameter (`qc_param`) | Under-the-hood Spark SQL Condition | Description |
| :--- | :--- | :--- | :--- |
| **`Null`** | — | `column_name IS NOT NULL` | Fails if the column is Null. |
| **`Blank`** | — | `trim(column_name) != ''` | Fails if the column value is blank or empty. |
| **`Length`** | Comparison op & length (e.g. `>=10`, `=5`) | `length(column_name) >= 10` | Validates column string length. |
| **`Length-Range`** | Interval tuple list (e.g. `[2, 12]`) | `length(column_name) BETWEEN 2 AND 12` | Fails if string length falls outside the range. |
| **`Integer`** | — | `column_name RLIKE '^-?[0-9]+\$'` | Validates if string contains only integer values. |
| **`Decimal`** | — | `column_name RLIKE '^-?([0-9]+\\.[0-9]+...`| Validates string representation of float/decimals. |
| **`Date`** | Code lookup key (e.g. `YYYYMMDD`) | `column_name RLIKE '^\d{4}\d{2}\d{2}\$'` | Regex check to confirm matching date formats. |
| **`Regex`** | Custom Regex (e.g. `^[A-Z]{3}\$`) | `column_name RLIKE '^[A-Z]{3}\$'` | Evaluates field against custom regex syntax. |
| **`Domain`** | Comma-separated list (e.g. `US,CA,MX`) | `column_name IN ('US', 'CA', 'MX')` | Validates if value exists inside domain dataset. |
| **`Custom`** | Spark SQL string (e.g. `price > tax`) | `price > tax` | Evaluates arbitrary Spark expressions. |
| **`Unique`** | PK column list (e.g. `id` or `id,date`) | Spark Window: `row_number() == 1` | Flags duplicate keys using window function. |

---

## 8. Operational Troubleshooting & Logging

### Querying Logs for Audits & Error Tracing
You can monitor pipeline runs by querying the metadata database:

```sql
-- Query failed acquisition downloads
SELECT batch_id, pre_ingestion_dataset_id, outbound_source_location, exception_details, start_time
FROM logDataAcquisitionDetail
WHERE status = 'FAILED'
ORDER BY start_time DESC;

-- Query failed file landing ingestion runs
SELECT batch_id, dataset_id, source_file, exception_details, file_process_end_time
FROM logRawProcessDtl
WHERE file_status = 'FAILED'
ORDER BY file_process_end_time DESC;

-- Query data quality error metrics per batch run
SELECT batch_id, dataset_id, column_name, qc_type, error_count, error_pct, status, dqm_end_time
FROM logDqmDtl
WHERE status = 'FAILED' OR error_count > 0
ORDER BY dqm_end_time DESC;
```

### Common Failure Points & Recovery Actions

#### 1. Ingestion File Schema Mismatch
* **Symptom:** Ingestion halts with the error `Column Metadata mismatch in batch`.
* **Cause:** The structure of the source file has changed (new columns, missing columns, or different column names).
* **Recovery:**
  1. Inspect the source file columns and compare them with metadata in `CtlColumnMetadata`.
  2. Update the columns in `CtlColumnMetadata` to match the new source structure, ensuring correct types and ordinal numbers.
  3. Re-run the orchestrator runner.

#### 2. Critical DQM Exception Failure
* **Symptom:** Validation halts with the error `Critical DQM check failed: <column> in batch <id>`.
* **Cause:** The input dataset contains bad records that exceed the allowable error threshold (e.g., null values in a primary key column).
* **Recovery:**
  1. Query `logDqmDtl` to identify the rule violation and the number of failed records.
  2. Read the quarantined invalid records in the `dqm_error_location` folder to pinpoint the bad values.
  3. Work with the source system provider to correct the data quality issue, or adjust the `criticality_threshold_pct` / `criticality` parameters in `ctlDqmMasterDtl` if the error rate is acceptable.
  4. Reprocess the batch files.

#### 3. KMS Decryption / Credentials Exception
* **Symptom:** Ingestion fails with the error `DATADALLION_ENCRYPTION_KEY environment variable is not set...` or `Azure credentials not configured...`.
* **Cause:** Required environment credentials for AWS, Azure, HashiCorp Vault, or local Fernet symmetric keys are missing on the runner instance.
* **Recovery:**
  1. Confirm the environment configuration on the runner host matches Section 5.
  2. Export missing credentials in the terminal shell or add them to the local `.env` configuration file.
  3. Verify the decryption target connection prefix maps to the correct provider syntax.
