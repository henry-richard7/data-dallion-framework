# Standard Operating Procedure (SOP): DataDallion Framework

This document outlines the standard procedures for configuring, executing, and maintaining data pipelines using the **DataDallion Framework**.

---

## 1. Overview of the Architecture

DataDallion is a metadata-driven orchestration framework matching the **Medallion Architecture**. All dataset structures, ingestion endpoints, data quality constraints, and joins are configured in a relational metadata store.

```
+------------------+     +-------------------+     +------------------+     +-------------------+
| External Sources | --> | Bronze Layer      | --> | Silver Layer     | --> | Gold Layer        |
| (API, SFTP, S3,  |     | (Raw ingestion to |     | (Standardization |     | (Curated business |
|  DB, Salesforce) |     |  Delta Lake)      |     |  & DQM checks)   |     |  aggregates/SCD2) |
+------------------+     +-------------------+     +------------------+     +-------------------+
```

---

## 2. Environment Configuration

The framework loads settings from environment variables or a local `.env` file.

### Backend Metadata Store
| Parameter | Purpose | Default |
| :--- | :--- | :--- |
| `DB_TYPE` | Type of relational metadata store: `sqlite`, `mysql`, `postgresql`, `mariadb` | `sqlite` |
| `DATABASE_URL` | Full SQLAlchemy connection string (overrides separate DB params) | — |
| `DB_HOST` / `DB_NAME` | Connection details when `DATABASE_URL` is not used | — |
| `DB_USER` / `DB_PASSWORD` | Authentication for the metadata store database | — |
| `DATACRAFT_FRAMEWORK_HOME` | Storage path for SQLite metadata file | `~/datacraft_framework` |

---

## 3. Registering a New Dataset (Metadata Setup)

To onboard a new pipeline, you must populate the metadata tables:

### Step 3.1: Register the Dataset Master (`ctlDatasetMaster` table)
Maps the dataset identity, paths, and formats:
```sql
INSERT INTO ctlDatasetMaster (
    pre_ingestion_dataset_id, 
    dataset_name, 
    bronze_table_name, 
    silver_table_name, 
    gold_table_name, 
    dataset_type
) VALUES (
    101, 
    'Sales Transactions', 
    'bronze_sales_transactions', 
    'silver_sales_transactions', 
    'gold_sales_transactions', 
    'SILVER'
);
```

### Step 3.2: Map Source Columns (`ctlColumnMetadata` table)
Defines how fields are mapped from source to targets, casting types, and specifying JSON path configurations for nested REST API bodies:
```sql
INSERT INTO ctlColumnMetadata (
    seq_no, 
    pre_ingestion_dataset_id, 
    source_column_name, 
    target_column_name, 
    target_data_type, 
    date_format, 
    json_path
) VALUES (
    1, 
    101, 
    'transaction_id', 
    'transaction_id', 
    'INT', 
    NULL, 
    '$.tx_id'
);
```

### Step 3.3: Set Up Ingestion Details (`ctlDataAcquisitionDetail` table)
Specifies the inbound platform, landing location, file pattern matching, query extraction filters, and column projections:
```sql
INSERT INTO ctlDataAcquisitionDetail (
    process_id, 
    pre_ingestion_dataset_id, 
    pre_ingestion_dataset_name, 
    outbound_source_platform, 
    credentials_identifier, 
    outbound_source_location, 
    outbound_source_file_pattern, 
    outbound_source_file_format, 
    outbound_file_delimiter, 
    inbound_location
) VALUES (
    1, 
    101, 
    'SalesData', 
    'S3', 
    'COMPANY_AWS_CONN', 
    's3://sales-raw-bucket/inbound/', 
    'sales_report_*.csv', 
    'csv', 
    ',', 
    './data/inbound/'
);
```

---

## 4. Ingestion Settings & Connector Configurations

Ingestion credentials can be stored as plaintext, local symmetric ciphertext, or remote Key Management system values:

### A. Secret Prefix Schemas
* **AWS Secrets Manager:** `secretsmanager:<secret_id>[:json_field]`
* **Azure Key Vault:** `keyvault:<vault_name>/<secret_name>[:json_field]`
* **HashiCorp Vault:** `vault:<mount_path>/<secret_path>[:json_field]`
* **Local Symmetric Encryption:** `encrypted:<ciphertext>`

### B. Registering Platform Credentials (`ctlDataAcquisitionConnectionMaster`)
```sql
INSERT INTO ctlDataAcquisitionConnectionMaster (
    outbound_source_platform, 
    credentials_identifier, 
    connection_config
) VALUES (
    'S3', 
    'COMPANY_AWS_CONN', 
    '{
        "client_id": "keyvault:myvault/aws-credentials:access_key",
        "client_secret": "keyvault:myvault/aws-credentials:secret_key",
        "region": "us-east-1"
    }'
);
```

---

## 5. Key Management System (KMS) Credentials Configuration

To enable the runtime decryption of credentials using cloud KMS systems or local encryption, configure the following credentials/environment variables on the runner instance:

### A. AWS Secrets Manager
* **AWS Authentication:** Relies on standard AWS SDK credentials configuration. You can either authenticate via local files or direct environment variables:
  ```env
  AWS_ACCESS_KEY_ID=<your_aws_access_key>
  AWS_SECRET_ACCESS_KEY=<your_aws_secret_key>
  AWS_DEFAULT_REGION=us-east-1
  ```

### B. Azure Key Vault
* **Managed Identity (Recommended):** If running within an Azure VM, AKS pod, or Azure App Service, the framework automatically uses Managed Identity without credential environment variables.
* **Service Principal Fallback:** If running outside Azure, configure the following Service Principal variables:
  ```env
  AZURE_CLIENT_ID=<your_service_principal_client_id>
  AZURE_CLIENT_SECRET=<your_service_principal_client_secret>
  AZURE_TENANT_ID=<your_service_principal_tenant_id>
  ```

### C. HashiCorp Vault
* **Vault Address & Token Authentication:** Configure the endpoint URL and client token headers:
  ```env
  VAULT_ADDR=https://vault.company.com:8200
  VAULT_TOKEN=hvs.your_vault_authentication_token
  ```

### D. Local Symmetric Encryption Fallback
* **Encryption Key Configuration:** Export the base64-encoded 32-byte symmetric key:
  ```env
  DATADALLION_ENCRYPTION_KEY=<your_32_byte_base64_encoded_symmetric_key>
  ```
* **Generating Encrypted Credentials:** Use the helper functions to generate the local symmetric ciphertext (which you can then store in configuration tables):
  ```python
  from data_dallion_framework.Common.SecretManager import encrypt_value
  
  # Ensure DATADALLION_ENCRYPTION_KEY is set in your env first!
  encrypted_val = encrypt_value("MyPlainTextSecret")
  print(encrypted_val)
  # Output: encrypted:gAAAAABl...
  ```

---

## 6. Executing the Medallion Processing Layers

Create a Python orchestration entry point to invoke all three Medallion processing layers sequentially:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from data_dallion_framework.BronzeScripts import PerformBronze
from data_dallion_framework.SilverScripts import SilverLayerProcess
from data_dallion_framework.GoldScripts import PerformGoldLayerProcess

# Initialize Spark Session with Delta support
builder = (
    SparkSession.builder
    .appName("DataDallionEngine")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Orchestrator parameters
process_id = 1

# 1. BRONZE LAYER (Extraction & Raw Landing)
bronze = PerformBronze.PerformBronze(spark, process_id)
bronze.start_extraction()

# 2. SILVER LAYER (Standardization & Quality Validation)
SilverLayerProcess.SilverLayerProcess(spark, process_id)

# 3. GOLD LAYER (Business Joins, Aggregations, & SCD Type 2 History)
PerformGoldLayerProcess.GoldLayerProcess(spark, process_id)
```

---

## 7. Configuring Data Quality Checks (DQM)

Configure checks in `ctlDqmMasterDtl` to filter records and prevent dirty data from reaching downstream tables.

### Rule Properties
* **`qc_type`**: The type of check to execute.
* **`criticality`**: Set to `C` (Critical - halts pipeline if failure threshold is exceeded) or `W` (Warning - logs metrics but permits row processing).
* **`criticality_threshold_pct`**: The allowable percentage of failing rows (e.g. `5.0` allows up to 5% failure).

### Supported Checks
1. **Null Check (`Null`)**: Fails if the targeted column contains `NULL`.
2. **Blank Check (`Blank`)**: Fails if column contains empty/whitespace-only values.
3. **Regex Check (`Regex`)**: Validates strings against patterns (e.g., email `^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$`).
4. **Length Range Check (`Length-Range`)**: Fails if string length lies outside a range parameter (e.g., `[5, 20]`).
5. **Unique Check (`Unique`)**: Validates column uniqueness across one or more comma-separated columns (e.g. `order_id,product_id`).
6. **Custom SQL Check (`Custom`)**: Runs arbitrary spark SQL expressions (e.g. `amount > 0 AND tax_rate >= 0`).

---

## 8. Operational Troubleshooting & Logging

* **Execution Audits:** Access `logDataAcquisitionDetail` and `logDqmSummaryDetail` database logs to view execution metrics, counts, and status runs.
* **Logger Streams:** Runtime logs are formatted and output to `stdout` with precise timestamps and levels:
  `2026-08-05 22:05:00 [INFO] data_dallion_framework.Extractors.S3Extractor - file_save_name is already processed. Skipping.`
* **Incremental Runs:** S3 and SFTP connectors keep track of completed files in metadata tables and will skip previously processed files in subsequent runs.
