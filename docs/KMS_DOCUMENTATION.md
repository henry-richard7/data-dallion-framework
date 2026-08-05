# Key Management System (KMS) & Secrets Integration Guide

This guide explains how to secure connection strings, passwords, and private API keys using the unified Key Management System (KMS) integration in the **DataDallion Framework**.

---

## 1. Prefix-Based Secrets Resolution

The framework allows you to store specialized prefix strings directly in database credential columns (such as `sa_column` password fields or JSON fields inside `connection_config`). During pipeline execution, the framework dynamically fetches and decrypts the secret value.

### Supported Schemes

| Provider | Syntax | Example |
| :--- | :--- | :--- |
| **AWS Secrets Manager** | `secretsmanager:<secret_id>[:json_field]` | `secretsmanager:prod/db/credentials:password` |
| **Azure Key Vault** | `keyvault:<vault_name>/<secret_name>[:json_field]` | `keyvault:dallion-kv/salesforce-secret` |
| **HashiCorp Vault** | `vault:<mount_path>/<secret_path>[:json_field]` | `vault:secret/databases/postgres:username` |
| **Local Symmetric Encryption** | `encrypted:<ciphertext>` | `encrypted:gAAAAABmg...` |

---

## 2. Global Provider Configuration (Fallback)

If you prefer to configure a global secret provider rather than prefixing every database entry, set the `SECRETS_PROVIDER` environment variable.

| Provider | `SECRETS_PROVIDER` value | Description |
| :--- | :--- | :--- |
| **AWS Secrets Manager** | `aws` or `secretsmanager` | Resolves entries using AWS Secrets Manager |
| **Azure Key Vault** | `azure` or `keyvault` | Resolves entries using Azure Key Vault |
| **HashiCorp Vault** | `vault` | Resolves entries using HashiCorp Vault |
| **Local Encryption** | `encryption` | Resolves entries using symmetric decryption |

---

## 3. Local Symmetric Encryption Setup

If no cloud Key Management System is configured, the framework falls back to local symmetric encryption using **Fernet AES-128**.

### Step 1: Generate & Set Encryption Key
Generate a 32-byte URL-safe base64-encoded key and export it to your environment variables:

```bash
# Set key in terminal or env configuration
set DATADALLION_ENCRYPTION_KEY=7Nf_7K3K1X5aB6_V1v1f1R_K1v1f1R_K1v1f1R_K1v8=
```

### Step 2: Encrypting a Plaintext Value
Use the helper function to encrypt your credentials prior to inserting them into the database connection fields:

```python
from data_dallion_framework.Common.SecretManager import encrypt_value

encrypted_str = encrypt_value("MySuperSecurePassword123")
print(encrypted_str)
# Output: encrypted:gAAAAABl... (Save this exact value in database)
```

---

## 4. Configuration Table Setup Examples

You can place secret resolution prefix strings directly inside the configuration tables. Here are SQL configuration examples for the Metadata Tables:

### A. API Connections (`ctlApiConnectionsDtl` table)
For an OAuth-authenticated API extractor step, insert KMS/encryption reference strings directly into the credentials fields:

```sql
INSERT INTO ctlApiConnectionsDtl (
    seq_no, 
    pre_ingestion_dataset_id, 
    type, 
    auth_type, 
    client_id, 
    client_secret, 
    private_key
) VALUES (
    1, 
    101, 
    'TOKEN', 
    'oauth', 
    'my-client-app-id', 
    -- AWS Secrets Manager reference:
    'secretsmanager:prod/api/app:client_secret',
    -- Local symmetric encrypted private key:
    'encrypted:gAAAAABmgPrivateKeyValue...'
);
```

### B. Standard Platforms & Databases (`ctlDataAcquisitionConnectionMaster` table)
For database connections, configure the JSON `connection_config` column to use KMS prefixes. You can also specify the `ssh_private_key` directly as a KMS secret reference:

```sql
INSERT INTO ctlDataAcquisitionConnectionMaster (
    outbound_source_platform, 
    credentials_identifier, 
    connection_config, 
    ssh_private_key
) VALUES (
    'SFTP', 
    'SFTP_USER_CONN', 
    -- connection_config JSON:
    '{
        "host": "sftp.company.com",
        "user": "dallion_user",
        "password": "keyvault:myvault/sftp-password"
    }',
    -- HashiCorp Vault private key reference:
    'vault:secret/sftp/keys:private_key'
);
```

---

## 5. Cloud Providers Configuration & Prerequisites

### A. AWS Secrets Manager
* **Authentication:** Relies on standard AWS credential resolution (e.g. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, IAM Roles, or `~/.aws/credentials`).
* **Environment variables:**
  ```bash
  AWS_REGION=us-east-1
  ```

### B. Azure Key Vault
* **Authentication:** Supports either Service Principal credentials or Managed Identity (MSI) dynamically.
* **Environment variables (Service Principal):**
  ```bash
  AZURE_CLIENT_ID=<client_id>
  AZURE_CLIENT_SECRET=<client_secret>
  AZURE_TENANT_ID=<tenant_id>
  ```
* **Managed Identity (MSI):** Auto-detected if deployed on Azure VMs, Container Apps, or Kubernetes nodes.

### C. HashiCorp Vault
* **Authentication:** Token-based authentication.
* **Environment variables:**
  ```bash
  VAULT_ADDR=https://vault.company.com:8200
  VAULT_TOKEN=hvs.xxxxxxxxxxxxxxxxxxxxxx
  ```
