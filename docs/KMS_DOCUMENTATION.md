# Key Management System (KMS) & Secrets Integration Guide

This guide explains how to secure connection strings, passwords, and private API keys using the unified Key Management System (KMS) integration in the **DataDallion Framework**. 

It is divided into two parts: a **Non-Technical Business Overview** explaining the compliance rationale and operational roles, and a **Developer Technical Reference** describing the underlying Python implementation, credential resolution patterns, and environment configurations.

---

## 1. Non-Technical & Business Overview

### The Business Problem
In standard software pipelines, developers often save passwords, API keys, and connection strings directly in configuration tables or code repositories. This practice poses severe business risks:
* **Security Breaches:** If code is accidentally leaked or if database tables are compromised, attackers gain immediate access to sensitive production systems.
* **Compliance Violations:** Modern data privacy laws and standards (such as **GDPR**, **HIPAA**, and **SOC 2**) strictly forbid storing credentials in plaintext format.
* **Operational Overhead:** When passwords expire or change, developers must manually update code across multiple systems, risking downtime.

### The Solution: Key Management Systems (KMS)
A Key Management System (KMS) behaves like a **highly secure corporate password manager** (similar to 1Password or LastPass) but is built specifically for computers and software engines. Instead of writing actual passwords into the data pipeline configuration, we write a **secure reference label** (or prefix).

During execution, the DataDallion engine reads this label, securely contacts the corporate KMS, decodes the password on-the-fly in computer memory, connects to the source database, and immediately discards the password once the connection is established. **The password is never written to disk, saved in database tables, or exposed in logs.**

```
+---------------------------------------------+     +-------------------------------+
| Secure Reference Label in Metastore         | --> | Decrypted Secret (in Memory)  |
| 'keyvault:myvault/secret-name:json_key'     |     | 'MyDecryptedSuperPassword123' |
+---------------------------------------------+     +-------------------------------+
```

### Operational Roles & Workflow
Securing credentials is a collaborative effort involving three main roles:

1. **Security Administrators:** Manage the corporate vaults (AWS, Azure, or HashiCorp). They create the actual credentials inside the vaults and grant read permissions to the DataDallion pipeline runner.
2. **Business & Data Analysts:** Configure dataset parameters in the DataDallion control tables. Instead of pasting passwords, they obtain the secure reference label from the security team and paste it into the configuration database.
3. **Data Engineers & Developers:** Maintain the pipeline engine, ensuring the code resolves the reference labels correctly and handles errors gracefully.

### Business Process Flow

```mermaid
graph TD
    A[Security Admin] -->|1. Creates secret in Secure Vault| B(AWS Azure or HashiCorp Vault)
    A -->|2. Provides label string to Analyst| C[Data Analyst]
    C -->|3. Pastes label string into Metastore DB| D[(DataDallion Metastore)]
    
    subgraph Automatic Execution at Runtime
        E[DataDallion Engine] -->|4. Reads label string| D
        E -->|5. Contacts vault with label| B
        B -->|6. Validates runner permission & decrypts| E
        E -->|7. Connects to database/API using decrypted key| F[Source System]
    end
    
    style B fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#bbf,stroke:#333,stroke-width:2px
```

---

## 2. Developer Reference & Resolution Logic

For developers and engineers, credential resolution is implemented in [SecretManager.py](file:///g:/Coding/Python/data-dallion-framework/src/data_dallion_framework/Common/SecretManager.py).

### Core Resolution Algorithm
When the orchestrator connects to a database, API, or SFTP server, it passes connection strings through the `get_secret` or `resolve_credentials` utilities. The workflow resolves credentials through the following hierarchy:

1. **Prefix Detection:** Checks if the configuration string begins with a known provider scheme (`secretsmanager:`, `keyvault:`, `vault:`, `encrypted:`).
   * If detected, it splits the prefix, calls the corresponding provider API, parses the secret payload, and returns the plain value.
2. **Global Fallback:** If no prefix is detected, but the global environment variable `SECRETS_PROVIDER` is set, the engine assumes the entire string is a path inside the global provider.
   * *Example:* If `SECRETS_PROVIDER=azure` and the configuration value is `prod-sftp-password`, it calls Azure Key Vault to resolve `prod-sftp-password`.
3. **Plaintext Fallback:** If no prefix is found and no global provider is configured, the engine returns the configuration string as-is (useful for local development or testing with mock databases).

### Technical Flow Chart

```mermaid
flowchart TD
    A[Input Configuration String] --> B{Does it start with a prefix?}
    
    B -->|Yes| C{Which Prefix?}
    C -->|secretsmanager:| D[Call AWS Secrets Manager SDK]
    C -->|keyvault:| E[Call Azure Key Vault REST API]
    C -->|vault:| F[Call HashiCorp Vault REST API]
    C -->|encrypted:| G[Decrypt locally via Fernet AES-128]
    
    B -->|No| H{Is SECRETS_PROVIDER env var set?}
    H -->|Yes| I[Treat value as path for global provider]
    H -->|No| J[Return value as Plaintext]
    
    D --> K[Extract optional JSON key & Return plaintext value]
    E --> K
    F --> K
    G --> K
    I --> K
```

---

## 3. Supported Prefix Schemes & Syntax

You can store prefix strings directly in database credential columns (such as password fields or JSON fields inside `connection_config`). 

| Provider | Syntax | Example | Description |
| :--- | :--- | :--- | :--- |
| **AWS Secrets Manager** | `secretsmanager:<secret_id>[:json_field]` | `secretsmanager:prod/db/credentials:password` | Connects using standard AWS SDK credentials and parses key values. |
| **Azure Key Vault** | `keyvault:<vault_name>/<secret_name>[:json_field]` | `keyvault:dallion-kv/salesforce-secret:client_secret` | Calls the Azure Key Vault REST API to fetch secrets. |
| **HashiCorp Vault** | `vault:<mount_path>/<secret_path>[:json_field]` | `vault:secret/databases/postgres:username` | Requests secret values from a HashiCorp Vault instance. |
| **Local Symmetric Encryption** | `encrypted:<ciphertext>` | `encrypted:gAAAAABmg...` | Decrypts values locally using a base64 key. |

---

## 4. Cloud Provider Connectivity & Authentication Fallbacks

Each cloud provider has standard authentication methods that must be configured on the server instance running the DataDallion pipeline.

### A. AWS Secrets Manager
* **Authentication:** Relies on the standard AWS SDK credential chain (`boto3` client).
* **Credentials Resolution:** It checks environment variables first, then local configuration files, and finally IAM Roles associated with the running EC2 instance or ECS/EKS task.
* **Environment variables:**
  ```env
  AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
  AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  AWS_DEFAULT_REGION=us-east-1
  ```

### B. Azure Key Vault
* **Authentication:** Handled dynamically via token requests in [SecretManager.py](file:///g:/Coding/Python/data-dallion-framework/src/data_dallion_framework/Common/SecretManager.py#L53-L81).
* **Credentials Resolution Hierarchy:**
  1. **Service Principal (API Authentication):** Checks for environment variables representing an Azure Service Principal:
     ```env
     AZURE_CLIENT_ID=00000000-0000-0000-0000-000000000000
     AZURE_CLIENT_SECRET=myAzureClientSecretKeyStr~
     AZURE_TENANT_ID=00000000-0000-0000-0000-000000000000
     ```
  2. **Managed Identity (MSI Fallback):** If environment variables are missing, it sends an HTTP GET request to the Azure Instance Metadata Service (IMDS) link-local IP (`http://169.254.169.254/metadata/identity/oauth2/token`). This allows seamless authentication on Azure VMs, Container Apps, or AKS pods without managing secret files.

### C. HashiCorp Vault
* **Authentication:** Supports Token authentication or AppRole authentication.
* **Token Authentication:**
  ```env
  VAULT_ADDR=https://vault.company.com:8200
  VAULT_TOKEN=hvs.TokenExampleValueTextHere
  ```
* **AppRole Authentication:**
  AppRole is the recommended method for automated scripts. It exchanges a Role ID and Secret ID for a short-lived token:
  ```env
  VAULT_ADDR=https://vault.company.com:8200
  VAULT_ROLE_ID=00000000-0000-0000-0000-000000000000
  VAULT_SECRET_ID=00000000-0000-0000-0000-000000000000
  VAULT_APPROLE_PATH=approle  # Optional, defaults to "approle"
  ```

### D. Local Symmetric Encryption Setup
If no cloud KMS is available, the framework encrypts and decrypts values locally using **Fernet AES-128**.

* **Step 1: Set Encryption Key**
  Generate a 32-byte URL-safe base64-encoded key and export it to the environment variables on the runner instance:
  ```bash
  # Linux/macOS
  export DATADALLION_ENCRYPTION_KEY=7Nf_7K3K1X5aB6_V1v1f1R_K1v1f1R_K1v1f1R_K1v8=

  # Windows PowerShell
  $env:DATADALLION_ENCRYPTION_KEY="7Nf_7K3K1X5aB6_V1v1f1R_K1v1f1R_K1v1f1R_K1v8="
  ```
* **Step 2: Generate Ciphertext**
  Use the [SecretManager.py](file:///g:/Coding/Python/data-dallion-framework/src/data_dallion_framework/Common/SecretManager.py#L21-L30) helper function to encrypt a password. It returns the ciphertext prefixed with `encrypted:`:
  ```python
  from data_dallion_framework.Common.SecretManager import encrypt_value
  
  # Note: Ensure DATADALLION_ENCRYPTION_KEY is set in your environment!
  cipher_text = encrypt_value("MySuperSecretPassword")
  print(cipher_text)
  # Output: encrypted:gAAAAABl...
  ```
* **Step 3: Save to Metadata DB**
  Copy the output string (`encrypted:gAAAAABl...`) and paste it directly into your database connection tables.

---

## 5. Configuration Table Integration Examples

You can place secret resolution prefixes inside any configuration column. The framework decrypts them automatically at runtime.

### A. API Connections Configuration (`ctlApiConnectionsDtl` table)
For OAuth-authenticated API extractions, configure the client secrets or private key columns using KMS prefixes:

```sql
INSERT INTO ctlApiConnectionsDtl (
    seq_no, pre_ingestion_dataset_id, type, auth_type, client_id, client_secret, private_key
) VALUES (
    1, 101, 'TOKEN', 'oauth', 'my-client-app-id',
    -- AWS Secrets Manager reference:
    'secretsmanager:prod/api/app:client_secret',
    -- Local symmetric encrypted private key:
    'encrypted:gAAAAABmgPrivateKeyValue...'
);
```

### B. Standard Platforms & Databases (`ctlDataAcquisitionConnectionMaster` table)
For database connections, configure the JSON `connection_config` column to use Key Vault prefixes:

```sql
INSERT INTO ctlDataAcquisitionConnectionMaster (
    outbound_source_platform, credentials_identifier, connection_config, ssh_private_key
) VALUES (
    'SFTP', 'SFTP_USER_CONN',
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

## 6. Developer Troubleshooting & Testing

If a pipeline is failing to connect to a source system, you can debug the credential resolution using the Python interactive shell.

### Testing Credential Resolution in Python
Open a Python terminal on the runner machine and execute the following commands to verify connectivity:

```python
import os
from data_dallion_framework.Common import SecretManager

# 1. Test local symmetric decryption
os.environ["DATADALLION_ENCRYPTION_KEY"] = "7Nf_7K3K1X5aB6_V1v1f1R_K1v1f1R_K1v1f1R_K1v8="
test_secret = SecretManager.encrypt_value("HelloWorld")
print("Encrypted:", test_secret)
print("Decrypted:", SecretManager.decrypt_value(test_secret))

# 2. Test AWS Secrets Manager resolution
# Make sure AWS credentials are set in environment!
try:
    aws_val = SecretManager.get_aws_secret("prod/db/credentials:password")
    print("AWS Secret retrieved successfully!")
except Exception as e:
    print("AWS Secrets Manager Error:", str(e))

# 3. Test Azure Key Vault resolution
# Make sure Azure Service Principal credentials are set in environment!
try:
    azure_val = SecretManager.get_azure_secret("myvault/sftp-password")
    print("Azure Secret retrieved successfully!")
except Exception as e:
    print("Azure Key Vault Error:", str(e))
```

### FAQ & Common Errors

#### 1. Error: `DATADALLION_ENCRYPTION_KEY environment variable is not set`
* **Cause:** An `encrypted:...` prefix was used, but the symmetric key is missing from the environment variables.
* **Solution:** Export `DATADALLION_ENCRYPTION_KEY` on the machine running the pipeline before starting the run.

#### 2. Error: `Azure credentials not configured`
* **Cause:** The framework was unable to authenticate with Azure. It checked for Service Principal environment variables and tried to contact the IMDS Managed Identity endpoint, but both failed.
* **Solution:**
  * If running locally, export `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, and `AZURE_TENANT_ID`.
  * If running in Azure, verify that the VM or AKS pod has a **Managed Identity** assigned and has permission (Access Policies or Azure RBAC) to read secrets from the Key Vault.

#### 3. Error: `PermissionDenied` or `AccessDenied` from Cloud Providers
* **Cause:** The connection to the cloud provider succeeded, but the runner's IAM Role or Service Principal does not have read permissions for the requested secret path.
* **Solution:** Contact your Security Administrator to grant `GetSecretValue` (AWS) or `Get Secrets` (Azure) permissions for the specific secret path.
