import os
import niquests
import boto3
from json import loads
from typing import Optional
from cryptography.fernet import Fernet

from data_dallion_framework.Common.Logging import get_logger

logger = get_logger(__name__)

def decrypt_value(encrypted_val: str) -> str:
    """Decrypt a locally encrypted value using the symmetric encryption key."""
    cipher_text = encrypted_val[len("encrypted:"):]
    key = os.environ.get("DATADALLION_ENCRYPTION_KEY")
    if not key:
        raise ValueError("DATADALLION_ENCRYPTION_KEY environment variable is not set but an encrypted secret was requested.")
    f = Fernet(key.encode())
    return f.decrypt(cipher_text.encode()).decode()

def encrypt_value(plain_text: str) -> str:
    """Helper to encrypt a plain text value locally."""
    key = os.environ.get("DATADALLION_ENCRYPTION_KEY")
    if not key:
        key = Fernet.generate_key().decode()
        logger.warning(f"DATADALLION_ENCRYPTION_KEY not set. Generated a new key for you: {key}")
        os.environ["DATADALLION_ENCRYPTION_KEY"] = key
    f = Fernet(key.encode())
    cipher_text = f.encrypt(plain_text.encode()).decode()
    return f"encrypted:{cipher_text}"

def get_aws_secret(secret_id: str) -> str:
    """Retrieve secret from AWS Secrets Manager."""
    field = None
    if ":" in secret_id:
        secret_id, field = secret_id.split(":", 1)
        
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)
    
    if "SecretString" in response:
        secret = response["SecretString"]
        if field:
            try:
                secret_dict = loads(secret)
                return str(secret_dict[field])
            except Exception:
                pass
        return secret
    else:
        return response["SecretBinary"].decode("utf-8")

def get_azure_kv_token() -> str:
    """Authenticate with Azure Active Directory to get a Key Vault access token."""
    client_id = os.environ.get("AZURE_CLIENT_ID")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET")
    tenant_id = os.environ.get("AZURE_TENANT_ID")
    
    if client_id and client_secret and tenant_id:
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://vault.azure.net/.default"
        }
        res = niquests.post(url, data=data, timeout=10)
        res.raise_for_status()
        return res.json()["access_token"]
        
    # Try Managed Identity (MSI) IMDS endpoint
    try:
        url = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net"
        headers = {"Metadata": "true"}
        res = niquests.get(url, headers=headers, timeout=2)
        if res.status_code == 200:
            return res.json()["access_token"]
    except Exception:
        pass
        
    raise ValueError("Azure credentials not configured. Please set AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID.")

def get_azure_secret(secret_path: str) -> str:
    """Retrieve secret from Azure Key Vault."""
    field = None
    if ":" in secret_path:
        secret_path, field = secret_path.split(":", 1)
        
    if "/" not in secret_path:
        raise ValueError(f"Invalid Azure secret path '{secret_path}'. Expected format: vault-name/secret-name")
        
    vault_name, secret_name = secret_path.split("/", 1)
    token = get_azure_kv_token()
    
    url = f"https://{vault_name}.vault.azure.net/secrets/{secret_name}?api-version=7.4"
    headers = {"Authorization": f"Bearer {token}"}
    res = niquests.get(url, headers=headers, timeout=10)
    res.raise_for_status()
    
    secret_value = res.json()["value"]
    if field:
        try:
            secret_dict = loads(secret_value)
            return str(secret_dict[field])
        except Exception:
            pass
    return secret_value

def get_vault_secret(secret_path: str) -> str:
    """Retrieve secret from HashiCorp Vault."""
    field = None
    if ":" in secret_path:
        secret_path, field = secret_path.split(":", 1)
        
    vault_addr = os.environ.get("VAULT_ADDR", "http://localhost:8200").rstrip("/")
    vault_token = os.environ.get("VAULT_TOKEN")
    
    if not vault_token:
        raise ValueError("VAULT_TOKEN environment variable is not set.")
        
    parts = secret_path.split("/", 1)
    if len(parts) == 2:
        mount, path = parts[0], parts[1]
        url = f"{vault_addr}/v1/{mount}/data/{path}"
    else:
        url = f"{vault_addr}/v1/secret/data/{secret_path}"
        
    headers = {"X-Vault-Token": vault_token}
    res = niquests.get(url, headers=headers, timeout=10)
    
    if res.status_code == 200:
        secret_data = res.json()["data"]["data"]
    else:
        # Fallback to KV v1 endpoint
        url_v1 = f"{vault_addr}/v1/{secret_path}"
        res_v1 = niquests.get(url_v1, headers=headers, timeout=10)
        res_v1.raise_for_status()
        secret_data = res_v1.json()["data"]
        
    if field:
        if isinstance(secret_data, dict) and field in secret_data:
            return str(secret_data[field])
        elif isinstance(secret_data, str):
            try:
                secret_dict = loads(secret_data)
                return str(secret_dict[field])
            except Exception:
                pass
    elif isinstance(secret_data, dict):
        import json
        return json.dumps(secret_data)
        
    return str(secret_data)

def resolve_secret(val: Optional[str]) -> Optional[str]:
    """Unified secret resolver that handles both prefix-based and provider-based decryption."""
    if not val or not isinstance(val, str):
        return val
        
    # 1. Prefix-based resolution
    if val.startswith("secretsmanager:"):
        return get_aws_secret(val[len("secretsmanager:"):])
    elif val.startswith("aws-secrets:"):
        return get_aws_secret(val[len("aws-secrets:"):])
        
    elif val.startswith("keyvault:"):
        return get_azure_secret(val[len("keyvault:"):])
    elif val.startswith("azure-kv:"):
        return get_azure_secret(val[len("azure-kv:"):])
        
    elif val.startswith("vault:"):
        return get_vault_secret(val[len("vault:"):])
        
    elif val.startswith("encrypted:"):
        return decrypt_value(val)
        
    # 2. Global Provider Fallback (if configured)
    provider = os.environ.get("SECRETS_PROVIDER", "").lower()
    if provider in ("aws", "secretsmanager"):
        try:
            return get_aws_secret(val)
        except Exception:
            pass
    elif provider in ("azure", "keyvault"):
        try:
            return get_azure_secret(val)
        except Exception:
            pass
    elif provider == "vault":
        try:
            return get_vault_secret(val)
        except Exception:
            pass
    elif provider == "encryption":
        try:
            key = os.environ.get("DATADALLION_ENCRYPTION_KEY")
            if key:
                f = Fernet(key.encode())
                return f.decrypt(val.encode()).decode()
        except Exception:
            pass
            
    return val
