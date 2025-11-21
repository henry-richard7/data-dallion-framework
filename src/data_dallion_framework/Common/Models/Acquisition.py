from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text, String
from typing import Optional


class ctlDataAcquisitionConnectionMaster(SQLModel, table=True):
    """
    Stores connection details for external data acquisition platforms (e.g., SFTP, databases).
    Used to securely store credentials and configuration for connecting to source systems.
    """

    outbound_source_platform: str = Field(
        description="The platform in which the source file is stored.",
        sa_column=Column(
            String(4000),
            primary_key=True,
        ),
    )
    credentials_identifier: str = Field(
        description="The unique identifier for the source credentials.",
        sa_column=Column(
            String(4000),
            primary_key=True,
        ),
    )
    connection_config: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="JSON containing the connection details like hostname, port, etc.",
    )
    ssh_private_key: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Private key file contents for SFTP connection authentication.",
    )


class ctlApiConnectionsDtl(SQLModel, table=True):
    """
    Stores detailed API connection configurations including authentication and request parameters.
    """

    seq_no: int = Field(
        primary_key=True, description="Auto-incremented sequence number."
    )
    pre_ingestion_dataset_id: Optional[int] = Field(
        default=None,
        description="The dataset ID for RAW/Bronze layer associated with this API call.",
    )
    credentials_identifier: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="The unique identifier for the source credentials used for the API.",
    )
    type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="The API request type. Expected values: ['TOKEN', 'RESPONSE', 'CUSTOM']",
    )
    token_url: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="URL to fetch a token if type == 'TOKEN'.",
    )
    auth_type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Type of authentication used. Supported types: OAuth, JWT, Basic Auth, CUSTOM.",
    )
    token_type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Type of token used (e.g., Bearer).",
    )
    client_id: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Client ID used to request a token.",
    )
    client_secret: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Client secret used to request a token.",
    )
    username: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Username used for basic authentication.",
    )
    password: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Password used for basic authentication.",
    )
    issuer: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Issuer required for JWT or service account token requests.",
    )
    scope: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Scope required for JWT or service account token requests.",
    )
    private_key: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Private key required for JWT or service account token requests.",
    )
    token_path: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="JSON path where the token exists in the response body.",
    )
    method: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="HTTP method used for the API request (e.g., GET, POST).",
    )
    url: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="API endpoint URL to send the request to.",
    )
    headers: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="HTTP headers to be sent with the request as JSON string.",
    )
    params: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Query parameters to be appended to the request URL.",
    )
    data: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Raw body content sent when making an API request.",
    )
    json_body: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="JSON-formatted body sent when making an API request.",
    )
    body_values: Optional[str] = (
        Field(
            default=None,
            sa_column=Column(Text),
            description="Placeholders in the request body that need dynamic replacement.",
        ),
    )
    ssl_verify: Optional[str] = Field(
        default="Y",
        sa_column=Column(Text),
        description="Flag indicating whether to verify SSL certificates.",
    )


class ctlDataAcquisitionDetail(SQLModel, table=True):
    """
    Contains metadata about each data acquisition process such as source location, file format, query, and destination.
    """

    process_id: int = Field(
        primary_key=True,
        description="The unique ID representing a specific data acquisition process.",
    )
    pre_ingestion_dataset_id: int = Field(
        primary_key=True,
        description="The Bronze Layer dataset ID. Use Salesforce object name if source platform is Salesforce.",
    )
    pre_ingestion_dataset_name: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="The Bronze Layer dataset name.",
    )
    outbound_source_platform: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Platform where the source file is stored (e.g., SFTP, DB, Salesforce).",
    )
    credentials_identifier: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Unique identifier for the source system credentials.",
    )
    outbound_source_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Location where the source file is stored (e.g., directory path or database schema).",
    )
    outbound_source_file_pattern_static: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Flag indicating whether the source filename is static ('Y' or 'N').",
    )
    outbound_source_file_pattern: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="File name pattern of the source file. Supports YYYY-MM-DD and regex patterns.",
    )
    outbound_source_file_format: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Format of the source file (e.g., CSV, JSON, XML).",
    )
    outbound_file_delimiter: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Delimiter used in the source file (e.g., comma, tab).",
    )
    query: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="SQL query to fetch data from a database. Use only when source platform is a database.",
    )
    columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Comma-separated list of columns to select from Salesforce. Use only when source platform is Salesforce.",
    )
    inbound_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Destination location where the acquired data will be saved.",
    )
