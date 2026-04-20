from sqlmodel import SQLModel, Field
from typing import Optional
from sqlalchemy import Column, Text, String


class ctlDatasetMaster(SQLModel, table=True):
    """
    A persistent ORM model representing configuration metadata for dataset layers.

    Captures critical ingestion, transformation, destination, and layer properties
    required by the framework's orchestration engine to execute processing steps correctly.
    """
    process_id: int = Field(
        primary_key=True, description="Unique identifier for the data pipeline process."
    )
    dataset_id: int = Field(
        primary_key=True, description="Unique identifier for the dataset."
    )

    dataset_name: Optional[str] = Field(
        default=None, sa_column=Column(Text), description="Name of the dataset."
    )
    dataset_type: Optional[str] = Field(
        default=None,
        sa_column=Column(String(50)),
        description="Type of dataset (e.g., RAW, Bronze, Silver, Gold).",
    )
    inbound_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="File system path where raw data is initially received.",
    )
    inbound_file_pattern: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Pattern used to match incoming files (supports date patterns and regex).",
    )
    inbound_static_file_pattern: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Flag indicating if the inbound filename is static ('Y'/'N').",
    )
    inbound_file_format: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Format of the source file (e.g., CSV, JSON, XML).",
    )
    inbound_file_delimiter: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Delimiter used in flat files (e.g., comma, tab).",
    )
    landing_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where data is stored after landing (RAW/Landing Layer).",
    )
    landing_table: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Target table name for structured storage in the Landing Layer.",
    )
    landing_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the Landing Layer table.",
    )
    data_standardisation_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where standardized version of the dataset is stored.",
    )
    data_standardisation_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the Standardized Layer.",
    )
    dqm_error_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Location where data quality checked version is stored.",
    )
    dqm_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the DQM Layer.",
    )
    staging_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where data is stored before transformation into final format.",
    )
    staging_table: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Table name for the Staging Layer.",
    )
    staging_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the Staging Layer.",
    )
    transformation_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where transformed dataset is stored.",
    )
    transformation_table: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Table name for the Transformation Layer.",
    )
    transformation_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the Transformation Layer.",
    )
    archive_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Archive path for historical versions of the dataset.",
    )
    publish_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Final destination where the dataset is published for consumption.",
    )
    publish_table: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Table name for the Published Layer.",
    )
    publish_partition_columns: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Partitioning strategy for the Published Layer.",
    )
    table_location_type: Optional[str] = Field(
        default="MANAGED",
        sa_column=Column(Text),
        description="The table location type. MANAGED or EXTERNAL.",
    )
    external_storage_prefix: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="The protocol prefix for external location. Eg. s3a, s3, adls",
    )
