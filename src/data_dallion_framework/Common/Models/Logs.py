from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text, BigInteger, String, Identity, Integer
from typing import Optional
from datetime import date, datetime


class logDataAcquisitionDetail(SQLModel, table=True):
    """
    Logs execution details of data acquisition processes including status, timing, and exception information.
    """

    seq_no: Optional[int] = Field(
        description="Auto-incremented sequence number.",
        sa_column=Column(Integer, Identity(start=1, always=True), primary_key=True),
    )
    batch_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Batch ID of the running process.",
    )
    run_date: Optional[date] = Field(
        default=None, description="Date on which the process was executed."
    )
    process_id: Optional[int] = Field(
        default=None, description="Unique ID of the process."
    )
    pre_ingestion_dataset_id: Optional[int] = Field(
        default=None, description="Dataset ID of the Bronze Layer."
    )
    outbound_source_location: Optional[str] = Field(
        default=None, sa_column=Column(Text), description="Source file location."
    )
    inbound_file_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Inbound file storage location.",
    )
    status: Optional[str] = Field(
        default="IN-PROGRESS",
        sa_column=Column(String(50)),
        description="Current status of the BRONZE dataset process.",
    )
    exception_details: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Details of any exceptions encountered during processing.",
    )
    start_time: Optional[datetime] = Field(
        default=None, description="Start time of the process."
    )
    end_time: Optional[datetime] = Field(
        default=None, description="End time of the process."
    )


class logRawProcessDtl(SQLModel, table=True):
    """
    Logs processing details for files in the RAW/Landing layer including status and performance metrics.
    """

    file_id: Optional[int] = Field(
        description="Unique identifier for the processed file.",
        sa_column=Column(Integer, Identity(start=1, always=True), primary_key=True),
    )
    run_date: Optional[date] = Field(
        default_factory=date.today, description="Date when the file was processed."
    )
    batch_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Batch ID associated with the file processing.",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the process responsible for file ingestion."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset being processed."
    )
    source_file: Optional[str] = Field(
        default=None,
        sa_column=Column(String(4000)),
        description="Name of the source file being processed.",
    )
    landing_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where the file was saved in Landing Layer.",
    )
    file_status: Optional[str] = Field(
        default="IN-PROGRESS",
        sa_column=Column(String(50)),
        description="Status of the file processing.",
    )
    exception_details: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Error message if file processing failed.",
    )
    file_process_start_time: Optional[datetime] = Field(
        default=None, description="Start time of file processing."
    )
    file_process_end_time: Optional[datetime] = Field(
        default=None, description="End time of file processing."
    )


class logDataStandardisationDtl(SQLModel, table=True):
    """
    Logs execution details of data standardization processes.
    Tracks transformation steps and any errors that occurred.
    """

    seq_no: Optional[int] = Field(
        default=None,
        description="Auto-incremented sequence number for logging entries.",
        sa_column=Column(
            Integer,
            Identity(start=1, always=True),
            primary_key=True,
        ),
    )
    batch_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Batch ID associated with the standardization process.",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the data pipeline process involved."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset undergoing standardization."
    )
    source_file: Optional[str] = Field(
        default=None,
        sa_column=Column(String(4000)),
        description="Name of the source file processed during standardization.",
    )
    data_standardisation_location: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Path where standardized output was written.",
    )
    status: Optional[str] = Field(
        default=None,
        sa_column=Column(String(50)),
        description="Current status of the standardization process (e.g., SUCCESS, FAILED).",
    )
    exception_details: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Details of any exceptions encountered during processing.",
    )
    start_datetime: Optional[datetime] = Field(
        default=None, description="Start time of the standardization process."
    )
    end_datetime: Optional[datetime] = Field(
        default=None, description="End time of the standardization process."
    )


class logDqmDtl(SQLModel, table=True):
    """
    Logs results of data quality checks performed on datasets.
    Tracks error counts, failure thresholds, and execution times for DQM rules.
    """

    seq_no: Optional[int] = Field(
        default=None,
        description="Auto-incremented sequence number for logging entries.",
        sa_column=Column(
            Integer,
            Identity(start=1, always=True),
            primary_key=True,
        ),
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the data pipeline process involved."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset being validated."
    )
    batch_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Batch ID associated with the DQM run.",
    )
    source_file: Optional[str] = Field(
        default=None,
        sa_column=Column(String(4000)),
        description="Name of the source file processed during DQM validation.",
    )
    column_name: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Name of the column being validated.",
    )
    qc_type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Type of quality check performed (e.g., not_null, unique).",
    )
    qc_param: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Parameters used for the QC rule (e.g., regex, value ranges).",
    )
    qc_filter: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Filter condition applied during QC validation.",
    )
    criticality: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Severity level of the QC failure (e.g., HIGH, MEDIUM).",
    )
    criticality_threshold_pct: Optional[int] = Field(
        default=None, description="Threshold percentage for criticality violation."
    )
    error_count: Optional[int] = Field(
        default=None, description="Number of records failing the QC rule."
    )
    error_pct: Optional[int] = Field(
        default=None, description="Percentage of records failing the QC rule."
    )
    status: Optional[str] = Field(
        default=None,
        sa_column=Column(String(50)),
        description="Status of the QC validation (e.g., PASSED, FAILED).",
    )
    dqm_start_time: Optional[datetime] = Field(
        default=None, description="Start time of the DQM validation process."
    )
    dqm_end_time: Optional[datetime] = Field(
        default=None, description="End time of the DQM validation process."
    )


class logTransformationDtl(SQLModel, table=True):
    """
    Logs execution details of transformation jobs.
    Tracks performance metrics and failures related to dataset transformations.
    """

    seq_no: Optional[int] = Field(
        default=None,
        description="Auto-incremented sequence number for logging entries.",
        sa_column=Column(
            Integer,
            Identity(start=1, always=True),
            primary_key=True,
        ),
    )
    batch_id: Optional[int] = Field(
        default=None,
        sa_column=Column(BigInteger),
        description="Batch ID associated with the transformation process.",
    )
    data_date: Optional[date] = Field(
        default=None,
        description="Date of the data being transformed (typically partitioning key).",
    )
    process_id: Optional[int] = Field(
        default=None, description="ID of the transformation process."
    )
    dataset_id: Optional[int] = Field(
        default=None, description="ID of the dataset undergoing transformation."
    )
    source_file: Optional[str] = Field(
        default=None,
        sa_column=Column(String(4000)),
        description="Name of the source file processed during transformation.",
    )
    status: Optional[str] = Field(
        default=None,
        sa_column=Column(String(50)),
        description="Current status of the transformation job (e.g., SUCCESS, FAILED).",
    )
    exception_details: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Details of any exceptions encountered during transformation.",
    )
    transformation_start_time: Optional[datetime] = Field(
        default=None, description="Start time of the transformation job."
    )
    transformation_end_time: Optional[datetime] = Field(
        default=None, description="End time of the transformation job."
    )
