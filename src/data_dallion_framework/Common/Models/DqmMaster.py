from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text
from typing import Optional

class ctlDqmMasterDtl(SQLModel, table=True):
    """
    Configuration table for Data Quality Management (DQM) rules applied to datasets.
    Defines quality checks, thresholds, and criticality levels for validation.
    """

    qc_id: int = Field(
        primary_key=True,
        description="Unique ID for the data quality check."
    )
    process_id: Optional[int] = Field(
        default=None,
        description="ID of the process associated with the QC rule."
    )
    dataset_id: Optional[int] = Field(
        default=None,
        description="ID of the dataset being validated."
    )
    column_name: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Name of the column being validated."
    )
    qc_type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Type of quality check (e.g., not_null, unique, range, regex)."
    )
    qc_param: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Parameters for the QC rule (e.g., min/max values, regex pattern)."
    )
    active_flag: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Whether the QC rule is active ('Y'/'N')."
    )
    qc_filter: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Optional filter condition for applying the QC rule."
    )
    criticality: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Severity level of the QC failure (e.g., NC, C)."
    )
    criticality_threshold_pct: Optional[int] = Field(
        default=None,
        description="Threshold percentage for criticality violation (e.g., 5% nulls allowed)."
    )
