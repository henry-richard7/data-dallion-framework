from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text
from typing import Optional


class CtlColumnMetadata(SQLModel, table=True):
    """
    Maintains metadata about individual columns in datasets including data types, descriptions, and mappings.
    """

    column_id: Optional[int] = Field(
        primary_key=True, default=None, description="Unique identifier for the column."
    )
    table_name: str = Field(
        primary_key=True,
        max_length=255,
        description="Name of the table for which the column metadata is defined.",
    )
    dataset_id: int = Field(
        primary_key=True, description="ID of the dataset this column belongs to."
    )
    column_name: str = Field(
        primary_key=True, max_length=255, description="Name of the column."
    )
    column_data_type: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Data type of the column (e.g., string, integer, float, date).",
    )
    column_date_format: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Date format if column_data_type is Date (e.g., 'yyyy-MM-dd').",
    )
    column_description: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Description explaining the meaning or business context of the column.",
    )
    column_json_mapping: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="JSON path mapping used to extract value from raw JSON input.",
    )

    source_column_name: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Original column name from the source dataset.",
    )
    column_sequence_number: Optional[int] = Field(
        default=None, description="Order of the column within the table structure."
    )
    column_tag: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Tags for column usage in dashboards (e.g., KPI, KPI-Filter).",
    )
