from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text
from typing import Optional

class ctlDataStandardisationDtl(SQLModel, table=True):
    """
    Configuration table for data standardization rules applied to specific columns.
    Includes functions and parameters for transforming raw data into standardized formats.
    """

    dataset_id: int = Field(
        primary_key=True,
        description="ID of the dataset being standardized."
    )
    column_name: str = Field(
        primary_key=True,
        max_length=255,
        description="Name of the column to which the standardization rule applies."
    )
    function_name: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Name of the function used for standardizing the column (e.g., trim, cast, parse_date)."
    )
    function_params: Optional[str] = Field(
        default=None,
        sa_column=Column(Text),
        description="Parameters passed to the function (as JSON or string)."
    )
