from sqlmodel import SQLModel, Field
from sqlalchemy import Column, Text
from typing import Optional

class ctlTransformationDependencyMaster(SQLModel, table=True):
    """
    Stores transformation dependencies between datasets.
    Defines join logic, primary keys, and custom queries used during transformations.
    """

    process_id: int = Field(
        primary_key=True, description="ID of the transformation process."
    )
    transformation_step: Optional[str] = Field(
        default=None,
        description="Order of transformation step (e.g., 'JOIN', 'AGGREGATE').",
        sa_column=Column(Text),
    )
    dataset_id: int = Field(
        primary_key=True, description="ID of the current dataset being transformed."
    )
    depedent_dataset_id: int = Field(
        primary_key=True,
        description="ID of the dependent dataset needed for transformation.",
    )
    transformation_type: Optional[str] = Field(
        default=None,
        description="Type of transformation logic (e.g., JOIN, UNION, CUSTOM).",
        sa_column=Column(Text),
    )
    join_how: Optional[str] = Field(
        default=None,
        description="Join type (e.g., inner, left, outer).",
        sa_column=Column(Text),
    )
    left_table_columns: Optional[str] = Field(
        default=None,
        description="Columns from the left table involved in the join.",
        sa_column=Column(Text),
    )
    right_table_columns: Optional[str] = Field(
        default=None,
        description="Columns from the right table involved in the join.",
        sa_column=Column(Text),
    )
    primary_keys: Optional[str] = Field(
        default=None,
        description="List of primary keys for the resulting transformed dataset.",
        sa_column=Column(Text),
    )
    custom_transformation_type: Optional[str] = Field(
        default="SQL",
        description="The type of custom transformation. i.e Databricks-SQL or Python code or Databricks Notebook.",
        sa_column=Column(Text),
    )
    custom_transformation_query: Optional[str] = Field(
        default=None,
        description="Custom SQL or transformation query when using custom logic.",
        sa_column=Column(Text),
    )
    custom_transformation_script_path: Optional[str] = Field(
        default=None,
        description="Custom Transformation Script Path.",
        sa_column=Column(Text),
    )
    extra_values: Optional[str] = Field(
        default=None,
        description="Additional parameters or metadata required for the transformation.",
        sa_column=Column(Text),
    )
