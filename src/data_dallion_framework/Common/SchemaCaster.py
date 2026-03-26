from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    StringType,
    DateType,
    TimestampType,
    BooleanType,
    DoubleType,
    LongType,
)
from pyspark.sql.functions import col, to_date, to_timestamp
from data_dallion_framework.Common.Models.ColumnMetadata import CtlColumnMetadata


class SchemaCaster:
    def __init__(self, df: DataFrame, schema_config: list[CtlColumnMetadata]):
        self.schema_config = schema_config
        self.df = df

    def perform_casting(self):
        cast_exprs = {}
        for column_config in self.schema_config:
            column_name = column_config.column_name
            column_data_type = column_config.column_data_type.lower()
            column_date_format = column_config.column_date_format

            if column_data_type == "integer":
                cast_exprs[column_name] = col(column_name).cast(IntegerType())
            elif column_data_type == "float":
                cast_exprs[column_name] = col(column_name).cast(FloatType())
            elif column_data_type == "double":
                cast_exprs[column_name] = col(column_name).cast(DoubleType())
            elif column_data_type == "long":
                cast_exprs[column_name] = col(column_name).cast(LongType())
            elif column_data_type == "string":
                cast_exprs[column_name] = col(column_name).cast(StringType())
            elif column_data_type == "boolean":
                cast_exprs[column_name] = col(column_name).cast(BooleanType())
            elif column_data_type == "date":
                if column_date_format:
                    cast_exprs[column_name] = to_date(
                        col(column_name), column_date_format
                    )
                else:
                    cast_exprs[column_name] = col(column_name).cast(DateType())

            elif column_data_type == "timestamp":
                if column_date_format:
                    cast_exprs[column_name] = to_timestamp(
                        col(column_name), column_date_format
                    )
                else:
                    cast_exprs[column_name] = to_timestamp(col(column_name))

        select_exprs = [
            cast_exprs[c].alias(c) if c in cast_exprs else col(c)
            for c in self.df.columns
        ]
        self.df = self.df.select(*select_exprs)

        return self.df
