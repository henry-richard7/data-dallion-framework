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
from pyspark.sql.functions import to_date, to_timestamp
from data_dallion_framework.Common.Models.ColumnMetadata import CtlColumnMetadata


class SchemaCaster:
    def __init__(self, df: DataFrame, schema_config: list[CtlColumnMetadata]):
        self.schema_config = schema_config
        self.df = df

    def perform_casting(self):
        for column_config in self.schema_config:
            column_name = column_config.column_name
            column_data_type = column_config.column_data_type.lower()
            column_date_format = column_config.column_date_format

            if column_data_type == "integer":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(IntegerType())
                )
            elif column_data_type == "float":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(FloatType())
                )
            elif column_data_type == "double":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(DoubleType())
                )
            elif column_data_type == "long":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(LongType())
                )
            elif column_data_type == "string":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(StringType())
                )
            elif column_data_type == "boolean":
                self.df = self.df.withColumn(
                    column_name, self.df[column_name].cast(BooleanType())
                )

            elif column_data_type == "date":
                if column_date_format:
                    self.df = self.df.withColumn(
                        column_name, to_date(self.df[column_name], column_date_format)
                    )
                else:
                    self.df = self.df.withColumn(
                        column_name, self.df[column_name].cast(DateType())
                    )

            elif column_data_type == "timestamp":
                if column_date_format:
                    self.df = self.df.withColumn(
                        column_name,
                        to_timestamp(self.df[column_name], column_date_format),
                    )
                else:
                    self.df = self.df.withColumn(
                        column_name, self.df[column_name].cast(TimestampType())
                    )

        return self.df
