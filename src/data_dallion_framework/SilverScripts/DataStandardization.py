from datetime import datetime
from json import loads as json_loads
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, substring, lpad, rpad, trim, regexp_replace, upper, lower, lit
)
from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models.Logs import logDataStandardisationDtl
from data_dallion_framework.Common.Models.DataStandardisation import ctlDataStandardisationDtl


class DataStandardization:

    def __init__(self, spark: SparkSession, process_id, dataset_id,
                 landing_location, data_standardisation_location,
                 data_standardisation_partition_columns):
        self.spark = spark
        self.process_id = process_id
        self.dataset_id = dataset_id
        self.landing_location = landing_location
        self.data_standardisation_location = data_standardisation_location
        self.partition_columns = data_standardisation_partition_columns

        self.run_standardization()

    def build_column_transformations(self, df:DataFrame, data_standards:list[ctlDataStandardisationDtl]):
        """
        Creates a mapping of column_name → transformed_column_expression.
        All transformations are planned in one go for Spark optimization.
        """
        transformations = {c: col(c) for c in df.columns}

        for standard in data_standards:
            params = json_loads(standard.function_params) if standard.function_params else {}
            fn = standard.function_name.lower()
            col_name = standard.column_name

            if fn == "padding":
                pad_fn = lpad if params.get("type") == "left" else rpad
                transformations[col_name] = pad_fn(
                    transformations[col_name],
                    int(params["length"]),
                    params.get("padding_value", " ")
                )

            elif fn == "trim":
                transformations[col_name] = trim(transformations[col_name])

            elif fn == "blank_conversion":
                transformations[col_name] = trim(
                    regexp_replace(transformations[col_name], r"\s+", " ")
                )

            elif fn == "replace":
                transformations[col_name] = regexp_replace(
                    transformations[col_name],
                    params.get("to_replace", ""),
                    params.get("value", "")
                )

            elif fn == "type_conversion":
                transformations[col_name] = (
                    lower(transformations[col_name])
                    if params.get("type") == "lower"
                    else upper(transformations[col_name])
                )

            elif fn == "sub_string":
                transformations[col_name] = substring(
                    transformations[col_name],
                    int(params["start_index"]),
                    int(params["length"])
                )

            else:
                raise ValueError(f"Unknown Data Standardization Function: {standard.function_name}")

        return transformations

    def write_and_log(self, df, batch_id, start_datetime, source_file, status, exception_details=None):
        """
        Writes DataFrame to Delta format and logs process details.
        """
        df.write.format("delta") \
            .mode("append") \
            .partitionBy(self.partition_columns) \
            .save(self.data_standardisation_location)

        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            orch_process.insert_log_data_acquisition_detail(
                log_data_acquisition=logDataStandardisationDtl(
                    batch_id=batch_id,
                    process_id=self.process_id,
                    dataset_id=self.dataset_id,
                    source_file=source_file,
                    data_standardisation_location=self.data_standardisation_location,
                    status=status,
                    start_datetime=start_datetime,
                    end_datetime=datetime.now(),
                    exception_details=exception_details
                )
            )

    def run_standardization(self):
        """
        Main execution method:
        - Fetches unprocessed files and transformation rules
        - Reads data
        - Applies transformations in a single Spark plan
        - Writes output and logs status
        """
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            unprocessed_files = orch_process.get_data_standardisation_unprocessed_files(
                process_id=self.process_id, dataset_id=self.dataset_id
            )
            data_standards = orch_process.get_data_standard_dtl(dataset_id=self.dataset_id)
            column_meta_data_details = orch_process.get_ctl_column_metadata(dataset_id=self.dataset_id)

        col_names = [x.source_column_name.lower() for x in column_meta_data_details]

        if not unprocessed_files:
            self.write_and_log(
                self.spark.createDataFrame([], schema=None),
                None, datetime.now(), None, "FAILED",
                f"Data Standardisation is already processed for Dataset ID {self.dataset_id}."
            )
            raise Exception(f"Data Standardisation is already processed for Dataset ID {self.dataset_id}.")

        for file in unprocessed_files:
            start_time = datetime.now()
            try:
                df = (
                    self.spark.read.format("delta")
                    .load(self.landing_location)
                    .filter(col("batch_id") == file.batch_id)
                )

                # Rename columns all at once if counts match
                if len(df.columns) == len(col_names):
                    df = df.toDF(*col_names)

                # Build transformations
                transformations = self.build_column_transformations(df, data_standards)

                # Apply transformations with ALIAS to preserve schema compatibility
                df = df.select([transformations[c].alias(c) for c in df.columns])

                # Add batch_id column
                df = df.withColumn("batch_id", lit(file.batch_id))

                # Write output and log success
                self.write_and_log(df, file.batch_id, start_time, file.source_file, "SUCCEEDED")

            except Exception as e:
                # Write empty dataframe & log failure (schema-safe if df exists)
                self.write_and_log(
                    self.spark.createDataFrame([], schema=df.schema if 'df' in locals() else None),
                    file.batch_id, start_time, file.source_file, "FAILED", str(e)
                )
                raise
