from datetime import datetime
from json import loads as json_loads
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    substring,
    lpad,
    rpad,
    trim,
    regexp_replace,
    upper,
    lower,
    lit,
)
from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models.Logs import logDataStandardisationDtl
from data_dallion_framework.Common.Models.DataStandardisation import (
    ctlDataStandardisationDtl,
)


class DataStandardization:

    def __init__(
        self,
        spark: SparkSession,
        process_id,
        dataset_id,
        landing_location,
        data_standardisation_location,
        data_standardisation_partition_columns,
        table_location_type,
        env,
        landing_table_name,
    ):
        self.spark = spark
        self.process_id = process_id
        self.dataset_id = dataset_id
        self.landing_location = landing_location
        self.data_standardisation_location = data_standardisation_location
        self.partition_columns = data_standardisation_partition_columns.split(",") if data_standardisation_partition_columns else []
        self.table_location_type = table_location_type
        self.env = env
        self.landing_table_name = landing_table_name

        self.run_standardization()

    def build_column_transformations(
        self, df: DataFrame, data_standards: list[ctlDataStandardisationDtl]
    ):
        """
        Creates a mapping of column_name → transformed_column_expression.
        All transformations are planned in one go for Spark optimization.
        """
        transformations = {c: col(c) for c in df.columns}

        for standard in data_standards:
            params = (
                json_loads(standard.function_params) if standard.function_params else {}
            )
            fn = standard.function_name.lower()
            col_name = standard.column_name

            if fn == "padding":
                pad_fn = lpad if params.get("type") == "left" else rpad
                transformations[col_name] = pad_fn(
                    transformations[col_name],
                    int(params["length"]),
                    params.get("padding_value", " "),
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
                    params.get("value", ""),
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
                    int(params["length"]),
                )

            else:
                raise ValueError(
                    f"Unknown Data Standardization Function: {standard.function_name}"
                )

        return transformations

    def write_and_log_batch(self, files: list, status, start_time, exception_details=None):
        """
        Logs status for multiple files in a single pass.
        """
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            for file in files:
                orch_process.insert_log_data_acquisition_detail(
                    log_data_acquisition=logDataStandardisationDtl(
                        batch_id=file.batch_id,
                        process_id=self.process_id,
                        dataset_id=self.dataset_id,
                        source_file=file.source_file,
                        data_standardisation_location=self.data_standardisation_location,
                        status=status,
                        start_datetime=start_time,
                        end_datetime=datetime.now(),
                        exception_details=exception_details,
                    )
                )

    def run_standardization(self):
        """
        Main execution method:
        - Fetches unprocessed files and transformation rules
        - Reads data in bulk
        - Applies transformations in a single Spark plan
        - Writes output and logs status
        """
        start_time = datetime.now()
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            unprocessed_files = orch_process.get_data_standardisation_unprocessed_files(
                process_id=self.process_id, dataset_id=self.dataset_id
            )
            data_standards = orch_process.get_data_standard_dtl(
                dataset_id=self.dataset_id
            )
            column_meta_data_details = orch_process.get_ctl_column_metadata(
                dataset_id=self.dataset_id
            )

        if not unprocessed_files:
            return

        source_column_names = [x.source_column_name for x in column_meta_data_details]
        target_column_names = [x.column_name for x in column_meta_data_details]
        batch_ids = [f.batch_id for f in unprocessed_files]

        try:
            # Optimization: Bulk read all pending batches
            if self.table_location_type.lower() == "external":
                df = (
                    self.spark.read.format("delta")
                    .load(self.landing_location)
                    .filter(col("batch_id").isin(batch_ids))
                )
            else:
                df = self.spark.read.table(f"{self.env}.{self.landing_table_name}").filter(col("batch_id").isin(batch_ids))

            # Strictly check that the input columns (minus batch_id) match expectations
            input_cols = [c for c in df.columns if c != "batch_id"]
            if sorted(input_cols) == sorted(source_column_names):
                # Ensure correct column order for rename
                df_data = df.select(*source_column_names, "batch_id")
                
                # Rename columns
                for old, new in zip(source_column_names, target_column_names):
                    df_data = df_data.withColumnRenamed(old, new)

                # Build transformations
                transformations = self.build_column_transformations(df_data, data_standards)

                # Apply transformations in a single Spark plan
                # Preserve 'batch_id'
                final_cols = [transformations[c].alias(c) for c in target_column_names] + [col("batch_id")]
                df_data = df_data.select(*final_cols)

                # Batch write
                df_data.write.format("delta").mode("append").partitionBy(
                    self.partition_columns or ["batch_id"]
                ).save(self.data_standardisation_location)

                # Log success for all batches
                self.write_and_log_batch(unprocessed_files, "SUCCEEDED", start_time)
            
            else:
                raise Exception(
                    f"Column header mismatch in batch: expected {source_column_names}, found {input_cols}."
                )

        except Exception as e:
            # Log failure for all batches
            self.write_and_log_batch(unprocessed_files, "FAILED", start_time, str(e))
            raise
