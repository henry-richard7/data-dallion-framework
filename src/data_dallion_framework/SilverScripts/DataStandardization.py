from datetime import datetime
from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models.Logs import logDataStandardisationDtl

from pyspark.sql import SparkSession
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
from json import loads as json_loads


class DataStandardization:
    def __init__(
        self,
        spark: SparkSession,
        process_id,
        dataset_id,
        landing_location,
        data_standardisation_location,
        data_standardisation_partition_columns,
    ):
        with OrchestrationProcess.OrchestrationProcess() as orch_process:

            unprocessed_files = orch_process.get_data_standardisation_unprocessed_files(
                process_id=process_id, dataset_id=dataset_id
            )

            data_standards = orch_process.get_data_standard_dtl(
                dataset_id=dataset_id
            )
            
            column_meta_data_details = orch_process.get_ctl_column_metadata(
                        dataset_id=dataset_id,
                    )
            column_meta_data_source_column_names: list[str] = [
            x.source_column_name.lower() for x in column_meta_data_details
        ]

        if len(unprocessed_files) != 0:
            for ingestion_processed_file in unprocessed_files:
                start_datetime = datetime.now()

                batch_id = ingestion_processed_file.batch_id
                
                
                pre_df = (
                    spark.read.format("delta")
                    .load(landing_location)
                    .filter(col("batch_id") == batch_id)
                )
                
                rename_mapping = {
                                old: new
                                for old, new in zip(
                                    pre_df.columns, column_meta_data_source_column_names
                                )
                            }
                for old_name, new_name in rename_mapping.items():
                                pre_df = pre_df.withColumnRenamed(old_name, new_name)

                if len(data_standards) != 0:
                    for data_standard in data_standards:
                        if data_standard.function_name == "padding":
                            parsed_json = json_loads(data_standard.function_params)

                            padding_type = parsed_json["type"]
                            padding_length = int(parsed_json["length"])
                            padding_value = parsed_json["padding_value"]

                            if padding_type == "left":
                                pre_df = pre_df.withColumn(
                                    data_standard.column_name,
                                    lpad(
                                        col(data_standard.column_name),
                                        len=padding_length,
                                        pad=padding_value,
                                    ),
                                )
                            else:
                                pre_df = pre_df.withColumn(
                                    data_standard.column_name,
                                    rpad(
                                        col(data_standard.column_name),
                                        len=padding_length,
                                        pad=padding_value,
                                    ),
                                )

                        elif data_standard.function_name == "trim":
                            pre_df = pre_df.withColumn(
                                data_standard.column_name,
                                trim(col(data_standard.column_name)),
                            )

                        elif data_standard.function_name == "blank_conversion":
                            pre_df = pre_df.withColumn(
                                data_standard.column_name,
                                trim(
                                    regexp_replace(
                                        col(data_standard.column_name),
                                        pattern=r"\s+",
                                        replacement=" ",
                                    )
                                ),
                            )

                        elif data_standard.function_name == "replace":
                            parsed_json = json_loads(data_standard.function_params)

                            to_replace_pattern = parsed_json["value"]
                            replacement = parsed_json["value"]

                            pre_df = pre_df.withColumn(
                                data_standard.column_name,
                                regexp_replace(
                                    col(data_standard.column_name),
                                    pattern=to_replace_pattern,
                                    replacement=replacement,
                                ),
                            )

                        elif data_standard.function_name == "type_conversion":
                            parsed_json = json_loads(data_standard.function_params)
                            type_conversion_type = parsed_json["type"]

                            if type_conversion_type == "lower":
                                pre_df = pre_df.withColumn(
                                    data_standard.column_name,
                                    lower(
                                        col(data_standard.column_name),
                                    ),
                                )
                            else:
                                pre_df = pre_df.withColumn(
                                    data_standard.column_name,
                                    upper(
                                        col(data_standard.column_name),
                                    ),
                                )

                        elif data_standard.function_name == "sub_string":
                            parsed_json = json_loads(data_standard.function_params)

                            start_index = int(parsed_json["start_index"])
                            length = int(parsed_json["length"])

                            pre_df = pre_df.withColumn(
                                data_standard.column_name,
                                substring(
                                    col(data_standard.column_name),
                                    start_index,
                                    length,
                                ),
                            )

                        else:
                            raise Exception(
                                f"Unknown Data Standardization Function: {data_standard.function_name}"
                            )
                    pre_df = pre_df.withColumn("batch_id", lit(batch_id))
                    pre_df.write.format("delta").mode("append").partitionBy(
                        data_standardisation_partition_columns
                    ).save(data_standardisation_location)

                    with OrchestrationProcess.OrchestrationProcess() as orch_process:
                        orch_process.insert_log_data_acquisition_detail(
                            
                            log_data_acquisition=logDataStandardisationDtl(
                            batch_id=batch_id,
                            process_id=process_id,
                            dataset_id=dataset_id,
                            source_file=ingestion_processed_file.source_file,
                            data_standardisation_location=data_standardisation_location,
                            status="SUCCEEDED",
                            start_datetime=start_datetime,
                            end_datetime=datetime.now(),
                            exception_details=datetime.now(),
                            )
                        )

                else:
                    pre_df = pre_df.withColumn("batch_id", lit(batch_id))
                    pre_df.write.format("delta").mode("append").partitionBy(
                        data_standardisation_partition_columns
                    ).save(data_standardisation_location)

                    with OrchestrationProcess.OrchestrationProcess() as orch_process:
                        orch_process.insert_log_data_acquisition_detail(
                            log_data_acquisition=logDataStandardisationDtl(
                            batch_id=batch_id,
                            process_id=process_id,
                            dataset_id=dataset_id,
                            source_file=ingestion_processed_file.source_file,
                            data_standardisation_location=data_standardisation_location,
                            status="SUCCEEDED",
                            start_datetime=start_datetime,
                            end_datetime=datetime.now(),
                            )
                        )
        else:
            with OrchestrationProcess.OrchestrationProcess() as orch_process:
                orch_process.insert_log_data_acquisition_detail(
                    log_data_acquisition=logDataStandardisationDtl(
                    batch_id=None,
                    process_id=process_id,
                    dataset_id=dataset_id,
                    source_file=None,
                    data_standardisation_location=data_standardisation_location,
                    status="FAILED",
                    start_datetime=start_datetime,
                    end_datetime=datetime.now(),
                    exception_details=f"Data Standardisation is already processed for Dataset ID {dataset_id}.",
                    )
                )
            raise Exception(
                f"Data Standardisation is already processed for Dataset ID {dataset_id}."
            )
