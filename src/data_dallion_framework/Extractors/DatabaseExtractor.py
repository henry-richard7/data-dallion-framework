import traceback

from data_dallion_framework.Common import FileNameGenerator, OrchestrationProcess
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail

from datetime import datetime
from pathlib import Path
import csv
from json import loads as json_loads

from pyspark.sql import SparkSession


class DatabaseExtractor:
    """
    Extracts tabular data from specific relational databases mapped by JDBC connections.

    It executes an underlying SQL query and persists the result set onto the targeted inbound filesystem.
    """
    def __init__(
        self,
        spark: SparkSession,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        inbound_location,
        connection_config,
        query,
        outbound_source_platform,
        outbound_source_file_format,
        file_pattern,
        pre_ingestion_dataset_id,
        outbound_file_delimiter,
        process_id,
    ):
        """
        Initializes the DatabaseExtractor and immediately begins the JDBC extraction sequence.

        Args:
            spark (SparkSession): Current active Spark application context to use for ingestion runtime.
            pre_ingestion_logs (list[logDataAcquisitionDetail]): Historical log details tracking extraction executions.
            inbound_location (str): Standardized filesystem landing path destination.
            connection_config (str/dict): Parsed JSON authentication combinations defining connection string rules.
            query (str): Defined relational database executable native SQL string.
            outbound_source_platform (str): Name representing source DBMS architecture configuration details.
            outbound_source_file_format (str): Extension structure mapping defining flatfile saves (e.g. csv).
            file_pattern (str): The regex configurations building mapped outgoing files.
            pre_ingestion_dataset_id (int): Matching structural metadata orchestrator relational ID.
            outbound_file_delimiter (str): Data flattening separator character parameter constraint.
            process_id (int): Correlated ingestion run state ID to pass onto process log creation blocks.
        """
        connection_config: dict = json_loads(connection_config)
        file_pattern = (
            file_pattern.split(".")[0] if "." in file_pattern else file_pattern
        )
        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True)
        except:
            pass

        save_file_name = FileNameGenerator.file_name_generator(file_pattern)
        file_save_name = (
            f"{inbound_location}{save_file_name}.{outbound_source_file_format}"
        )

        if file_save_name not in pre_ingestion_processed_files:
            start_time = datetime.now()
            batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

            try:
                df = (
                    spark.read.format("jdbc")
                    .options(**connection_config)
                    .option("query", query)
                    .load()
                )

                results = [row.asDict() for row in df.collect()]

                with open(file_save_name, mode="w", newline="") as file:
                    writer = csv.DictWriter(
                        file,
                        fieldnames=df.columns,
                        delimiter=outbound_file_delimiter,
                    )
                    writer.writeheader()
                    writer.writerows(results)

                with OrchestrationProcess.OrchestrationProcess() as orch_process:
                    orch_process.insert_log_data_acquisition_detail(
                        log_data_acquisition=logDataAcquisitionDetail(
                            batch_id=batch_id,
                            process_id=process_id,
                            run_date=datetime.now().date(),
                            outbound_source_location="DATABASE",
                            inbound_file_location=file_save_name,
                            pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                            status="SUCCEEDED",
                            start_time=start_time,
                            end_time=datetime.now(),
                        )
                    )

            except Exception as e:
                with OrchestrationProcess.OrchestrationProcess() as orch_process:
                    orch_process.insert_log_data_acquisition_detail(
                        log_data_acquisition=logDataAcquisitionDetail(
                            batch_id=batch_id,
                            process_id=process_id,
                            run_date=datetime.now().date(),
                            outbound_source_location="DATABASE",
                            exception_details=traceback.format_exc(),
                            inbound_file_location=None,
                            pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                            status="FAILED",
                            start_time=start_time,
                            end_time=datetime.now(),
                        )
                    )
                raise
        else:
            raise Exception(
                f"{save_file_name} File is already moved to inbound location."
            )
