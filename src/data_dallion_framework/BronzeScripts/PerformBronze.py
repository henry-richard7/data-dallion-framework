from data_dallion_framework.Common import OrchestrationProcess, PatternValidator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail, logRawProcessDtl
from data_dallion_framework.Common.Models.Acquisition import ctlDataAcquisitionDetail, ctlDataAcquisitionConnectionMaster
from data_dallion_framework.Common.Models.DatasetMaster import ctlDatasetMaster

from datetime import datetime
from os import listdir
import traceback
from concurrent.futures import ThreadPoolExecutor
from data_dallion_framework.Extractors import (
    SftpExtractor,
    S3Extractor,
    APIExtractor,
    DatabaseExtractor,
    SalesforceExtractor,
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit



class PerformExtraction:
    def __init__(self, spark: SparkSession, process_id):
        self.spark: SparkSession = spark
        self.process_id = process_id
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            self.bronze_datasets = orch_process.get_ctl_data_acquisition_detail(
                process_id=process_id
            )
            self.bronze_dataset_masters = orch_process.get_dataset_master(
                process_id=process_id,
                dataset_type="BRONZE",
            )

    def _handle_extraction(
        self, dataAcquisitionDetail: ctlDataAcquisitionDetail
    ):
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            pre_ingestion_logs: logDataAcquisitionDetail = (
                orch_process.get_log_data_acquisition_detail(
                    process_id=dataAcquisitionDetail.process_id,
                    dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    status="SUCCEEDED",
                )
            )

            connection_dtl: ctlDataAcquisitionConnectionMaster = (
                orch_process.get_ctl_data_acquisition_connection_master(
                    outbound_source_platform=dataAcquisitionDetail.outbound_source_platform,
                    credentials_identifier=dataAcquisitionDetail.credentials_identifier,
                )
            )

            if dataAcquisitionDetail.outbound_source_platform == "SFTP":
                SftpExtractor.SFTPExtractor(
                    pre_ingestion_logs=pre_ingestion_logs,
                    inbound_location=dataAcquisitionDetail.inbound_location,
                    pre_ingestion_dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    outbound_source_location=dataAcquisitionDetail.outbound_source_location,
                    file_pattern=dataAcquisitionDetail.outbound_source_file_pattern,
                    file_pattern_static=dataAcquisitionDetail.outbound_source_file_pattern_static,
                    connection_config=connection_dtl.connection_config,
                    ssh_key=connection_dtl.ssh_private_key,
                    process_id=dataAcquisitionDetail.process_id,
                )
            elif dataAcquisitionDetail.outbound_source_platform == "S3":
                S3Extractor.S3Extractor(
                    pre_ingestion_logs=pre_ingestion_logs,
                    inbound_location=dataAcquisitionDetail.inbound_location,
                    outbound_source_location=dataAcquisitionDetail.outbound_source_location,
                    file_pattern_static=dataAcquisitionDetail.outbound_source_file_pattern_static,
                    file_pattern=dataAcquisitionDetail.outbound_source_file_pattern,
                    connection_config=connection_dtl.connection_config,
                    pre_ingestion_dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    process_id=dataAcquisitionDetail.process_id,
                )
            elif dataAcquisitionDetail.outbound_source_platform == "API":
                APIExtractor.APIExtractor(
                    pre_ingestion_logs=pre_ingestion_logs,
                    inbound_location=dataAcquisitionDetail.inbound_location,
                    outbound_source_file_format=dataAcquisitionDetail.outbound_source_file_format,
                    outbound_file_delimiter=dataAcquisitionDetail.outbound_file_delimiter,
                    file_pattern=dataAcquisitionDetail.outbound_source_file_pattern,
                    pre_ingestion_dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    process_id=dataAcquisitionDetail.process_id,
                )

            elif "DATABASE" in dataAcquisitionDetail.outbound_source_platform:
                DatabaseExtractor.DatabaseExtractor(
                    spark=self.spark,
                    pre_ingestion_logs=pre_ingestion_logs,
                    inbound_location=dataAcquisitionDetail.inbound_location,
                    connection_config=connection_dtl.connection_config,
                    query=dataAcquisitionDetail.query,
                    outbound_source_platform=dataAcquisitionDetail.outbound_source_platform,
                    outbound_source_file_format=dataAcquisitionDetail.outbound_source_file_format,
                    file_pattern=dataAcquisitionDetail.outbound_source_file_pattern,
                    pre_ingestion_dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    outbound_file_delimiter=dataAcquisitionDetail.outbound_file_delimiter,
                    process_id=dataAcquisitionDetail.process_id,
                )

            elif (dataAcquisitionDetail.outbound_source_platform == "SALESFORCE") or (
                dataAcquisitionDetail.outbound_source_platform
            ) == "VEEVA":
                SalesforceExtractor.SalesforceExtractor(
                    pre_ingestion_logs=pre_ingestion_logs,
                    inbound_location=dataAcquisitionDetail.inbound_location,
                    outbound_source_file_format=dataAcquisitionDetail.outbound_source_file_format,
                    outbound_file_delimiter=dataAcquisitionDetail.outbound_file_delimiter,
                    file_pattern=dataAcquisitionDetail.outbound_source_file_pattern,
                    pre_ingestion_dataset_id=dataAcquisitionDetail.pre_ingestion_dataset_id,
                    process_id=dataAcquisitionDetail.process_id,
                    columns=dataAcquisitionDetail.columns,
                    pre_ingestion_dataset_name=dataAcquisitionDetail.pre_ingestion_dataset_name,
                    connection_config=connection_dtl.connection_config,
                )
                
    def _handle_raw_table_creation(self, dataset: ctlDatasetMaster):
        """
        Create a raw Delta table in the Bronze layer for files that have been ingested.

        Validates file patterns, filters out already processed files, and writes new ones to the landing zone.

        Args:
            dataset (ctlDatasetMaster): Dataset configuration including paths and partitioning info.

        Raises:
            Exception: If no new unprocessed files are found matching the pattern.
        """
        
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            ingestion_logs = orch_process.get_log_raw_process_dtl(
                process_id=dataset.process_id,
                dataset_id=dataset.dataset_id,
                status="SUCCEEDED",
            )
            
            column_meta_data_details = orch_process.get_ctl_column_metadata(
                dataset_id=dataset.dataset_id
            )
            column_meta_data_source_column_names: list[str] = [
            x.source_column_name.lower() for x in column_meta_data_details
        ]

            inbound_path = dataset.inbound_location
            landing_path = dataset.landing_location
            file_pattern = dataset.inbound_file_pattern

            
            files_in_inbound = [
            f"{inbound_path.rstrip('/')}/{x}" for x in listdir(inbound_path)
        ]
            raw_completed_files = [x.source_file for x in ingestion_logs]

            new_files = set(files_in_inbound) - set(raw_completed_files)
            new_files = [
                x
                for x in new_files
                if PatternValidator.validate_pattern(
                    file_pattern=file_pattern,
                    file_name=x.split("/")[-1],
                )
            ]
            if len(new_files) == 0:
                
                raise Exception("No new files found to create raw delta table.")

            for new_file in new_files:

                batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

                start_time = datetime.now()
                try:
                    if dataset.inbound_file_format == "parquet":
                        df = (
                            self.spark.read.format("parquet")
                            .option("inferSchema", "true")
                            .option(
                                "header",
                                ("true"),
                            )
                            .option("delimiter", dataset.inbound_file_delimiter)
                            .load(new_file)
                        )
                    else:

                        df = (
                            self.spark.read.format("csv")
                            .option("inferSchema", "true")
                            .option(
                                "header",
                                ("true"),
                            )
                            .option("delimiter", dataset.inbound_file_delimiter)
                            .load(
                                new_file,
                            )
                        )
                    dataframe_columns = [x.lower() for x in df.columns]
                    if column_meta_data_source_column_names != dataframe_columns:
                        orch_process.insert_log_raw_process_detail(
                        log_raw_process_dtl=logRawProcessDtl(
                            process_id=self.process_id,
                            dataset_id=dataset.dataset_id,
                            source_file=new_file,
                            landing_location=dataset.landing_location,
                            file_status="FAILED",
                            exception_details=f"Column Metadata Columns: {column_meta_data_source_column_names} || File Columns: {dataframe_columns} || Status: Columns not matching with Column Metadata and File.",
                            file_process_start_time=start_time,
                            file_process_end_time=datetime.now(),
                        )
                    )
                        raise Exception(
                                f"Column Metadata Columns: {column_meta_data_source_column_names} || File Columns: {dataframe_columns} || Status: Columns not matching with Column Metadata and File."
                            )
                    df = df.select(
                                [
                                    col(column).cast("string").alias(column)
                                    for column in df.columns
                                ]
                            )
                    
                    df = df.withColumn("batch_id", lit(batch_id))
                    df.write.format("delta").mode("append").partitionBy(
                                dataset.landing_partition_columns.split(",")
                            ).save(landing_path)
                    orch_process.insert_log_raw_process_detail(
                            log_raw_process_dtl=logRawProcessDtl(
                                batch_id=batch_id,
                                process_id=self.process_id,
                                dataset_id=dataset.dataset_id,
                                source_file=new_file,
                                landing_location=dataset.landing_location,
                                file_status="SUCCEEDED",
                                exception_details=None,
                                file_process_start_time=start_time,
                                file_process_end_time=datetime.now(),
                            )
                        )
                    

                except Exception as e:
                    orch_process.insert_log_raw_process_detail(
                        log_raw_process_dtl=logRawProcessDtl(
                            process_id=self.process_id,
                            dataset_id=dataset.dataset_id,
                            source_file=new_file,
                            landing_location=dataset.landing_location,
                            file_status="FAILED",
                            exception_details=traceback.format_exc(),
                            file_process_start_time=start_time,
                            file_process_end_time=datetime.now(),
                        )
                    )
                    

                    raise

    def start_extraction(self):
        with ThreadPoolExecutor(
            max_workers=min(5, len(self.bronze_datasets))
        ) as executor:
            futures = [
                executor.submit(self._handle_extraction, bronze_dataset)
                for bronze_dataset in self.bronze_datasets
            ]

            for future in futures:
                try:
                    future.result()  # Wait for each thread to finish
                except Exception as e:
                    raise
                
        # Raw Inbound To Landing Table
        with ThreadPoolExecutor(
            max_workers=min(
                5, len(self.bronze_dataset_masters)
            )
        ) as executor:
            futures = [
                executor.submit(self._handle_raw_table_creation, bronze_dataset_master)
                for bronze_dataset_master in self.bronze_dataset_masters
            ]

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise
