from data_dallion_framework.Common import OrchestrationProcess, PatternValidator, DDLGenerator
from data_dallion_framework.Common.Models.Logs import (
    logDataAcquisitionDetail,
    logRawProcessDtl,
)
from data_dallion_framework.Common.Models.Acquisition import (
    ctlDataAcquisitionDetail,
    ctlDataAcquisitionConnectionMaster,
)
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
from pyspark.sql import SparkSession, functions as F


class PerformBronze:
    def __init__(self, spark: SparkSession, process_id, env="dev"):
        self.env = env
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
        self.ddl_gen = DDLGenerator.DDLGenerator()

    def _handle_extraction(self, dataAcquisitionDetail: ctlDataAcquisitionDetail):
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
        
        column_meta_data_source_column_names = [x.source_column_name.lower() for x in column_meta_data_details]

        inbound_path = dataset.inbound_location
        landing_path = dataset.landing_location
        file_pattern = dataset.inbound_file_pattern

        files_in_inbound = [f"{inbound_path.rstrip('/')}/{x}" for x in listdir(inbound_path)]
        raw_completed_files = [x.source_file for x in ingestion_logs]

        new_files = [x for x in set(files_in_inbound) - set(raw_completed_files)
                     if PatternValidator.validate_pattern(file_pattern, x.split("/")[-1])]

        if not new_files:
            return

        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])
        start_time = datetime.now()

        # Optimization: Process all files in a single Spark session if they meet strict schema rules
        try:
            format_str = "parquet" if dataset.inbound_file_format == "parquet" else "csv"
            reader = self.spark.read.format(format_str).option("inferSchema", "true").option("header", "true")
            if format_str == "csv":
                reader = reader.option("delimiter", dataset.inbound_file_delimiter)
            
            # Read all files at once (Spark handles the list)
            df = reader.load(new_files)
            
            dataframe_columns = [x.lower() for x in df.columns]
            
            # Strict schema check: all columns must match metadata
            if sorted(dataframe_columns) != sorted(column_meta_data_source_column_names):
                 raise Exception(f"Column Metadata mismatch in batch. Expected {column_meta_data_source_column_names}, found {dataframe_columns}")

            # Cast to string and align columns
            df = df.select(*[F.col(c).cast("string").alias(c.lower()) for c in column_meta_data_source_column_names])
            df = df.withColumn("batch_id", F.lit(batch_id))

            # External table creation via Jinja2
            partition_cols = dataset.landing_partition_columns.split(",") if dataset.landing_partition_columns else []
            if dataset.table_location_type.lower() == "external":
                columns = [{"name": c.column_name, "type": c.column_data_type} for c in column_meta_data_details]
                ddl = self.ddl_gen.generate_ddl(
                    env=self.env, table_name=dataset.landing_table, columns=columns,
                    partition_by=partition_cols, location=landing_path, is_external=True
                )
                self.spark.sql(ddl)
                df.write.format("delta").mode("append").partitionBy(partition_cols).save(landing_path)
            else:
                df.write.format("delta").mode("append").partitionBy(partition_cols).saveAsTable(f"{self.env}.{dataset.landing_table}")

            # Bulk logging
            with OrchestrationProcess.OrchestrationProcess() as orch:
                for new_file in new_files:
                    orch.insert_log_raw_process_detail(log_raw_process_dtl=logRawProcessDtl(
                        batch_id=batch_id, process_id=self.process_id, dataset_id=dataset.dataset_id,
                        source_file=new_file, landing_location=dataset.landing_location,
                        file_status="SUCCEEDED", file_process_start_time=start_time, file_process_end_time=datetime.now()
                    ))

        except Exception as e:
            # Failure logging
            with OrchestrationProcess.OrchestrationProcess() as orch:
                for new_file in new_files:
                    orch.insert_log_raw_process_detail(log_raw_process_dtl=logRawProcessDtl(
                        process_id=self.process_id, dataset_id=dataset.dataset_id,
                        source_file=new_file, landing_location=dataset.landing_location,
                        file_status="FAILED", exception_details=str(e),
                        file_process_start_time=start_time, file_process_end_time=datetime.now()
                    ))
            raise

    def start_extraction(self):
        with ThreadPoolExecutor(max_workers=min(5, len(self.bronze_datasets))) as executor:
            futures = [executor.submit(self._handle_extraction, dtl) for dtl in self.bronze_datasets]
            for f in futures: f.result()

        with ThreadPoolExecutor(max_workers=min(5, len(self.bronze_dataset_masters))) as executor:
            futures = [executor.submit(self._handle_raw_table_creation, master) for master in self.bronze_dataset_masters]
            for f in futures: f.result()
