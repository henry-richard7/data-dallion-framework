from data_dallion_framework.Common import OrchestrationProcess, Constants
from data_dallion_framework.Common.Models import DatasetMaster
from concurrent.futures import ThreadPoolExecutor
from data_dallion_framework.SilverScripts import (
    DataStandardization,
    DataQualityCheck,
)

from time import time
from pyspark.sql import SparkSession


class SilverLayerProcess:
    """
    Coordinates the execution of the entire Silver Layer data processing phase.

    This involves fetching datasets marked for the Silver Layer and running
    data standardization and data quality check workflows in parallel across them.
    """
    def __init__(self, spark: SparkSession, process_id, env="dev"):
        """
        Initializes the SilverLayerProcess and immediately begins execution.

        Args:
            spark (SparkSession): The active SparkSession used for data processing.
            process_id (int/str): The unique identifier for the current orchestration process.
            env (str, optional): The target environment prefix (e.g., 'dev', 'prod'). Defaults to "dev".
        """
        self.spark: SparkSession = spark
        self.env = env

        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            self.silver_datasets = orch_process.get_dataset_master(
                process_id=process_id, dataset_type=Constants.LAYER_SILVER
            )

        if not self.silver_datasets:
            return

        with ThreadPoolExecutor(
            max_workers=min(5, len(self.silver_datasets))
        ) as executor:
            futures = [
                executor.submit(self._handle_silver_layer_process, silver_dataset)
                for silver_dataset in self.silver_datasets
            ]

            for future in futures:
                try:
                    future.result()  # Wait for each thread to finish
                except Exception as e:
                    raise

    def _handle_silver_layer_process(
        self, dataset_master: DatasetMaster.ctlDatasetMaster
    ):
        """
        Executes the chained processes for a single dataset within the Silver Layer.

        Sequentially runs DataStandardization followed by DataQualityCheck,
        and manages performance timing for each step.

        Args:
            dataset_master (DatasetMaster.ctlDatasetMaster): Configuration attributes for the dataset.
        """

        DataStandardization_start_time = time()
        DataStandardization.DataStandardization(
            spark=self.spark,
            process_id=dataset_master.process_id,
            dataset_id=dataset_master.dataset_id,
            landing_location=dataset_master.landing_location,
            data_standardisation_location=dataset_master.data_standardisation_location,
            data_standardisation_partition_columns=dataset_master.data_standardisation_partition_columns,
            table_location_type=dataset_master.table_location_type,
            env=self.env,
            landing_table_name=dataset_master.landing_table,
        )
        DataStandardization_end_time = round(
            (time() - DataStandardization_start_time) / 60, 2
        )

        DataQualityCheck_start_time = time()
        DataQualityCheck.DataQualityCheck(
            spark=self.spark,
            process_id=dataset_master.process_id,
            dataset_id=dataset_master.dataset_id,
            data_standardisation_location=dataset_master.data_standardisation_location,
            dqm_error_location=dataset_master.dqm_error_location,
            staging_location=dataset_master.staging_location,
            staging_partition_columns=dataset_master.staging_partition_columns,
            staging_table_name=dataset_master.staging_table,
            publish_location=dataset_master.publish_location,
            publish_partition_columns=dataset_master.publish_partition_columns,
            publish_table_name=dataset_master.publish_table,
            table_location_type=dataset_master.table_location_type,
            env=self.env,
        )
        DataQualityCheck_end_time = round(
            (time() - DataQualityCheck_start_time) / 3600, 6
        )

        # StagingDDL_start_time = time()
        # StagingDDL.StagingDDL(
        #     process_id=dataset_master.process_id,
        #     env="dev",
        #     spark=self.spark,
        #     staging_table=dataset_master.staging_table,
        #     dataset_id=dataset_master.dataset_id,
        # ).execute_ddl(dataset_type="L1")
        # StagingDDL_end_time = round((time() - StagingDDL_start_time) / 3600, 6)
        # print(f"\t Staging DDL completed in {StagingDDL_end_time} Hours.")

        # PublishDDL_start_time = time()
        # PublishDDL.PublishDDL(
        #     process_id=dataset_master.process_id,
        #     env="dev",
        #     spark=self.spark,
        #     publish_table=dataset_master.publish_table,
        #     dataset_id=dataset_master.dataset_id,
        # ).execute_ddl(dataset_type="L1")
        # PublishDDLL_end_time = round((time() - PublishDDL_start_time) / 3600, 6)
