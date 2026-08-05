from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models import DatasetMaster
from concurrent.futures import ThreadPoolExecutor

from time import time
from pyspark.sql import SparkSession

from data_dallion_framework.GoldScripts.TransformationScripts import (
    DataQualityCheckTransformation,
    Transformation,
)


class GoldLayerProcess:
    """
    Coordinates the execution of the entire Gold Layer data processing phase.

    This involves fetching matching Gold datasets and running custom standard
    transformations and data quality verifications in parallel.
    """
    def __init__(self, spark: SparkSession, process_id, env="dev"):
        """
        Initializes the GoldLayerProcess and launches thread pool processing.

        Args:
            spark (SparkSession): The underlying Spark session executing the pipelines.
            process_id (int/str): Workflow identifier correlating all execution records.
            env (str, optional): The deployment environment context prefix. Defaults to "dev".
        """
        self.spark: SparkSession = spark
        self.env = env
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            self.gold_datasets = orch_process.get_dataset_master(
                process_id=process_id, dataset_type="GOLD"
            )

        if not self.gold_datasets:
            return

        with ThreadPoolExecutor(
            max_workers=min(5, len(self.gold_datasets))
        ) as executor:
            futures = [
                executor.submit(self._handle_gold_layer_process, gold_dataset)
                for gold_dataset in self.gold_datasets
            ]

            for future in futures:
                try:
                    future.result()  # Wait for each thread to finish
                except Exception as e:
                    raise

    def _handle_gold_layer_process(
        self, dataset_master: DatasetMaster.ctlDatasetMaster
    ):
        """
        Processes an individual dataset sequentially through Gold layer steps.

        Runs Transformation rules followed immediately by Data Quality Checks
        designed specifically for the finalized data structures.

        Args:
            dataset_master (DatasetMaster.ctlDatasetMaster): Metastore configuration representing this dataset's lifecycle tracking.
        """

        transformation_start_time = time()
        Transformation.PerformTransformation(
            spark=self.spark,
            process_id=dataset_master.process_id,
            dataset_id=dataset_master.dataset_id,
            transformation_table_name=dataset_master.transformation_table,
            table_location_type=dataset_master.table_location_type,
            env=self.env,
        )
        transformation_end_time = round((time() - transformation_start_time) / 3600, 6)

        DataQualityCheck_start_time = time()
        DataQualityCheckTransformation.DataQualityCheckTransformation(
            spark=self.spark,
            process_id=dataset_master.process_id,
            dataset_id=dataset_master.dataset_id,
            dqm_error_location=dataset_master.dqm_error_location,
            transformation_location=dataset_master.transformation_location,
            publish_location=dataset_master.publish_location,
            publish_partition_columns=dataset_master.publish_partition_columns,
            publish_table_name=dataset_master.publish_table,
            table_location_type=dataset_master.table_location_type,
            env=self.env,
        )
        DataQualityCheck_end_time = round(
            (time() - DataQualityCheck_start_time) / 3600, 6
        )
