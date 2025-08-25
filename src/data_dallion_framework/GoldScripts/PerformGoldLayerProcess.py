from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models import DatasetMaster
from concurrent.futures import ThreadPoolExecutor

from time import time
from pyspark.sql import SparkSession
# from nextgenframework.Common import TransformationDDL, PublishDDL

from data_dallion_framework.GoldScripts.TransformationScripts import PerformTransformation, DataQualityCheckTransformation, Transformation


class GoldLayerProcess:
    def _handle_gold_layer_process(self, dataset_master: DatasetMaster.ctlDatasetMaster):

        # TransformationDDL_start_time = time()
        # TransformationDDL.TransformationDDL(
        #     process_id=dataset_master.process_id,
        #     transformation_table=dataset_master.transformation_table,
        #     env="dev",
        #     spark=self.spark,
        #     dataset_id=dataset_master.dataset_id,
        # ).execute_ddl(dataset_type="L2")
        # TransformationDDL_end_time = round(
        #     (time() - TransformationDDL_start_time) / 3600, 6
        # )

        transformation_start_time = time()
        Transformation.PerformTransformation(
            spark=self.spark,
            process_id=dataset_master.process_id,
            dataset_id=dataset_master.dataset_id,
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
        )
        DataQualityCheck_end_time = round(
            (time() - DataQualityCheck_start_time) / 3600, 6
        )

        # PublishDDL_start_time = time()
        # PublishDDL.PublishDDL(
        #     process_id=dataset_master.process_id,
        #     publish_table=dataset_master.publish_table,
        #     env="dev",
        #     spark=self.spark,
        #     dataset_id=dataset_master.dataset_id,
        # ).execute_ddl(dataset_type="L2")
        # PublishDDLL_end_time = round((time() - PublishDDL_start_time) / 3600, 6)

    def __init__(self, spark: SparkSession, process_id):
        self.spark: SparkSession = spark
        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            self.gold_datasets = orch_process.get_dataset_master(
                process_id=process_id, dataset_type="GOLD"
            )

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
