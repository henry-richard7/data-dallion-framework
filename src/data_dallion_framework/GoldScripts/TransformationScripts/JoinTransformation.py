from pyspark.sql.functions import (
    sha2,
    concat_ws,
    current_date,
    lit,
    to_date,
    current_timestamp,
    col,
    asc,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame

from json import loads as json_loads
from datetime import datetime

from delta.tables import DeltaTable
from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models import TransformationDependencyMaster, Logs


class JoinTransformation:
    def _get_unique_columns(self, source_details):
        seen = set()
        columns_to_select = []
        for join_info in source_details:
            for column in join_info["columns"]:
                if column not in seen:
                    seen.add(column)
                    columns_to_select.append(
                        f"{join_info['source_table_name']}.{column}"
                    )
        return columns_to_select

    def _read_and_filter_latest_batch(self, source_table_location, table_name):
        staging_df = self.spark.read.format("delta").load(source_table_location)
        # staging_df = staging_df.dropDuplicates(self.primary_keys)
        window_spec = Window.partitionBy()

        return (
            staging_df.withColumn(
                "max_batch_id", spark_max("batch_id").over(window_spec)
            )
            .where(col("batch_id") == col("max_batch_id"))
            .drop("max_batch_id")
            .drop("batch_id")
            .alias(table_name)
        )
        # return staging_df.alias(table_name)

    def __init__(
        self,
        spark: SparkSession,
        process_id,
        dataset_id,
        transformation_depedencies: list[TransformationDependencyMaster.ctlTransformationDependencyMaster],
    ):
        self.spark = spark
        primary_keys = transformation_depedencies[0].primary_keys.split(",")
        self.primary_keys = primary_keys
        primary_key_conditions = " AND ".join(
            [f"TARGET.{key} = staging.{key}" for key in primary_keys]
        )

        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            
            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=process_id,
                    dataset_id=transformation_depedencies[0].dependent_dataset_id  ,
                )
            )

            target_table_details = orch_process.get_dataset_master(
                process_id=process_id, dataset_id=dataset_id,dataset_type="GOLD"
            )
            target_table_details = target_table_details

            target_table_location = target_table_details.transformation_location
            target_table_partition_columns = (
                target_table_details.transformation_partition_columns
            )

            target_table_column_metadata_details = orch_process.get_ctl_column_metadata(
                dataset_id=dataset_id
            )
            target_table_column_names = [
                x.column_name for x in target_table_column_metadata_details
            ]

            source_details = list()

            for transformation_depedency in transformation_depedencies:
                dependent_dataset_details = orch_process.get_dataset_master(
                    process_id=process_id,
                    dataset_id=transformation_depedency.dependent_dataset_id  ,
                    dataset_type="BRONZE"
                )

                dependent_dataset_column_metadata = (
                    orch_process.get_ctl_column_metadata(
                        dataset_id=transformation_depedency.dependent_dataset_id  ,
                    )
                )

                dependent_columns = [
                    x.column_name for x in dependent_dataset_column_metadata
                ]

                source_details.append(
                    {
                        "source_table_location": dependent_dataset_details.staging_location,
                        "source_table_name": dependent_dataset_details.staging_table.split(
                            "."
                        )[
                            -1
                        ],
                        "how": transformation_depedency.join_how,
                        "left_table_columns": transformation_depedency.left_table_columns,
                        "right_table_columns": transformation_depedency.right_table_columns,
                        "columns": dependent_columns,
                    }
                )
        if len(unprocessed_transformation_files) != 0:
            for dqm_log in unprocessed_transformation_files:
                try:
                    start_time = datetime.now()
                    batch_id = dqm_log.batch_id

                    source_dfs = list()

                    for join_info in source_details:
                        source_table_location = join_info["source_table_location"]
                        staging_df = self._read_and_filter_latest_batch(
                            source_table_location=source_table_location,
                            table_name=join_info["source_table_name"],
                        )

                        source_dfs.append(staging_df)

                    result_df = source_dfs[0]

                    for i in range(1, len(source_dfs)):
                        left_columns = source_details[i]["left_table_columns"].split(
                            ","
                        )
                        right_columns = source_details[i]["right_table_columns"].split(
                            ","
                        )

                        right_df = source_dfs[i]
                        join_condition = [
                            result_df[left_column] == right_df[right_column]
                            for left_column, right_column in zip(
                                left_columns, right_columns
                            )
                        ]

                        result_df = result_df.join(
                            right_df, join_condition, source_details[i]["how"]
                        )

                    columns_to_select = self._get_unique_columns(source_details)
                    final_result_df:DataFrame = (
                        result_df.select(columns_to_select)
                        .withColumn("batch_id", lit(batch_id))
                        .withColumn("data_date", current_date())
                        .withColumn("eff_strt_dt", current_date())
                        .withColumn(
                            "eff_end_dt", to_date(lit("9999-12-31"), "yyyy-MM-dd")
                        )
                        .withColumn("sys_del_flg", lit("N"))
                        .withColumn("sys_created_ts", current_timestamp())
                        .withColumn("sys_modified_ts", current_timestamp())
                        .withColumn(
                            "sys_checksum",
                            sha2(concat_ws("||", *target_table_column_names), 256),
                        )
                    )
                    
                    if DeltaTable.isDeltaTable(sparkSession=spark, identifier=target_table_location):
                        target_table = DeltaTable.forPath(spark, target_table_location)
                        target_table.alias("TARGET").merge(
                            final_result_df.alias("staging"),
                            f"TARGET.eff_end_dt = '9999-12-31' AND {primary_key_conditions}",
                        ).whenMatchedUpdate(
                            condition="TARGET.sys_checksum != staging.sys_checksum",
                            set={
                                "eff_end_dt": col("staging.eff_strt_dt"),
                                "sys_del_flg": lit("Y"),
                            },
                        ).whenNotMatchedInsertAll().execute()

                        target_table.alias("TARGET").merge(
                            final_result_df.alias("staging"),
                            f"TARGET.eff_end_dt = '9999-12-31' AND {primary_key_conditions}",
                        ).whenNotMatchedInsertAll().execute()
                    else:
                        final_result_df.write.format("delta").mode("append").partitionBy(*[c.strip() for c in target_table_details.transformation_partition_columns.split(",")]).save(target_table_location)

                    with OrchestrationProcess.OrchestrationProcess() as orch_process:
                        orch_process.insert_log_transformation(
                            log_transformation=Logs.logTransformationDtl(
                            batch_id=batch_id,
                            process_id=process_id,
                            dataset_id=dataset_id,
                            data_date=datetime.now(),
                            source_file=dqm_log.source_file,
                            status="SUCCEEDED",
                            transformation_start_time=start_time,
                            transformation_end_time=datetime.now(),
                            exception_details=None,
                        )
                            )
                except Exception as e:
                    with OrchestrationProcess.OrchestrationProcess() as orch_process:
                        orch_process.insert_log_transformation(
                            log_transformation=Logs.logTransformationDtl(
                            batch_id=batch_id,
                            process_id=process_id,
                            dataset_id=dataset_id,
                            data_date=datetime.now(),
                            source_file=dqm_log.source_file,
                            status="FAILED",
                            transformation_start_time=start_time,
                            transformation_end_time=datetime.now(),
                            exception_details=e,
                            )
                        )
                    raise

        else:
            with OrchestrationProcess.OrchestrationProcess() as orch_process:
                orch_process.insert_log_transformation(
                    log_transformation=Logs.logTransformationDtl(
                    batch_id=None,
                    process_id=process_id,
                    dataset_id=dataset_id,
                    data_date=datetime.now(),
                    source_file=None,
                    status="FAILED",
                    transformation_start_time=start_time,
                    transformation_end_time=datetime.now(),
                    exception_details=f"Transformation is already completed for dataset id: {dataset_id}.",
                )
                    )
            raise Exception(
                f"Transformation is already completed for dataset id: {dataset_id}."
            )
