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
from pyspark.sql import SparkSession

from datetime import datetime

from delta.tables import DeltaTable
from data_dallion_framework.Common import OrchestrationProcess
from data_dallion_framework.Common.Models import TransformationDependencyMaster, Logs


class SingleTransformation:
    def __init__(
        self,
        spark: SparkSession,
        process_id,
        dataset_id,
        transformation_depedencies: list[TransformationDependencyMaster.ctlTransformationDependencyMaster],
    ):
        transformation_dependency = transformation_depedencies[0]
        dependent_dataset_id = transformation_dependency.dependent_dataset_id

        primary_keys = transformation_dependency.primary_keys.split(",")
        primary_key_conditions = " AND ".join(
            [f"TARGET.{key} = staging.{key}" for key in primary_keys]
        )

        with OrchestrationProcess.OrchestrationProcess() as orch_process:

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=process_id,
                    dataset_id=dependent_dataset_id,
                )
            )

            source_table_details = orch_process.get_dataset_master(
                process_id=process_id, dataset_id=dependent_dataset_id, dataset_type="BRONZE"
            )

            target_table_details = orch_process.get_dataset_master(
                process_id=process_id, dataset_id=dataset_id, dataset_type="GOLD"
            )

            columns = orch_process.get_ctl_column_metadata(dataset_id=dataset_id)
            columns = [x.column_name for x in columns]

        source_table_location = source_table_details.staging_location
        target_table_location = target_table_details.transformation_location

        if len(unprocessed_transformation_files) != 0:
            for dqm_log in unprocessed_transformation_files:
                start_time = datetime.now()

                try:
                    batch_id = dqm_log.batch_id
                    window_spec = Window.partitionBy()

                    staging_df = spark.read.format("delta").load(source_table_location)
                    staging_df = (
                        staging_df.withColumn(
                            "max_batch_id", spark_max("batch_id").over(window_spec)
                        )
                        .where(col("batch_id") == col("max_batch_id"))
                        .drop("max_batch_id")
                    )

                    staging_df = (
                        staging_df.withColumn("data_date", current_date())
                        .withColumn("batch_id", lit(batch_id))
                        .withColumn("eff_strt_dt", current_date())
                        .withColumn(
                            "eff_end_dt", to_date(lit("9999-12-31"), "yyyy-MM-dd")
                        )
                        .withColumn("sys_del_flg", lit("N"))
                        .withColumn("sys_created_ts", current_timestamp())
                        .withColumn("sys_modified_ts", current_timestamp())
                        .withColumn(
                            "sys_checksum", sha2(concat_ws("||", *columns), 256)
                        )
                    )

                    if DeltaTable.isDeltaTable(sparkSession=spark, identifier=target_table_location):
                    
                        target_table = DeltaTable.forPath(spark, target_table_location)

                        target_table.alias("TARGET").merge(
                            staging_df.alias("staging"),
                            f"TARGET.eff_end_dt = '9999-12-31' AND {primary_key_conditions}",
                        ).whenMatchedUpdate(
                            condition="TARGET.sys_checksum != staging.sys_checksum",
                            set={
                                "eff_end_dt": col("staging.eff_strt_dt"),
                                "sys_del_flg": lit("Y"),
                            },
                        ).whenNotMatchedInsertAll().execute()

                        target_table.alias("TARGET").merge(
                            staging_df.alias("staging"),
                            f"TARGET.eff_end_dt = '9999-12-31' AND {primary_key_conditions}",
                        ).whenNotMatchedInsertAll().execute()
                    
                    else:
                        staging_df.write.format("delta").mode("append").partitionBy(*[c.strip() for c in target_table_details.transformation_partition_columns.split(",")]).save(target_table_location)

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
                    transformation_start_time=datetime.now(),
                    transformation_end_time=datetime.now(),
                    exception_details=f"Transformation is already completed for dataset id: {dataset_id}.",
                )
                    )
            raise Exception(
                f"Transformation is already completed for dataset id: {dataset_id}."
            )
