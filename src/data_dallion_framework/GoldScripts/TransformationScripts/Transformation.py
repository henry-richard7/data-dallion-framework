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
from pyspark.sql import functions as F

from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame

from json import loads as json_loads
from datetime import datetime

from functools import reduce
import operator

from delta.tables import DeltaTable
from data_dallion_framework.Common import OrchestrationProcess

from data_dallion_framework.Common.Models import Logs
from data_dallion_framework.Common import OrchestrationProcess


class PerformTransformation:
    def __init__(
        self,
        spark: SparkSession,
        process_id: int,
        dataset_id: int,
        transformation_table_name: str,
        table_location_type: str,
        env: str = "dev",
    ):
        self.spark = spark

        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    process_id=process_id, dataset_id=dataset_id
                )
            )

            primary_keys = transformation_depedencies[0].primary_keys.split(",")
            self.primary_keys = primary_keys
            primary_key_conditions = " AND ".join(
                [f"TARGET.{key} = staging.{key}" for key in primary_keys]
            )

            unprocessed_transformation_files = (
                orch_process.get_unprocessed_transformation_files(
                    process_id=process_id,
                    dataset_id=transformation_depedencies[0].dependent_dataset_id,
                )
            )

            target_table_details = orch_process.get_dataset_master(
                process_id=process_id, dataset_id=dataset_id, dataset_type="GOLD"
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

            source_details: list[dict[str, str]] = list()

            for transformation_depedency in transformation_depedencies:
                dependent_dataset_details = orch_process.get_dataset_master(
                    process_id=process_id,
                    dataset_id=transformation_depedency.dependent_dataset_id,
                    dataset_type="BRONZE",
                )

                dependent_dataset_column_metadata = (
                    orch_process.get_ctl_column_metadata(
                        dataset_id=transformation_depedency.dependent_dataset_id,
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
                        "transformation_type": transformation_depedency.transformation_type,
                        "join_how": transformation_depedency.join_how,
                        "left_table_columns": transformation_depedency.left_table_columns,
                        "right_table_columns": transformation_depedency.right_table_columns,
                        "columns": dependent_columns,
                        "group_by_columns": transformation_depedency.group_by_columns,
                        "measure_columns": transformation_depedency.measure_columns,
                    }
                )

            joining_tables = [
                j for j in source_details if j["transformation_type"] == "JOIN"
            ]

            if len(unprocessed_transformation_files) != 0:
                for dqm_log in unprocessed_transformation_files:
                    try:
                        start_time = datetime.now()
                        batch_id = dqm_log.batch_id

                        df = self._read_and_filter_latest_batch(
                            source_table_location=source_details[0][
                                "source_table_location"
                            ],
                            table_name=source_details[0]["source_table_name"],
                        )

                        for source_detail in source_details[1:]:
                            if source_detail["transformation_type"] == "UNION":
                                df_ = self._read_and_filter_latest_batch(
                                    source_table_location=source_detail[
                                        "source_table_location"
                                    ],
                                    table_name=source_detail["source_table_name"],
                                )
                                df = df.union(df_)

                            elif source_detail["transformation_type"] == "JOIN":
                                df_ = self._read_and_filter_latest_batch(
                                    source_table_location=source_detail[
                                        "source_table_location"
                                    ],
                                    table_name=source_detail["source_table_name"],
                                )

                                left_cols = source_detail["left_table_columns"].split(
                                    ","
                                )
                                right_cols = source_detail["right_table_columns"].split(
                                    ","
                                )

                                join_conditions = [
                                    df[l] == df_[r]
                                    for l, r in zip(left_cols, right_cols)
                                ]
                                join_condition = (
                                    reduce(operator.and_, join_conditions)
                                    if len(join_conditions) > 1
                                    else join_conditions[0]
                                )

                                df = df.join(
                                    df_,
                                    on=join_condition,
                                    how=source_detail["join_how"].lower(),
                                )

                                if joining_tables[-1] == source_detail:
                                    columns_to_select = self._get_unique_columns(
                                        source_details
                                    )
                                    df = df.select(columns_to_select)

                            elif source_detail["transformation_type"] == "AGGREGATE":
                                if source_detail != source_details[-1]:
                                    raise Exception(
                                        "Aggregation must be performed after UNION or JOIN. Not before."
                                    )

                                group_cols = source_detail["group_by_columns"].split(
                                    ","
                                )
                                measures = json_loads(source_detail["measure_columns"])

                                agg_exprs = []

                                for col_, agg_func in measures.items():
                                    func = getattr(F, agg_func.lower(), None)
                                    if func is None:
                                        raise ValueError(
                                            f"Unsupported aggregation: {agg_func}"
                                        )
                                    agg_exprs.append(
                                        func(F.col(col_)).alias(
                                            f"{col_}_{agg_func.lower()}"
                                        )
                                    )

                                df = df.groupBy(group_cols).agg(*agg_exprs)

                            elif source_detail["transformation_type"] == "CUSTOM":
                                raise NotImplementedError()

                        final_result_df: DataFrame = df.select(
                            *target_table_column_names
                        )
                        final_result_df = (
                            final_result_df.withColumn("batch_id", lit(batch_id))
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

                        if table_location_type.lower() == "external":
                            if DeltaTable.isDeltaTable(
                                sparkSession=spark, identifier=target_table_location
                            ):
                                target_table = DeltaTable.forPath(
                                    spark, target_table_location
                                )
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
                                final_result_df.write.format("delta").mode(
                                    "append"
                                ).partitionBy(
                                    *[
                                        c.strip()
                                        for c in target_table_details.transformation_partition_columns.split(
                                            ","
                                        )
                                    ]
                                ).save(
                                    target_table_location
                                )
                        else:
                            if spark.catalog.tableExists(
                                f"{env}.{transformation_table_name}"
                            ):
                                target_table = DeltaTable.forName(
                                    spark, f"{env}.{transformation_table_name}"
                                )
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
                                final_result_df.write.format("delta").mode(
                                    "append"
                                ).partitionBy(
                                    *[
                                        c.strip()
                                        for c in target_table_details.transformation_partition_columns.split(
                                            ","
                                        )
                                    ]
                                ).saveAsTable(
                                    f"{env}.{transformation_table_name}"
                                )

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

    def _get_unique_columns(self, source_details):
        seen = set()
        columns_to_select = []
        for join_info in source_details:

            for column in join_info["columns"]:
                if join_info["transformation_type"] != "AGGREGATE":
                    if column not in seen:
                        seen.add(column)
                        columns_to_select.append(
                            f"{join_info['source_table_name']}.{column}"
                        )
        return columns_to_select

    def _read_and_filter_latest_batch(self, source_table_location, table_name):
        staging_df = self.spark.read.format("delta").load(source_table_location)
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
