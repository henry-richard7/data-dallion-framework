from pyspark.sql.functions import (
    sha2,
    concat_ws,
    current_date,
    lit,
    to_date,
    current_timestamp,
    col,
    max as spark_max,
    expr,
)
from pyspark.sql import SparkSession, DataFrame, functions as F
from json import loads as json_loads
from datetime import datetime
from functools import reduce
import operator
from delta.tables import DeltaTable
from data_dallion_framework.Common import OrchestrationProcess, Constants
from data_dallion_framework.Common.Models import Logs


class PerformTransformation:
    """
    Conducts Gold layer transformations such as JOINS, AGGREGATES, and UNIONS.

    Reads metadata defined dependencies tying multiple upstream sources together
    into cohesive modeled records spanning multiple files dynamically.
    """
    def __init__(
        self,
        spark: SparkSession,
        process_id: int,
        dataset_id: int,
        transformation_table_name: str,
        table_location_type: str,
        env: str = "dev",
    ):
        """
        Initializes the Gold Transformation sequence and triggers rule interpretation.

        Args:
            spark (SparkSession): Main driver Spark runtime context reference.
            process_id (int): ID tagging current batch across all system executions.
            dataset_id (int): Internal target configuration dataset matching logic.
            transformation_table_name (str): Standardize table identifier representing gold outputs.
            table_location_type (str): Flag denoting the table access type (EXTERNAL/MANAGED).
            env (str, optional): Target environment scope parameter prefix. Defaults to "dev".
        """
        self.spark = spark
        self.table_location_type = table_location_type
        self.env = env
        self.log_buffer = []

        with OrchestrationProcess.OrchestrationProcess() as orch_process:
            transformation_depedencies = (
                orch_process.get_transformation_dependency_master(
                    process_id=process_id, dataset_id=dataset_id
                )
            )

            primary_keys = transformation_depedencies[0].primary_keys.split(",")
            self.primary_keys = primary_keys
            self.primary_key_conditions = " AND ".join(
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

            target_table_location = target_table_details.transformation_location
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

                source_details.append(
                    {
                        "source_table_location": dependent_dataset_details.staging_location,
                        "source_table_name": dependent_dataset_details.staging_table.split(".")[-1],
                        "full_source_table_name": dependent_dataset_details.staging_table,
                        "transformation_type": transformation_depedency.transformation_type,
                        "join_how": transformation_depedency.join_how,
                        "left_table_columns": transformation_depedency.left_table_columns,
                        "right_table_columns": transformation_depedency.right_table_columns,
                        "columns": [x.column_name for x in orch_process.get_ctl_column_metadata(dataset_id=transformation_depedency.dependent_dataset_id)],
                        "group_by_columns": transformation_depedency.group_by_columns,
                        "measure_columns": transformation_depedency.measure_columns,
                        "extra_values": transformation_depedency.extra_values,
                    }
                )

            if len(unprocessed_transformation_files) != 0:
                # Pre-fetch max batch ID once if possible, or handle it inside read
                for dqm_log in unprocessed_transformation_files:
                    start_time = datetime.now()
                    batch_id = dqm_log.batch_id
                    try:
                        # Optimization: Pass batch_id directly to filter rather than scanning for max
                        df = self._read_and_filter_batch(
                            source_table_location=source_details[0]["source_table_location"],
                            full_source_table_name=source_details[0]["full_source_table_name"],
                            batch_id=batch_id
                        )
                        df = df.drop("batch_id").alias(source_details[0]["source_table_name"])

                        if source_details[0]["transformation_type"] == "SINGLE":
                            if source_details[0]["extra_values"]:
                                extra_vals = json_loads(source_details[0]["extra_values"])
                                for col_name, expression in extra_vals.items():
                                    df = df.withColumn(col_name, expr(expression))
                        else:
                            for source_detail in source_details[1:]:
                                df_ = self._read_and_filter_batch(
                                    source_table_location=source_detail["source_table_location"],
                                    full_source_table_name=source_detail["full_source_table_name"],
                                    batch_id=batch_id
                                )
                                df_ = df_.drop("batch_id").alias(source_detail["source_table_name"])
                                if source_detail["transformation_type"] == "UNION":
                                    df = df.union(df_)
                                elif source_detail["transformation_type"] == "JOIN":
                                    l_cols, r_cols = source_detail["left_table_columns"].split(","), source_detail["right_table_columns"].split(",")
                                    cond = reduce(operator.and_, [df[l] == df_[r] for l, r in zip(l_cols, r_cols)])
                                    df = df.join(df_, on=cond, how=source_detail["join_how"].lower())
                                    if source_detail == [j for j in source_details if j["transformation_type"] == "JOIN"][-1]:
                                        df = df.select(self._get_unique_columns(source_details))
                                elif source_detail["transformation_type"] == "AGGREGATE":
                                    measures = json_loads(source_detail["measure_columns"])
                                    agg_exprs = [getattr(F, f.lower())(col(c)).alias(f"{c}_{f.lower()}") for c, f in measures.items()]
                                    df = df.groupBy(source_detail["group_by_columns"].split(",")).agg(*agg_exprs)

                        final_df = df.select(*target_table_column_names)
                        final_df = final_df.withColumn("batch_id", lit(batch_id)) \
                                           .withColumn("data_date", current_date()) \
                                           .withColumn("eff_strt_dt", current_date()) \
                                           .withColumn("eff_end_dt", to_date(lit(Constants.HIGH_DATE_STR), "yyyy-MM-dd")) \
                                           .withColumn("sys_del_flg", lit("N")) \
                                           .withColumn("sys_created_ts", current_timestamp()) \
                                           .withColumn("sys_modified_ts", current_timestamp()) \
                                           .withColumn("sys_checksum", sha2(concat_ws("||", *target_table_column_names), 256))

                        self._save_data(final_df, transformation_table_name, target_table_location, target_table_details)
                        self.buffer_log(dqm_log, batch_id, process_id, dataset_id, start_time, Constants.STATUS_SUCCEEDED)

                    except Exception as e:
                        self.buffer_log(dqm_log, batch_id, process_id, dataset_id, start_time, Constants.STATUS_FAILED, str(e))
                        self.flush_logs()
                        raise
                self.flush_logs()

    def _save_data(self, df, table_name, location, details):
        if self.table_location_type.lower() == Constants.TABLE_TYPE_EXTERNAL:
            if DeltaTable.isDeltaTable(self.spark, location):
                target = DeltaTable.forPath(self.spark, location)
                # (Skip merge logic refactoring per user request, using existing dual-merge)
                target.alias("TARGET").merge(df.alias("staging"), f"TARGET.eff_end_dt = '{Constants.HIGH_DATE_STR}' AND {self.primary_key_conditions}") \
                      .whenMatchedUpdate(condition="TARGET.sys_checksum != staging.sys_checksum", set={"eff_end_dt": col("staging.eff_strt_dt"), "sys_del_flg": lit("Y")}) \
                      .whenNotMatchedInsertAll().execute()
                target.alias("TARGET").merge(df.alias("staging"), f"TARGET.eff_end_dt = '{Constants.HIGH_DATE_STR}' AND {self.primary_key_conditions}") \
                      .whenNotMatchedInsertAll().execute()
            else:
                df.write.format("delta").mode("append").partitionBy(details.transformation_partition_columns.split(",")).save(location)
        else:
            full_name = f"{self.env}.{table_name}"
            if self.spark.catalog.tableExists(full_name):
                target = DeltaTable.forName(self.spark, full_name)
                target.alias("TARGET").merge(df.alias("staging"), f"TARGET.eff_end_dt = '{Constants.HIGH_DATE_STR}' AND {self.primary_key_conditions}") \
                      .whenMatchedUpdate(condition="TARGET.sys_checksum != staging.sys_checksum", set={"eff_end_dt": col("staging.eff_strt_dt"), "sys_del_flg": lit("Y")}) \
                      .whenNotMatchedInsertAll().execute()
                target.alias("TARGET").merge(df.alias("staging"), f"TARGET.eff_end_dt = '{Constants.HIGH_DATE_STR}' AND {self.primary_key_conditions}") \
                      .whenNotMatchedInsertAll().execute()
            else:
                df.write.format("delta").mode("append").partitionBy(details.transformation_partition_columns.split(",")).saveAsTable(full_name)

    def _read_and_filter_batch(self, source_table_location, full_source_table_name, batch_id):
        # Optimization: Use the known batch_id instead of scanning for max()
        if self.table_location_type.lower() == Constants.TABLE_TYPE_EXTERNAL:
            return self.spark.read.format("delta").load(source_table_location).filter(col("batch_id") == batch_id)
        else:
            return self.spark.table(f"{self.env}.{full_source_table_name}").filter(col("batch_id") == batch_id)

    def buffer_log(self, dqm_log, batch_id, process_id, dataset_id, start_time, status, error=None):
        self.log_buffer.append(Logs.logTransformationDtl(
            batch_id=batch_id, process_id=process_id, dataset_id=dataset_id,
            data_date=datetime.now(), source_file=dqm_log.source_file if dqm_log else None,
            status=status, transformation_start_time=start_time,
            transformation_end_time=datetime.now(), exception_details=error
        ))

    def flush_logs(self):
        if not self.log_buffer: return
        with OrchestrationProcess.OrchestrationProcess() as orch:
            for entry in self.log_buffer: orch.insert_log_transformation(entry)
        self.log_buffer = []

    def _get_unique_columns(self, source_details):
        seen, cols = set(), []
        for info in source_details:
            if info["transformation_type"] != "AGGREGATE":
                for c in info["columns"]:
                    if c not in seen:
                        seen.add(c)
                        cols.append(f"{info['source_table_name']}.{c}")
        return cols
