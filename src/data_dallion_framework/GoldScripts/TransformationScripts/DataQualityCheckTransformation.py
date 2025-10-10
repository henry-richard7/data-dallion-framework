# optimized_dqm_check.py
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, length
from ast import literal_eval
from data_dallion_framework.Common import (
    OrchestrationProcess,
    RegexDateFormats,
    SchemaCaster,
)
from data_dallion_framework.Common.Models import Logs, DqmMaster


class DataQualityCheckTransformation:
    def __init__(
        self,
        spark: SparkSession,
        process_id,
        dataset_id,
        transformation_location,
        dqm_error_location,
        publish_location,
        publish_partition_columns,
        publish_table_name,
        table_location_type,
        env="dev",
    ):
        self.spark = spark
        self.process_id = process_id
        self.dataset_id = dataset_id
        self.transformation_location = transformation_location
        self.dqm_error_location = dqm_error_location
        self.publish_location = publish_location
        self.publish_partition_columns = publish_partition_columns
        self.publish_table_name = publish_table_name
        self.table_location_type = table_location_type
        self.env = env

        with OrchestrationProcess.OrchestrationProcess() as orch:
            self.dqm_unprocessed_files = orch.get_transformation_dqm_unprocessed_files(
                process_id, dataset_id
            )
            self.dqm_masters = orch.get_dqm_detail(process_id, dataset_id)
            self.column_metadata = orch.get_ctl_column_metadata(dataset_id=dataset_id)

        self.qc_type_to_function = {
            "Null": self.null_check,
            "Length": self.length_check,
            "Length-Range": self.length_range_check,
            "Date": self.date_check,
            "Integer": self.integer_check,
            "Decimal": self.decimal_check,
            "Regex": self.regex_check,
            "Domain": self.domain_check,
            "Custom": self.custom_check,
            "Unique": self.unique_check,
            "Blank": self.blank_check,
        }

        if self.dqm_masters:
            self.start_dqm_check()
        else:
            self.handle_no_dqm_masters()

    def apply_qc_filter(self, df, qc_filter):
        return df.filter(" AND ".join(qc_filter.split(","))) if qc_filter else df

    def calculate_metrics(self, input_df, passed_df, total):
        failed = total - passed_df.count()
        return failed, (failed / total) * 100 if total else 0

    def write_failed(
        self, input_df: DataFrame, passed_df: DataFrame, column, check_type, path
    ):
        print("Writing Failed records.")
        if check_type != "UNIQUE":
            failed = (
                input_df.subtract(passed_df)
                .withColumn("dqm_check_type", lit(check_type))
                .withColumn("failed_column_name", lit(column))
                .withColumn("fail_value", col(column))
                .select(
                    "dqm_check_type", "failed_column_name", "fail_value", "batch_id"
                )
            )
        else:
            columns = column.split(",")
            failed = (
                input_df.subtract(passed_df)
                .withColumn("dqm_check_type", lit(check_type))
                .withColumn("failed_column_name", lit(",".join(columns)))
            )

            for col_name in columns:
                failed = failed.withColumn(f"fail_value_{col_name}", col(col_name))
            failed = failed.select(
                "dqm_check_type",
                "failed_column_name",
                *[f"fail_value_{c}" for c in columns],
                "batch_id",
            )

        failed.write.format("delta").mode("append").partitionBy("batch_id").save(path)

    def _prepare_result(self, df, fail_count, fail_pct, threshold):
        return {
            "df": None if fail_pct >= threshold else df,
            "error_count": fail_count,
            "error_percentage": fail_pct,
            "success": fail_pct < threshold,
        }

    def null_check(self, df, **kwargs):
        col_name = kwargs["column_name"]
        df_valid = df.filter(col(col_name).isNotNull())
        return self._run_check(df, df_valid, col_name, "NULL", **kwargs)

    def length_check(self, df, **kwargs):
        col_name, param = kwargs["column_name"], kwargs["qc_param"]
        op = "".join([c for c in param if not c.isdigit()])
        val = int("".join([c for c in param if c.isdigit()]))
        ops = {">=": "ge", "<=": "le", ">": "gt", "<": "lt", "=": "eq", "!=": "ne"}
        df_valid = df.filter(getattr(length(col(col_name)), ops[op])(val))
        return self._run_check(df, df_valid, col_name, "LENGTH", **kwargs)

    def length_range_check(self, df, **kwargs):
        col_name = kwargs["column_name"]
        r = literal_eval(kwargs["qc_param"])
        df_valid = df.filter(length(col(col_name)).between(min(r), max(r)))
        return self._run_check(df, df_valid, col_name, "LENGTH-RANGE", **kwargs)

    def date_check(self, df, **kwargs):
        regex = RegexDateFormats.get_date_regex(qc_param=kwargs["qc_param"])
        df_valid = df.filter(col(kwargs["column_name"]).rlike(regex))
        return self._run_check(df, df_valid, kwargs["column_name"], "DATE", **kwargs)

    def integer_check(self, df, **kwargs):
        df_valid = df.filter(col(kwargs["column_name"]).rlike(r"^-?\d+$"))
        return self._run_check(df, df_valid, kwargs["column_name"], "INTEGER", **kwargs)

    def decimal_check(self, df, **kwargs):
        df_valid = df.filter(
            col(kwargs["column_name"]).rlike(r"^-?(\d+\.\d+|\d+|\.\d+)$")
        )
        return self._run_check(df, df_valid, kwargs["column_name"], "DECIMAL", **kwargs)

    def regex_check(self, df, **kwargs):
        df_valid = df.filter(col(kwargs["column_name"]).rlike(kwargs["qc_param"]))
        return self._run_check(df, df_valid, kwargs["column_name"], "REGEX", **kwargs)

    def domain_check(self, df, **kwargs):
        values = kwargs["qc_param"].split(",")
        df_valid = df.filter(col(kwargs["column_name"]).isin(values))
        return self._run_check(df, df_valid, kwargs["column_name"], "DOMAIN", **kwargs)

    def custom_check(self, df, **kwargs):
        self.spark.sql(
            f"CREATE OR REPLACE TEMP VIEW table_{kwargs['qc_id']} AS SELECT * FROM df"
        )
        df_valid = self.spark.sql(kwargs["qc_param"])
        return self._run_check(df, df_valid, "", "CUSTOM", **kwargs)

    def unique_check(self, df, **kwargs):
        cols = kwargs["column_name"].split(",")
        df_valid = df.dropDuplicates(cols)
        return self._run_check(df, df_valid, kwargs["column_name"], "UNIQUE", **kwargs)

    def blank_check(self, df, **kwargs):
        df = self.apply_qc_filter(df, kwargs.get("qc_filter"))
        return {
            "df": None if df.isEmpty() else df,
            "error_count": None if df.isEmpty() else 0,
            "error_percentage": 100 if df.isEmpty() else 0,
            "success": False,
        }

    def _run_check(self, df, df_valid, column, check_type, **kwargs):
        df_valid = self.apply_qc_filter(df_valid, kwargs.get("qc_filter"))
        fail_count, fail_pct = self.calculate_metrics(
            df, df_valid, kwargs["total_count"]
        )
        if fail_count:
            self.write_failed(df, df_valid, column, check_type, kwargs["failure_path"])
        return self._prepare_result(
            df_valid, fail_count, fail_pct, kwargs["error_threshold"]
        )

    def log_result(
        self,
        resp,
        dqm: DqmMaster.ctlDqmMasterDtl,
        log: Logs.logDqmDtl,
        batch_id,
        start_time,
    ):
        with OrchestrationProcess.OrchestrationProcess() as orch:
            status = (
                "SUCCEEDED"
                if resp["success"]
                else ("FAILED" if dqm.criticality == "C" else "SUCCEEDED")
            )
            orch.insert_log_dqm(
                log_dqm=Logs.logDqmDtl(
                    process_id=self.process_id,
                    dataset_id=self.dataset_id,
                    batch_id=batch_id,
                    source_file=log.source_file,
                    column_name=dqm.column_name,
                    qc_type=dqm.qc_type,
                    qc_param=dqm.qc_param,
                    qc_filter=dqm.qc_filter,
                    criticality=dqm.criticality,
                    criticality_threshold_pct=dqm.criticality_threshold_pct,
                    error_count=resp["error_count"],
                    error_pct=resp["error_percentage"],
                    status=status,
                    dqm_start_time=start_time,
                    dqm_end_time=datetime.now(),
                )
            )

    def start_dqm_check(self):
        for log in self.dqm_unprocessed_files:
            start_time = datetime.now()
            batch_id = log.batch_id
            df = self.spark.read.format("delta").load(self.transformation_location)

            original_df = df

            for dqm in self.dqm_masters:
                func = self.qc_type_to_function.get(dqm.qc_type)
                if not func:
                    continue
                total = df.count()
                resp = func(
                    df,
                    column_name=dqm.column_name,
                    total_count=total,
                    error_threshold=dqm.criticality_threshold_pct,
                    qc_param=dqm.qc_param,
                    qc_filter=dqm.qc_filter,
                    qc_id=dqm.qc_id,
                    dataset_id=self.dataset_id,
                    failure_path=self.dqm_error_location,
                    batch_id=batch_id,
                )
                self.log_result(resp, dqm, log, batch_id, start_time)
                if not resp["success"] and dqm.criticality == "C":
                    raise Exception(
                        f"Critical DQM check failed for {dqm.column_name} [{dqm.qc_type}]"
                    )
                if resp["success"]:
                    df = resp["df"]

            self._write_data(df if dqm.criticality == "C" else original_df, batch_id)

    def _write_data(self, df: DataFrame, batch_id):
        df = SchemaCaster.SchemaCaster(
            df=df, schema_config=self.column_metadata
        ).perform_casting()

        df = df.withColumn("batch_id", lit(batch_id)).filter(col("sys_del_flg") == "N")

        if self.table_location_type.lower() == "external":
            df.write.format("delta").mode("overwrite").partitionBy(
                *[c.strip() for c in self.publish_partition_columns.split(",")]
            ).save(f"{self.env}.{self.publish_location}")
        else:
            df.write.format("delta").mode("overwrite").partitionBy(
                *[c.strip() for c in self.publish_partition_columns.split(",")]
            ).saveAsTable(f"{self.env}{self.publish_table_name}")

    def handle_no_dqm_masters(self):
        if not self.dqm_unprocessed_files:
            raise Exception(f"DQM already processed for Dataset ID {self.dataset_id}.")
        for log in self.dqm_unprocessed_files:
            start_time = datetime.now()
            batch_id = log.batch_id
            df = self.spark.read.format("delta").load(self.transformation_location)
            df = SchemaCaster.SchemaCaster(
                df=df, schema_config=self.column_metadata
            ).perform_casting()
            self._write_data(df, batch_id)
            with OrchestrationProcess.OrchestrationProcess() as orch:
                orch.insert_log_dqm(
                    log_dqm=Logs.logDqmDtl(
                        process_id=self.process_id,
                        dataset_id=self.dataset_id,
                        batch_id=batch_id,
                        source_file=log.source_file,
                        status="SUCCEEDED",
                        dqm_start_time=start_time,
                        dqm_end_time=datetime.now(),
                    )
                )
