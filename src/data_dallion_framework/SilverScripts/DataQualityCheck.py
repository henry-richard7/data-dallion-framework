# optimized_dqm_check.py
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, functions as F
from ast import literal_eval
from data_dallion_framework.Common import OrchestrationProcess, RegexDateFormats, Constants
from data_dallion_framework.Common.Models import Logs, DqmMaster


class DataQualityCheck:
    def __init__(
        self,
        spark: SparkSession,
        process_id: int,
        dataset_id: int,
        data_standardisation_location: str,
        dqm_error_location: str,
        staging_location: str,
        staging_partition_columns: str,
        staging_table_name: str,
        publish_location: str,
        publish_partition_columns: str,
        publish_table_name: str,
        table_location_type: str,
        env="dev",
    ):
        self.spark = spark
        self.process_id = process_id
        self.dataset_id = dataset_id
        self.data_standardisation_location = data_standardisation_location
        self.dqm_error_location = dqm_error_location
        self.staging_location = staging_location
        self.staging_partition_columns = staging_partition_columns
        self.publish_location = publish_location
        self.publish_partition_columns = publish_partition_columns
        self.table_location_type = table_location_type
        self.env = env
        self.staging_table_name = staging_table_name
        self.publish_table_name = publish_table_name
        self.log_buffer = []

        with OrchestrationProcess.OrchestrationProcess() as orch:
            self.dqm_unprocessed_files = orch.get_dqm_unprocessed_files(
                process_id, dataset_id
            )
            self.dqm_masters = orch.get_dqm_detail(process_id, dataset_id)

        if self.dqm_masters:
            self.start_dqm_check()
        else:
            self.handle_no_dqm_masters()

    def get_qc_condition(self, dqm: DqmMaster.ctlDqmMasterDtl):
        """Map DQM master rule to a Spark SQL condition string."""
        tp = dqm.qc_type
        col_name = dqm.column_name
        param = dqm.qc_param

        if tp == "Null":
            return f"{col_name} IS NOT NULL"
        elif tp == "Length":
            op = "".join([c for c in param if not c.isdigit()])
            val = "".join([c for c in param if c.isdigit()])
            if not op: op = "="
            return f"length({col_name}) {op} {val}"
        elif tp == "Length-Range":
            try:
                r = literal_eval(param)
                return f"length({col_name}) BETWEEN {min(r)} AND {max(r)}"
            except:
                return "1=1"
        elif tp == "Date":
            regex = RegexDateFormats.get_date_regex(qc_param=param)
            regex = regex.replace("'", "''")
            return f"{col_name} RLIKE '{regex}'"
        elif tp == "Integer":
            return f"{col_name} RLIKE '^-?[0-9]+$'"
        elif tp == "Decimal":
            return f"{col_name} RLIKE '^-?([0-9]+\\.[0-9]+|[0-9]+|\\.[0-9]+)$'"
        elif tp == "Regex":
            param_esc = param.replace("'", "''")
            return f"{col_name} RLIKE '{param_esc}'"
        elif tp == "Domain":
            values = ",".join([f"'{v.strip()}'" for v in param.split(",")])
            return f"{col_name} IN ({values})"
        elif tp == "Blank":
            return f"trim({col_name}) != ''"
        elif tp == "Custom":
            return param
        return "1=1"

    def start_dqm_check(self):
        batch_ids = [log.batch_id for log in self.dqm_unprocessed_files]
        if not batch_ids:
            return

        try:
            # Optimization: Process all batches in one pass
            full_df = (
                self.spark.read.format("delta")
                .load(self.data_standardisation_location)
                .filter(F.col("batch_id").isin(batch_ids))
            )

            # Separate rules: Unique rules require actions (dropDuplicates), others are row-level
            one_pass_rules = [d for d in self.dqm_masters if d.qc_type != "Unique"]
            unique_rules = [d for d in self.dqm_masters if d.qc_type == "Unique"]
            
            # We handle batches individually for result logging and write-out
            for batch_log in self.dqm_unprocessed_files:
                start_time = datetime.now()
                bid = batch_log.batch_id
                df = full_df.filter(F.col("batch_id") == bid)
                
                # --- Phase 1: Row-level rules (Null, Length, Date, Regex, Custom, etc.) ---
                if one_pass_rules:
                    agg_exprs = [F.count("*").alias("total_rows")]
                    rule_meta = []
                    current_valid_expr = F.lit(True)
                    
                    for dqm in one_pass_rules:
                        cond_str = self.get_qc_condition(dqm)
                        if dqm.qc_filter:
                            filter_expr = " AND ".join(dqm.qc_filter.split(","))
                            rule_valid_expr = F.expr(f"({filter_expr}) AND ({cond_str})")
                        else:
                            rule_valid_expr = F.expr(cond_str)
                        
                        next_valid_expr = current_valid_expr & rule_valid_expr
                        rule_pass_col = f"passed_{dqm.qc_id}"
                        df = df.withColumn(rule_pass_col, F.when(next_valid_expr, 1).otherwise(0))
                        
                        agg_exprs.append(F.sum(rule_pass_col).alias(f"sum_{dqm.qc_id}"))
                        rule_meta.append((dqm, rule_pass_col))
                        current_valid_expr = next_valid_expr

                    df = df.withColumn("final_valid", F.when(current_valid_expr, 1).otherwise(0))
                    metrics = df.agg(*agg_exprs).collect()[0]
                    total = metrics["total_rows"]
                    
                    prev_sum = total
                    for dqm, rule_pass_col in rule_meta:
                        curr_sum = metrics[f"sum_{dqm.qc_id}"] or 0
                        fail_count = prev_sum - curr_sum
                        fail_pct = (fail_count / total * 100) if total > 0 else 0
                        success = fail_pct < (dqm.criticality_threshold_pct or 0)
                        
                        if fail_count > 0:
                            self._write_failed_optimized(df, bid, dqm, rule_pass_col)
                        
                        self.buffer_log(dqm, batch_log, bid, fail_count, fail_pct, success, start_time)
                        if not success and dqm.criticality == Constants.CRITICALITY_CRITICAL:
                            raise Exception(f"Critical DQM check failed: {dqm.column_name} in batch {bid}")
                        prev_sum = curr_sum
                    
                    df = df.filter(F.col("final_valid") == 1)
                
                # --- Phase 2: Unique rules (Sequential using dropDuplicates) ---
                for dqm in unique_rules:
                    before_count = df.count()
                    if before_count == 0:
                        self.buffer_log(dqm, batch_log, bid, 0, 0, True, start_time)
                        continue
                        
                    unique_cols = [c.strip() for c in dqm.column_name.split(",")]
                    df_unique = df.dropDuplicates(unique_cols)
                    after_count = df_unique.count()
                    
                    fail_count = before_count - after_count
                    fail_pct = (fail_count / before_count * 100)
                    success = fail_pct < (dqm.criticality_threshold_pct or 0)
                    
                    if fail_count > 0:
                        failed_rows = df.subtract(df_unique)
                        self._write_unique_failed(failed_rows, bid, dqm)
                    
                    self.buffer_log(dqm, batch_log, bid, fail_count, fail_pct, success, start_time)
                    if not success and dqm.criticality == Constants.CRITICALITY_CRITICAL:
                        raise Exception(f"Critical Uniqueness check failed: {dqm.column_name} in batch {bid}")
                    
                    df = df_unique

                # Handle case where NO rules were defined
                if not one_pass_rules and not unique_rules:
                    self.buffer_log(DqmMaster.ctlDqmMasterDtl(qc_id=0, column_name="N/A", qc_type="N/A", criticality="W"), batch_log, bid, 0, 0, True, start_time)

                # Final write of the now-validated dataframe
                self._write_data_optimized(df, bid)

        except Exception as e:
            # Buffer a generic failure if we don't have rule context here
            # But flush_logs() in finally will save whatever is in the buffer so far
            raise
        finally:
            self.flush_logs()

    def _write_unique_failed(self, failed_df: DataFrame, batch_id, dqm):
        # Uniqueness failure handling: log the duplicate values
        cols = [c.strip() for c in dqm.column_name.split(",")]
        # Cast key columns to string for unified "fail_value"
        failed = failed_df.withColumn("dqm_check_type", F.lit(dqm.qc_type)) \
                          .withColumn("failed_column_name", F.lit(dqm.column_name)) \
                          .withColumn("fail_value", F.concat_ws(",", *[F.col(c) for c in cols])) \
                          .select("dqm_check_type", "failed_column_name", "fail_value", "batch_id")
        
        failed.write.format("delta").mode("append").partitionBy("batch_id").save(self.dqm_error_location)

    def _write_failed_optimized(self, df: DataFrame, batch_id, dqm, rule_pass_col):
        # Rows that FAILED this specific rule (were valid before but invalid now)
        # Note: In our progressive logic, fail_count = sum_previous - sum_current
        # To get the actual rows: we need rows where rule_pass_col == 0 AND (previous rule passed)
        # However, for simplicity and safety, we filter for the specific failure on the raw data
        # but only for records that were still in the 'valid' set.
        
        # Actually, if we just want the failed records for this rule:
        failed = df.filter(F.col(rule_pass_col) == 0)
        # wait, this includes records that failed PREVIOUS rules too.
        # We need records that failed EXACTLY this rule.
        # That would be: (previous_rule_pass_col == 1) AND (this_rule_pass_col == 0)
        
        # Let's keep it simple: just grab rows where this_rule_pass_col == 0 
        # but ensure we don't double count if we just want "records that failed".
        # Current framework writes ALL failed records for each rule to the error table.
        
        failed = failed.withColumn("dqm_check_type", F.lit(dqm.qc_type)) \
                       .withColumn("failed_column_name", F.lit(dqm.column_name)) \
                       .withColumn("fail_value", F.col(dqm.column_name).cast("string")) \
                       .select("dqm_check_type", "failed_column_name", "fail_value", "batch_id")
        
        failed.write.format("delta").mode("append").partitionBy("batch_id").save(self.dqm_error_location)

    def buffer_log(self, dqm, log, batch_id, fail_count, fail_pct, success, start_time):
        status = Constants.STATUS_SUCCEEDED if success else (Constants.STATUS_FAILED if dqm.criticality == Constants.CRITICALITY_CRITICAL else Constants.STATUS_SUCCEEDED)
        log_entry = Logs.logDqmDtl(
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
            error_count=fail_count,
            error_pct=fail_pct,
            status=status,
            dqm_start_time=start_time,
            dqm_end_time=datetime.now(),
        )
        self.log_buffer.append(log_entry)

    def flush_logs(self):
        if not self.log_buffer:
            return
        with OrchestrationProcess.OrchestrationProcess() as orch:
            for entry in self.log_buffer:
                orch.insert_log_dqm(entry)
        self.log_buffer = []

    def _write_data_optimized(self, df: DataFrame, batch_id):
        # Select original columns + batch_id (drop the temporary pass/fail flags)
        original_cols = [c for c in df.columns if not (c.startswith("passed_") or c in ["final_valid", "all_valid_so_far"])]
        df = df.select(*original_cols)
        
        if self.table_location_type.lower() == Constants.TABLE_TYPE_EXTERNAL:
            df.write.format("delta").mode("append").partitionBy(self.staging_partition_columns.split(",")).save(self.staging_location)
            df.write.format("delta").mode("append").partitionBy(self.publish_partition_columns.split(",")).save(self.publish_location)
        else:
            df.write.format("delta").mode("append").partitionBy(self.staging_partition_columns.split(",")).saveAsTable(f"{self.env}.{self.staging_table_name}")
            df.write.format("delta").mode("append").partitionBy(self.publish_partition_columns.split(",")).saveAsTable(f"{self.env}.{self.publish_table_name}")

    def handle_no_dqm_masters(self):
        for log in self.dqm_unprocessed_files:
            bid = log.batch_id
            df = self.spark.read.format("delta").load(self.data_standardisation_location).filter(F.col("batch_id") == bid)
            self._write_data_optimized(df, bid)
            self.buffer_log(DqmMaster.ctlDqmMasterDtl(qc_type="N/A", criticality="W"), log, bid, 0, 0, True, datetime.now())
        self.flush_logs()
