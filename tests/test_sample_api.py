from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from data_dallion_framework.BronzeScripts import PerformBronze
import os
import shutil

def run_test():
    # Initialize Spark with Delta Support
    builder = (
        SparkSession.builder
        .appName("delta")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    process_id = 1
    env = "dev"

    print(f"Starting Bronze Processing for Process ID: {process_id}...")
    
    # 1. Run Bronze Layer
    bronze = PerformBronze.PerformBronze(spark=spark, process_id=process_id, env=env)
    
    # Run Extraction (API -> Inbound)
    bronze.start_extraction()
    
    # Run Table Creation (Inbound -> Bronze Delta)
    # The dataset master loop is inside start_extraction in some versions or called separately
    # In my current version of PerformBronze.py:
    # with ThreadPoolExecutor(...) as executor:
    #     futures = [executor.submit(self._handle_raw_table_creation, master) for master in self.bronze_dataset_masters]
    
    print("Bronze Processing Completed.")

    # Check results
    target_path = "./data/bronze/jsonplaceholder_posts/"
    if os.path.exists(target_path):
        df = spark.read.format("delta").load(target_path)
        print(f"Bronze Table Schema: {df.schema}")
        print(f"Sample Records (Count: {df.count()}):")
        df.show(5)
    else:
        print(f"Error: Target path {target_path} not found.")

if __name__ == "__main__":
    run_test()
