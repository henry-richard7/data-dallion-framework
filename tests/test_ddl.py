from data_dallion_framework.Common.DDLGenerator import DDLGenerator
import sys

def test_ddl_generation():
    gen = DDLGenerator()
    
    # Mock data
    env = "dev"
    table_name = "raw_sales_data"
    columns = [
        {"name": "order_id", "type": "string"},
        {"name": "total_amount", "type": "double"},
        {"name": "order_date", "type": "timestamp"},
        {"name": "customer_id", "type": "string"}
    ]
    partition_by = ["order_date"]
    location = "abfss://container@storage.dfs.core.windows.net/bronze/raw_sales_data"
    
    # Test External Table
    print("Testing External Table DDL:")
    ddl_external = gen.generate_ddl(
        env=env,
        table_name=table_name,
        columns=columns,
        partition_by=partition_by,
        location=location,
        is_external=True
    )
    print(ddl_external)
    print("-" * 40)
    
    # Test Managed Table (No location)
    print("Testing Managed Table DDL:")
    ddl_managed = gen.generate_ddl(
        env=env,
        table_name=table_name,
        columns=columns,
        partition_by=partition_by,
        is_external=False
    )
    print(ddl_managed)

if __name__ == "__main__":
    test_ddl_generation()
