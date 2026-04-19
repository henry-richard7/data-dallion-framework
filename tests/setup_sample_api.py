import os
from datetime import datetime
from sqlmodel import SQLModel, Session, create_engine, delete
from data_dallion_framework.Common.OrchestrationProcess import BackendSettings, _get_session_factory
from data_dallion_framework.Common.Models.Acquisition import (
    ctlApiConnectionsDtl,
    ctlDataAcquisitionDetail,
)
from data_dallion_framework.Common.Models.DatasetMaster import ctlDatasetMaster
from data_dallion_framework.Common.Models.ColumnMetadata import CtlColumnMetadata
from data_dallion_framework.Common.Models.DqmMaster import ctlDqmMasterDtl
from data_dallion_framework.Common import Constants

def setup_sample():
    # 1. Initialize Engine and Tables
    # This will use the settings from .env or defaults
    session_factory = _get_session_factory()
    engine = session_factory.kw['bind']
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        # 2. Dataset Master (Bronze)
        dataset_id = 100
        process_id = 1
        
        # Clean up existing if any using modern exec(delete(...))
        session.exec(delete(ctlDatasetMaster).where(ctlDatasetMaster.dataset_id == dataset_id))
        session.exec(delete(ctlDataAcquisitionDetail).where(ctlDataAcquisitionDetail.pre_ingestion_dataset_id == dataset_id))
        session.exec(delete(ctlApiConnectionsDtl).where(ctlApiConnectionsDtl.pre_ingestion_dataset_id == dataset_id))
        session.exec(delete(CtlColumnMetadata).where(CtlColumnMetadata.dataset_id == dataset_id))
        session.exec(delete(ctlDqmMasterDtl).where(ctlDqmMasterDtl.dataset_id == dataset_id))
        
        # New: Clean logs for the test dataset
        from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail, logRawProcessDtl
        session.exec(delete(logDataAcquisitionDetail).where(logDataAcquisitionDetail.pre_ingestion_dataset_id == dataset_id))
        session.exec(delete(logRawProcessDtl).where(logRawProcessDtl.dataset_id == dataset_id))

        # New: Delete local test file if exists
        inbound_file = "./data/inbound/jsonplaceholder/posts_sample.csv"
        if os.path.exists(inbound_file):
            os.remove(inbound_file)
            print(f"Deleted old inbound file: {inbound_file}")

        session.add(ctlDatasetMaster(
            dataset_id=dataset_id,
            process_id=process_id,
            dataset_name="JSONPlaceholder_Posts",
            dataset_type="BRONZE",
            inbound_location="./data/inbound/jsonplaceholder/",
            inbound_file_pattern="posts_.*",
            inbound_file_format="csv",
            inbound_file_delimiter=",",
            landing_table="jsonplaceholder_posts",
            landing_location="./data/bronze/jsonplaceholder_posts/",
            # landing_partition_columns="batch_id",
            table_location_type=Constants.TABLE_TYPE_EXTERNAL,
            is_active="Y"
        ))

        # 3. Data Acquisition Detail
        session.add(ctlDataAcquisitionDetail(
            process_id=process_id,
            pre_ingestion_dataset_id=dataset_id,
            pre_ingestion_dataset_name="JSONPlaceholder_Posts",
            outbound_source_platform="API",
            outbound_source_file_format="csv",
            outbound_file_delimiter=",",
            outbound_source_file_pattern="posts_sample",
            inbound_location="./data/inbound/jsonplaceholder/"
        ))

        # 4. API Connection Detail
        session.add(ctlApiConnectionsDtl(
            seq_no=1,
            pre_ingestion_dataset_id=dataset_id,
            type="RESPONSE",
            method="GET",
            url="https://jsonplaceholder.typicode.com/posts",
            ssl_verify="Y"
        ))

        # 5. Column Metadata
        columns = [
            ("userId", "int", "$.userId"),
            ("id", "int", "$.id"),
            ("title", "string", "$.title"),
            ("body", "string", "$.body"),
        ]
        
        for i, (col_name, col_type, mapping) in enumerate(columns):
            session.add(CtlColumnMetadata(
                dataset_id=dataset_id,
                table_name="jsonplaceholder_posts",
                column_id=i+1,
                column_name=col_name,
                source_column_name=col_name,
                column_data_type=col_type,
                column_json_mapping=mapping,
                is_active="Y"
            ))

        # 6. DQM Rules
        session.add(ctlDqmMasterDtl(
            qc_id=1,
            dataset_id=dataset_id,
            column_name="id",
            qc_type="Null",
            criticality=Constants.CRITICALITY_CRITICAL,
            criticality_threshold_pct=0,
            is_active="Y"
        ))

        session.commit()
        print(f"Sample configuration for Dataset {dataset_id} created successfully.")

if __name__ == "__main__":
    setup_sample()
