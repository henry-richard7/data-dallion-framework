import boto3
from botocore.client import Config
from datetime import datetime
from json import loads as json_loads
from pathlib import Path

from data_dallion_framework.Common import OrchestrationProcess, PatternValidator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail


class S3Extractor:
    def __init__(
        self,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        inbound_location,
        outbound_source_location,
        file_pattern_static,
        file_pattern,
        connection_config,
        pre_ingestion_dataset_id,
        process_id,
    ):
        connection_config: dict = json_loads(connection_config)

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True)
        except:
            pass

        client_id = connection_config["client_id"]
        client_secret = connection_config["client_secret"]

        endpoint_url = connection_config.get("endpoint_url", False)
        region = connection_config.get("region", False)
        signature_version = connection_config.get("signature_version", False)

        config_ = dict()

        config_["aws_access_key_id"] = client_id
        config_["aws_secret_access_key"] = client_secret

        if endpoint_url:
            config_["endpoint_url"] = endpoint_url

        if region:
            config_["region"] = region

        if signature_version:
            config_["config"] = Config(signature_version=signature_version)

        s3_client = boto3.client(
            "s3",
            **config_,
        )

        s3_object_config = self.parse_location(
            outbound_location=outbound_source_location
        )

        files = s3_client.list_objects_v2(**s3_object_config).get("Contents")

        if files:
            for file in files:
                file_ = file.get("Key")
                file_name_s3 = file_.split("/")[-1]

                if PatternValidator.validate_pattern(
                    file_pattern=file_pattern,
                    file_name=file_name_s3,
                    custom=True if file_pattern_static == "Y" else False,
                ):
                    file_save_name = inbound_location + file_name_s3

                    if file_save_name not in pre_ingestion_processed_files:
                        start_time = datetime.now()
                        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

                        try:
                            with open(file_save_name, "wb") as f:
                                s3_client.download_fileobj(
                                    s3_object_config["Bucket"], file.get("Key"), f
                                )

                            with (
                                OrchestrationProcess.OrchestrationProcess() as orch_process
                            ):
                                orch_process.insert_log_data_acquisition_detail(
                                    batch_id=batch_id,
                                    process_id=process_id,
                                    run_date=datetime.now().date(),
                                    outbound_source_location=outbound_source_location,
                                    inbound_file_location=file_save_name,
                                    pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                    status="SUCCEEDED",
                                    start_time=start_time,
                                    end_time=datetime.now(),
                                )
                        except Exception as e:
                            with (
                                OrchestrationProcess.OrchestrationProcess() as orch_process
                            ):
                                orch_process.insert_log_data_acquisition_detail(
                                    batch_id=batch_id,
                                    process_id=process_id,
                                    run_date=datetime.now().date(),
                                    outbound_source_location=outbound_source_location,
                                    inbound_file_location=file_save_name,
                                    pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                    status="FAILED",
                                    exception_details=e,
                                    start_time=start_time,
                                    end_time=datetime.now(),
                                )
                            raise

                    else:
                        raise Exception(
                            f"{file_save_name} Is Already Moved to Inbound Location."
                        )

    def parse_location(self, outbound_location: str) -> dict:
        splited_path = outbound_location.rstrip("/").split("/")
        if splited_path[0] == "":
            splited_path.pop(0)
        bucket_name = splited_path[0]
        prefix = "/".join(splited_path[1:]) + "/"

        return {
            "Bucket": bucket_name,
            "Prefix": prefix,
        }
