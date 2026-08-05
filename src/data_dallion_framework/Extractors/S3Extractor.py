import boto3
from botocore.client import Config
from datetime import datetime
from json import loads as json_loads
from pathlib import Path
import traceback

from data_dallion_framework.Common import OrchestrationProcess, PatternValidator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail


class S3Extractor:
    """
    Handles extracting files out of an AWS S3 bucket directly into the local inbound landing directory.

    Requires securely parsing S3 protocols, configuring Boto3, and iterating matching files down to chunks.
    """
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
        """
        Initializes the S3Extractor and synchronizes the objects from the bucket.

        Args:
            pre_ingestion_logs (list[logDataAcquisitionDetail]): Log structures preventing extraction duplications.
            inbound_location (str): Physical cluster directory path acting as a local sink.
            outbound_source_location (str): Pre-configured raw S3 URI specifying bucket/folder.
            file_pattern_static (str): Indicator character ('Y' or 'N') detailing if regex evaluation is customized.
            file_pattern (str): Raw string defining matching configurations for finding specific subset files.
            connection_config (str/dict): Parsed JSON format defining programmatic access parameters like aws_access_key_id.
            pre_ingestion_dataset_id (int): Foreign key connecting to orchestration metadata parameters.
            process_id (int): Universal workflow process key indicating extraction status context.
        """
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
                                    log_data_acquisition=logDataAcquisitionDetail(
                                        batch_id=batch_id,
                                        run_date=start_time.date(),
                                        process_id=process_id,
                                        pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                        outbound_source_location="S3",
                                        inbound_file_location=file_save_name,
                                        status="SUCCEEDED",
                                        start_time=start_time,
                                        end_time=datetime.now(),
                                    )
                                )
                        except Exception as e:
                            with (
                                OrchestrationProcess.OrchestrationProcess() as orch_process
                            ):
                                orch_process.insert_log_data_acquisition_detail(
                                    log_data_acquisition=logDataAcquisitionDetail(
                                        batch_id=batch_id,
                                        run_date=start_time.date(),
                                        process_id=process_id,
                                        pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                        outbound_source_location="S3",
                                        inbound_file_location=None,
                                        exception_details=traceback.format_exc(),
                                        status="FAILED",
                                        start_time=start_time,
                                        end_time=datetime.now(),
                                    )
                                )
                                raise

                    else:
                        print(f"{file_save_name} is already processed. Skipping.")
                        continue

    def parse_location(self, outbound_location: str) -> dict:
        """
        Parses an S3 URI to isolate the root Bucket name and underlying prefix path.

        Args:
            outbound_location (str): Full S3 string representation.

        Returns:
            dict: Structured dictionary containing exact 'Bucket' and 'Prefix' keys.
        """
        cleaned_path = outbound_location
        for scheme in ["s3://", "s3a://", "s3n://"]:
            if cleaned_path.startswith(scheme):
                cleaned_path = cleaned_path[len(scheme):]
                break

        splited_path = cleaned_path.rstrip("/").split("/")
        if splited_path[0] == "":
            splited_path.pop(0)
        bucket_name = splited_path[0]
        prefix = "/".join(splited_path[1:]) + "/"

        return {
            "Bucket": bucket_name,
            "Prefix": prefix,
        }
