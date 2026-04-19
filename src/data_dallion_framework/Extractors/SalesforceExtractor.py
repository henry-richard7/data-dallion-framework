import traceback

from data_dallion_framework.Common import OrchestrationProcess, FileNameGenerator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail

import niquests
import base64
import jwt
from datetime import datetime
import csv

from json import loads as json_loads
from pathlib import Path


class SalesForce:
    def __init__(
        self,
        connection_config: dict,
    ) -> None:
        self.domain = connection_config["domain"]
        client_id = connection_config["client_id"]
        client_secret = connection_config["client_secret"]
        oauth_endpoint = "/services/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        response = niquests.post(url=f"{self.domain}{oauth_endpoint}", data=payload)

        if response.status_code == 200:
            access_token = response.json()["access_token"]
            self.headers = {"Authorization": "Bearer " + access_token}
        else:
            raise Exception(f"Failed In Getting Access Token:\n {response.text}")

    def query(self, columns: list[str], dataset_name: str) -> list[dict]:
        query_ = f"select {','.join(columns)} FROM {dataset_name}"

        endpoint = "/services/data/v62.0/queryAll"
        response = niquests.get(
            f"{self.domain}{endpoint}",
            headers=self.headers,
            params={"q": query_},
        ).json()

        records = response["records"]
        more_results = list()

        results = list()
        for record in records:
            results.append({column: record[column] for column in columns})

        while not response["done"]:
            response = niquests.get(
                f"{self.domain}{response['nextRecordsUrl']}",
                headers=self.headers,
            ).json()
            records_ = response["records"]

            for record in records_:
                more_results.append({column: record[column] for column in columns})

        if len(more_results) != 0:
            results = results + more_results

        return results


class SalesforceExtractor:
    def __init__(
        self,
        pre_ingestion_logs: list[logDataAcquisitionDetail],
        inbound_location,
        outbound_source_file_format,
        file_pattern,
        pre_ingestion_dataset_name,
        pre_ingestion_dataset_id,
        outbound_file_delimiter,
        columns,
        process_id,
        connection_config,
    ):

        connection_config: dict = json_loads(connection_config)
        file_pattern = (
            file_pattern.split(".")[0] if "." in file_pattern else file_pattern
        )
        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True)
        except:
            pass

        save_file_name = FileNameGenerator.file_name_generator(file_pattern)
        file_save_name = (
            f"{inbound_location}{save_file_name}.{outbound_source_file_format}"
        )

        if file_save_name not in pre_ingestion_processed_files:
            start_time = datetime.now()
            batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

            try:
                salesforce_extractor = SalesForce(
                    connection_config=connection_config,
                )
                columns = columns.split(",")
                records = salesforce_extractor.query(
                    columns=columns, dataset_name=pre_ingestion_dataset_name
                )

                with open(file_save_name, mode="w", newline="") as file:
                    writer = csv.DictWriter(
                        file, fieldnames=columns, delimiter=outbound_file_delimiter
                    )
                    writer.writeheader()
                    writer.writerows(records)

                with OrchestrationProcess.OrchestrationProcess() as orch_process:
                    orch_process.insert_log_data_acquisition_detail(
                        log_data_acquisition=logDataAcquisitionDetail(
                            batch_id=batch_id,
                            process_id=process_id,
                            run_date=datetime.now().date(),
                            outbound_source_location="SALESFORCE/VEEVA",
                            inbound_file_location=file_save_name,
                            pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                            status="SUCCEEDED",
                            start_time=start_time,
                            end_time=datetime.now(),
                        )
                    )

            except Exception as e:
                with OrchestrationProcess.OrchestrationProcess() as orch_process:
                    orch_process.insert_log_data_acquisition_detail(
                        log_data_acquisition=logDataAcquisitionDetail(
                            batch_id=batch_id,
                            process_id=process_id,
                            run_date=datetime.now().date(),
                            outbound_source_location="SALESFORCE/VEEVA",
                            inbound_file_location=file_save_name,
                            pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                            status="FAILED",
                            exception_details=traceback.format_exc(),
                            start_time=start_time,
                            end_time=datetime.now(),
                        )
                    )
                raise
        else:
            raise Exception(f"{file_save_name} Is Already Moved to Inbound Location.")
