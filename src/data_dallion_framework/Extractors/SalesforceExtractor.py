import traceback

from data_dallion_framework.Common import OrchestrationProcess, FileNameGenerator, Constants
from data_dallion_framework.Common.SecretManager import resolve_secret
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail

import niquests
import base64
import jwt
from datetime import datetime
import csv

from json import loads as json_loads
from pathlib import Path


class SalesForce:
    """
    A foundational connector enabling OAuth 2.0 authorized querying of a Salesforce instance.

    Uses `niquests` to retrieve access tokens from client credentials flow and queries SOQL iteratively.
    """
    def __init__(
        self,
        connection_config: dict,
    ) -> None:
        """
        Validates the configuration and logs into the Salesforce instance, capturing an access token.

        Args:
            connection_config (dict): Client authorization dictionary mapping keys like domain, client_id, and secret.
        
        Raises:
            Exception: Thrown if auth credentials yield non-200 responses.
        """
        self.domain = connection_config["domain"]
        client_id = connection_config["client_id"]
        client_secret = connection_config["client_secret"]
        oauth_endpoint = "/services/oauth2/token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        self.retry_config = niquests.RetryConfiguration(
            total=Constants.API_DEFAULT_RETRIES,
            backoff_factor=Constants.API_BACKOFF_FACTOR,
            status_forcelist=[502, 503, 504, 429]
        )

        response = niquests.post(
            url=f"{self.domain}{oauth_endpoint}", 
            data=payload,
            timeout=Constants.API_DEFAULT_TIMEOUT,
            retries=self.retry_config,
        )

        if response.status_code == 200:
            access_token = response.json()["access_token"]
            self.headers = {"Authorization": "Bearer " + access_token}
        else:
            raise Exception(f"Failed In Getting Access Token:\n {response.text}")

    def query(self, columns: list[str], dataset_name: str):
        """
        Executes a dynamic SOQL extraction query capturing all records.

        Iteratively fetches bulk data by traversing via standard Salesforce `nextRecordsUrl` keys.

        Args:
            columns (list[str]): The specific target fields to request in the SQL.
            dataset_name (str): Representative Salesforce object name (e.g. standard Contact, custom MyObject__c).

        Yields:
            list[dict]: Batch of python dictionaries mapping column to matching row entity.
        """
        query_ = f"select {','.join(columns)} FROM {dataset_name}"

        endpoint = "/services/data/v62.0/queryAll"
        response = niquests.get(
            f"{self.domain}{endpoint}",
            headers=self.headers,
            params={"q": query_},
            timeout=Constants.API_DEFAULT_TIMEOUT,
            retries=self.retry_config,
        ).json()

        records = response["records"]
        yield [{column: record[column] for column in columns} for record in records]

        while not response["done"]:
            response = niquests.get(
                f"{self.domain}{response['nextRecordsUrl']}",
                headers=self.headers,
                timeout=Constants.API_DEFAULT_TIMEOUT,
                retries=self.retry_config,
            ).json()
            records_ = response["records"]
            yield [{column: record[column] for column in columns} for record in records_]


class SalesforceExtractor:
    """
    Orchestration wrapper directing data retrieved by the SalesForce connector class onto the cluster inbound.

    Implements duplication detection and saves the data iteratively out to an explicitly requested formatting delimiter.
    """
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
        """
        Initializes extraction from Salesforce utilizing the given config elements.

        Args:
            pre_ingestion_logs (list[logDataAcquisitionDetail]): Contextual logs to compare filename history preventing duplicated ingestion pipelines.
            inbound_location (str): Folder structure location path.
            outbound_source_file_format (str): Expected extension type config like 'csv'.
            file_pattern (str): General generated naming rule prefix mapping string literal.
            pre_ingestion_dataset_name (str): Identifier denoting Salesforce SOQL Object names.
            pre_ingestion_dataset_id (int): Matching orchestration detail mapping ID context.
            outbound_file_delimiter (str): Data flattening character string logic for generated output files.
            columns (str): Flattened string listing variables explicitly projected mapped query.
            process_id (int): Identifying orchestration identifier correlating processing logs into a unified tracking interface.
            connection_config (str/dict): JSON format connection configuration dictionary containing API host domains and credentials.
        """

        connection_config: dict = json_loads(connection_config)
        connection_config = {k: resolve_secret(v) if isinstance(v, str) else v for k, v in connection_config.items()}
        file_pattern = (
            file_pattern.split(".")[0] if "." in file_pattern else file_pattern
        )
        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True, exist_ok=True)
        except OSError:
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
                with open(file_save_name, mode="w", newline="") as file:
                    writer = csv.DictWriter(
                        file, fieldnames=columns, delimiter=outbound_file_delimiter
                    )
                    writer.writeheader()
                    
                    for records_batch in salesforce_extractor.query(
                        columns=columns, dataset_name=pre_ingestion_dataset_name
                    ):
                        writer.writerows(records_batch)

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
