import niquests
import base64
import jwt
from datetime import datetime, timedelta, timezone
import csv
from typing import Union, List, Generator
import itertools
from json import loads as json_loads
import json
from pathlib import Path
import re
import traceback
from niquests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from data_dallion_framework.Common import (
    JsonDataMapper,
    FileNameGenerator,
    OrchestrationProcess,
    Constants
)
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail
from dateutil.relativedelta import relativedelta


class APIAutomation:
    """
    A class used to automate API requests with retry logic and dynamic request body generation.
    """

    def __init__(self, config):
        self.token = None
        self.config = config
        self.headers = {}
        self.params = {}
        self.data = {}
        self.json_body = {}
        
        # Setup session with retry logic
        self.session = niquests.Session(multiplexed=True)
        retry_strategy = Retry(
            total=Constants.API_DEFAULT_RETRIES,
            backoff_factor=Constants.API_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _replace_date(self, date_match: str) -> str:
        # (Logic unchanged)
        if ":" in date_match:
            date_part, date_format = date_match.split(":")
            date_format = date_format.replace("$", "")
        else:
            date_part = date_match
            date_format = "%s" if "current_timestamp" in date_part else "%Y-%m-%d"

        date_part = date_part.replace("$", "")
        subtract_match = re.search(r"([-+])([0-9]+)([MY]?)", date_part)

        offset_value = 0
        offset_type = "D"
        if subtract_match:
            offset_sign = subtract_match.group(1)
            offset_value = int(subtract_match.group(2))
            offset_type = subtract_match.group(3) or "D"
            if offset_sign == "-": offset_value = -offset_value

        base_key = re.sub(r"[-+][0-9]+[MY]?", "", date_part)
        base_date = self._get_base_date(base_key)

        if offset_type == "D": base_date += timedelta(days=offset_value)
        elif offset_type == "M": base_date += relativedelta(months=offset_value)
        elif offset_type == "Y": base_date += relativedelta(years=offset_value)

        return str(int(base_date.timestamp())) if date_format == "%s" else base_date.strftime(date_format)

    def _get_base_date(self, key: str) -> datetime:
        today = datetime.today()
        if key == "current_date": return datetime(today.year, today.month, today.day)
        if key == "current_timestamp": return today
        if key == "current_month_start": return datetime(today.year, today.month, 1)
        if key == "current_month_end":
            next_month = today.replace(day=28) + timedelta(days=4)
            return datetime(next_month.year, next_month.month, 1) - timedelta(days=1)
        if key == "current_week_start": return today - timedelta(days=today.weekday())
        if key == "current_week_end": return today + timedelta(days=(6 - today.weekday()))
        raise ValueError(f"Unknown placeholder: {key}")

    def date_parse_changer(self, body: dict) -> dict:
        body_str = json.dumps(body)
        full_matches = re.findall(
            r"\$(?:current_date|current_timestamp|current_month_start|current_month_end|"
            r"current_week_start|current_week_end)(?:[-+][0-9]+[MY]?)?(?::[^$]+)?\$",
            body_str,
        )
        for match in full_matches:
            body_str = body_str.replace(match, self._replace_date(match))
        return json.loads(body_str)

    def fetch_token(self, step: dict):
        auth_type = step.get("auth_type")
        
        if auth_type == "oauth":
            response = self.session.request(
                method=step.get("method", "GET"),
                url=step["token_url"],
                data={
                    "grant_type": "client_credentials",
                    "client_id": step["client_id"],
                    "client_secret": step["client_secret"],
                },
                timeout=Constants.API_DEFAULT_TIMEOUT
            )
            response.raise_for_status()
            self.headers = {
                "Authorization": f"{step['token_type']} {response.json().get(step.get('token_path', 'access_token'))}"
            }
        # ... (Rest of token types unchanged but using self.session and constants)
        elif auth_type == "service_account":
             # (Legacy logic using self.session)
             private_key = step["private_key"]
             payload = {
                 "iss": step["issuer"],
                 "scope": step["scope"],
                 "aud": step["token_url"],
                 "exp": datetime.now(timezone.utc) + timedelta(minutes=60),
                 "iat": datetime.now(timezone.utc),
             }
             jwt_token = jwt.encode(payload, private_key, algorithm="RS256")
             response = self.session.post(
                 step["token_url"],
                 data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": jwt_token},
                 timeout=Constants.API_DEFAULT_TIMEOUT
             )
             response.raise_for_status()
             self.headers = {"Authorization": f"Bearer {response.json().get(step.get('token_path', 'access_token'))}"}
        elif auth_type == "basic_auth":
            username, password = step["basic_auth"]["username"], step["basic_auth"]["password"]
            self.headers = {"Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()}
        elif auth_type == "custom":
            response = self.session.request(method=step["method"].upper(), url=step["token_url"], timeout=Constants.API_DEFAULT_TIMEOUT).json()
            self.headers = {"Authorization": "Bearer " + response.get(step["token_path"])}
        else:
            raise ValueError(f"Unsupported auth_type: {auth_type}")

    def execute_request(self, method, url, headers, params, data, json_body, ssl_verify=True, stream=True):
        """Executed a single request, optionally streaming the response to save memory."""
        response = self.session.request(
            method=method, url=url, headers=headers or None, params=params or None,
            data=data or None, json=json_body or None, verify=ssl_verify,
            timeout=Constants.API_DEFAULT_TIMEOUT,
            stream=stream
        )
        response.raise_for_status()
        return response

    def make_request(self, step: dict) -> Generator[dict, None, None]:
        url, method = step["url"], step.get("method", "GET").upper()
        headers = {**self.headers, **step.get("headers", {})}
        ssl_verify = step.get("ssl_verify", True)
        params, data, json_body = map(
            lambda k: {**getattr(self, k), **step.get(k, {})}, ["params", "data", "json_body"]
        )

        # Date parsing
        if "$current_date" in str(data): data = self.date_parse_changer(data)
        if "$current_date" in str(json_body): json_body = self.date_parse_changer(json_body)
        if "$current_date" in str(params): params = self.date_parse_changer(params)

        if not step.get("body_values"):
            # Return as a generator yielding one response object
            yield self.execute_request(method, url, headers, params, data, json_body, ssl_verify)
            return

        # Handle multiplexed requests (multiplexing is natively supported by the session we created)
        # We'll still process them and yield responses as they come
        body_values = step["body_values"]
        keys, values = list(body_values.keys()), list(body_values.values())
        for combination in itertools.product(*values):
            t_json = json.dumps(json_body)
            t_data = json.dumps(data)
            t_params = json.dumps(params)
            for k, v in zip(keys, combination):
                t_json = t_json.replace(k, v)
                t_data = t_data.replace(k, v)
                t_params = t_params.replace(k, v)
            
            p_json = json.loads(t_json) if t_json not in ("None", "", "{}") else None
            p_data = json.loads(t_data) if t_data not in ("None", "", "{}") else None
            p_params = json.loads(t_params) if t_params not in ("None", "", "{}") else None
            
            yield self.execute_request(method, url, headers, p_params, p_data, p_json, ssl_verify)

    def execute_workflow(self) -> Generator[niquests.Response, None, None]:
        for step in self.config:
            if step["type"] == "TOKEN":
                self.fetch_token(step)
            else:
                yield from self.make_request(step)

class APIExtractor:
    def __init__(self, pre_ingestion_logs: list[logDataAcquisitionDetail], inbound_location, 
                 outbound_source_file_format, file_pattern, pre_ingestion_dataset_id, 
                 outbound_file_delimiter, process_id):
        
        file_pattern = file_pattern.split(".")[0] if "." in file_pattern else file_pattern
        pre_ingestion_paths = [x.inbound_file_location for x in pre_ingestion_logs]
        
        try: Path(inbound_location).mkdir(parents=True, exist_ok=True)
        except: pass

        save_name = FileNameGenerator.file_name_generator(file_pattern)
        file_path = f"{inbound_location}{save_name}.{outbound_source_file_format}"

        if file_path in pre_ingestion_paths:
            raise Exception(f"{file_path} is already moved to Inbound Location.")

        start_time = datetime.now()
        batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])

        with OrchestrationProcess.OrchestrationProcess() as orch:
            column_meta = orch.get_ctl_column_metadata(dataset_id=pre_ingestion_dataset_id)
            api_conn_dtls = orch.get_ctl_api_connection_details(dataset_id=pre_ingestion_dataset_id)
        
        json_mapping = {x.source_column_name: x.column_json_mapping for x in column_meta}
        config = []
        for dt in api_conn_dtls:
            temp = {"method": dt.method, "type": dt.type, "ssl_verify": (dt.ssl_verify == "Y")}
            if dt.type == "TOKEN":
                temp.update({"token_url": dt.token_url, "auth_type": dt.auth_type, "token_type": dt.token_type, "token_path": dt.token_path})
                if dt.client_id: temp["client_id"] = dt.client_id
                if dt.client_secret: temp["client_secret"] = dt.client_secret
                if dt.username: temp["username"] = dt.username
                if dt.password: temp["password"] = dt.password
                if dt.issuer: temp["issuer"] = dt.issuer
                if dt.scope: temp["scope"] = dt.scope
                if dt.private_key: temp["private_key"] = dt.private_key
            else:
                temp["url"] = dt.url
                if dt.headers: temp["headers"] = json_loads(dt.headers)
                if dt.params: temp["params"] = json_loads(dt.params)
                if dt.data: temp["data"] = json_loads(dt.data)
                if dt.json_body: temp["json_body"] = json_loads(dt.json_body)
                if dt.body_values: temp["body_values"] = json_loads(dt.body_values)
                if dt.key_to_add_to_data: temp["key_to_add_to_data"] = dt.key_to_add_to_data
            config.append(temp)

        try:
            automation = APIAutomation(config=config)
            with open(file_path, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=[x.source_column_name for x in column_meta], 
                                         delimiter=outbound_file_delimiter)
                writer.writeheader()
                
                # Streaming extraction to JsonDataMapper
                for response in automation.execute_workflow():
                    raw_data = response.json()
                    # If the response is a list, we pass each item; if it's a dict, we pass as is
                    if isinstance(raw_data, list):
                        for item in raw_data:
                            mapped = JsonDataMapper.JsonDataMapper(mapping=json_mapping, json_data=item).get_mapped_data()
                            writer.writerows(mapped)
                    else:
                        mapped = JsonDataMapper.JsonDataMapper(mapping=json_mapping, json_data=raw_data).get_mapped_data()
                        writer.writerows(mapped)

            with OrchestrationProcess.OrchestrationProcess() as orch:
                orch.insert_log_data_acquisition_detail(log_data_acquisition=logDataAcquisitionDetail(
                    batch_id=batch_id, run_date=start_time.date(), process_id=process_id,
                    pre_ingestion_dataset_id=pre_ingestion_dataset_id, outbound_source_location="API",
                    inbound_file_location=file_path, status=Constants.STATUS_SUCCEEDED,
                    start_time=start_time, end_time=datetime.now()
                ))
        except Exception as e:
            with OrchestrationProcess.OrchestrationProcess() as orch:
                orch.insert_log_data_acquisition_detail(log_data_acquisition=logDataAcquisitionDetail(
                    batch_id=batch_id, run_date=start_time.date(), process_id=process_id,
                    pre_ingestion_dataset_id=pre_ingestion_dataset_id, outbound_source_location="API",
                    inbound_file_location=None, status=Constants.STATUS_FAILED,
                    exception_details=traceback.format_exc(), start_time=start_time, end_time=datetime.now()
                ))
            raise
