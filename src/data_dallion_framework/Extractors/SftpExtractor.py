import paramiko
from io import StringIO
from datetime import datetime
from json import loads as json_loads
from pathlib import Path
import traceback

from data_dallion_framework.Common import OrchestrationProcess, PatternValidator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail


class SFTPExtractor:
    """
    Extracts explicit remote flat files from SFTP servers over standard SSH channels.

    Utilizes Paramiko with private key or raw credential loading mechanisms to ingest the stream sequentially.
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
        ssh_key,
        process_id,
    ):
        """
        Initializes SFTP protocol interaction traversing foreign locations down to internal landing zones.

        Args:
            pre_ingestion_logs (list[logDataAcquisitionDetail]): Log entries from earlier loads confirming unique fetching parameters.
            inbound_location (str): Destination string representing physical file paths to insert flat file strings.
            outbound_source_location (str): Upstream SFTP source file path directory.
            file_pattern_static (str): Custom execution indicator 'Y' or 'N' altering regex application against filenames.
            file_pattern (str): The configuration matching validation string isolating files mapped correctly.
            connection_config (str/dict): Context mapped credentials JSON defining password, host mappings, and username.
            pre_ingestion_dataset_id (int): Identifier context linking records to overall meta configuration state.
            ssh_key (str): Optional Ed25519 PKIX string string buffer mapping if no explicit passwords config mapped.
            process_id (int): Workflow extraction integer tying together entire framework executions across tables.
        """
        connection_config: dict = json_loads(connection_config)

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True, exist_ok=True)
        except OSError:
            pass

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if ssh_key:
            privatekeyfile = StringIO(ssh_key)
            mykey = paramiko.Ed25519Key.from_private_key(privatekeyfile)

        else:
            mykey = None

        ssh.connect(
            hostname=connection_config.get("host"),
            password=connection_config.get("password"),
            username=connection_config.get("user"),
            allow_agent=True,
            pkey=mykey,
            timeout=10.0,
        )

        sftp = ssh.open_sftp()

        files = sftp.listdir(outbound_source_location)

        for file_ in files:
            file_save_name = inbound_location + file_
            batch_id = int(datetime.now().strftime("%Y%m%d%H%M%S%f")[:-1])
            start_time = datetime.now()

            if (len(pre_ingestion_logs) == 0) or (
                file_save_name not in pre_ingestion_processed_files
            ):
                try:
                    if PatternValidator.validate_pattern(
                        file_pattern=file_pattern,
                        file_name=file_,
                        custom=True if file_pattern_static == "Y" else False,
                    ):
                        remote_file = sftp.file(
                            outbound_source_location + file_, mode="r"
                        )
                        with open(file_save_name, "wb") as write_sftp:
                            while True:
                                data = remote_file.read(524288000)
                                if not data:
                                    break
                                write_sftp.write(data)
                        with (
                            OrchestrationProcess.OrchestrationProcess() as orch_process
                        ):
                            orch_process.insert_log_data_acquisition_detail(
                                log_data_acquisition=logDataAcquisitionDetail(
                                    batch_id=batch_id,
                                    run_date=start_time.date(),
                                    process_id=process_id,
                                    pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                    outbound_source_location="SFTP",
                                    inbound_file_location=file_save_name,
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
                                run_date=start_time.date(),
                                process_id=process_id,
                                pre_ingestion_dataset_id=pre_ingestion_dataset_id,
                                outbound_source_location="SFTP",
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
