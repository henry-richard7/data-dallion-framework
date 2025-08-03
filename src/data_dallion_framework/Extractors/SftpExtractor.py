import paramiko
from io import StringIO
from datetime import datetime
from json import loads as json_loads
from pathlib import Path
from data_dallion_framework.Common import OrchestrationProcess, PatternValidator
from data_dallion_framework.Common.Models.Logs import logDataAcquisitionDetail


class SFTPExtractor:
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
        connection_config: dict = json_loads(connection_config)

        pre_ingestion_processed_files = [
            x.inbound_file_location for x in pre_ingestion_logs
        ]

        try:
            Path(inbound_location).mkdir(parents=True)
        except:
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
                    with OrchestrationProcess.OrchestrationProcess() as orch_process:
                        orch_process.insert_log_data_acquisition_detail(
                            batch_id=batch_id,
                            process_id=process_id,
                            run_date=datetime.now().date(),
                            outbound_source_location=outbound_source_location,
                            inbound_file_location=None,
                            status="FAILED",
                            exception_details=e,
                            start_time=start_time,
                            end_time=datetime.now(),
                        )
                    raise
            else:
                raise Exception(
                    f"{file_save_name} Is Already Moved to Inbound location. Hence Failing the process."
                )
