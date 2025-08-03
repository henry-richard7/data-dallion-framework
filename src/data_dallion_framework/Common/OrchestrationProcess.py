from pydantic import computed_field, model_validator, Field
from pydantic_settings import BaseSettings
from typing import Literal
from sqlmodel import SQLModel, create_engine, Session, select
from typing import Optional, Union
from pathlib import Path

from data_dallion_framework.Common.Models.Acquisition import (
    ctlApiConnectionsDtl,
    ctlDataAcquisitionConnectionMaster,
    ctlDataAcquisitionDetail,
)

from data_dallion_framework.Common.Models.Logs import (
    logTransformationDtl,
    logDataAcquisitionDetail,
    logDataStandardisationDtl,
    logDqmDtl,
    logRawProcessDtl,
)

from data_dallion_framework.Common.Models.ColumnMetadata import CtlColumnMetadata
from data_dallion_framework.Common.Models.DatasetMaster import ctlDatasetMaster
from data_dallion_framework.Common.Models.DataStandardisation import ctlDataStandardisationDtl
from data_dallion_framework.Common.Models.DqmMaster import ctlDqmMasterDtl
from data_dallion_framework.Common.Models.TransformationDependencyMaster import ctlTransformationDependencyMaster


class BackendSettings(BaseSettings):
    """Configure backend database settings for the datacraft framework.

    This class manages configuration parameters related to connecting to a database.
    It supports MySQL, PostgreSQL, and SQLite databases. If a full SQLAlchemy URL is
    provided via `sqlalchemy_url`, it will be used directly. Otherwise, connection
    parameters like user, password, host, port, etc., are used to construct the URL.

    Attributes:
        sqlalchemy_url (Optional[str]): Full SQLAlchemy connection string.
            If provided, this takes precedence over other fields.
        database_type (Literal["mysql", "postgresql", "sqlite"]): Type of database to use.
            Defaults to "sqlite".
        database (str): Name of the database/schema.
        user (Optional[str]): Database user.
        password (Optional[str]): Database password.
        hostname (Optional[str]): Database host. Defaults to "localhost".
        port (Optional[int]): Database port. Defaults based on database type:
            * MySQL: 3306
            * PostgreSQL: 5432
        datacraft_framework_home (Optional[str]): Root directory for framework files.
            Defaults to `$HOME/datacraft_framework`.
        connection_string (str): Computed field that returns the final SQLAlchemy
            connection string based on the configured values.
    """

    sqlalchemy_url: Optional[str] = Field(default=None, alias="database_url")

    # Database type
    database_type: Literal["mysql", "postgresql", "sqlite","mariadb"] = Field(
        default="sqlite", alias="db_type"
    )

    # Common database fields
    database: str = Field(default="nextgen_framework_configuration", alias="db_name")
    user: Optional[str] = Field(default=None, alias="db_user")
    password: Optional[str] = Field(default=None, alias="db_password")
    hostname: Optional[str] = Field(default="localhost", alias="db_host")
    port: Optional[int] = Field(default=None, alias="db_port")
    datacraft_framework_home: Optional[str] = Field(
        default=str(Path.home() / "datacraft_framework")
    )

    # Port defaults based on database type
    @model_validator(mode="after")
    def set_defaults(self):
        Path(self.datacraft_framework_home).mkdir(parents=True, exist_ok=True)

        if self.port is None:
            if self.database_type == "mysql":
                self.port = 3306
            elif self.database_type == "postgresql":
                self.port = 5432
        return self

    @computed_field
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string based on database configuration.

        If `sqlalchemy_url` is provided, it is returned as-is. Otherwise,
        constructs the appropriate connection string using the relevant
        dialect driver (e.g., pymysql for MySQL, psycopg2 for PostgreSQL),
        or builds an SQLite path.

        Returns:
            str: A valid SQLAlchemy connection string.
        """
        if self.sqlalchemy_url:
            return self.sqlalchemy_url

        if self.database_type == "mysql":
            driver = "pymysql"
            return f"mysql+{driver}://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        elif self.database_type == "postgresql":
            driver = "psycopg2"
            return f"postgresql+{driver}://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        elif self.database_type == "sqlite":
            db_path = Path(self.datacraft_framework_home) / f"{self.database}.db"
            return f"sqlite:///{db_path}"
        elif self.database_type == "mariadb":
            driver = "mariadbconnector"
            return f"mariadb+{driver}://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")


class OrchestrationProcess:
    """
    A class responsible for orchestrating database operations in the application.

    This class initializes database connections, ensures necessary tables are created,
    and provides a session for performing CRUD operations using ORM models.
    """

    def __init__(self) -> None:
        """
        Initialize the OrchestrationProcess with database connections and session.

        This constructor:
        - Loads backend settings for database connection
        - Establishes a database engine connection
        - Creates all tables defined under `SQLModel.metadata` if they don't exist
        - Initializes a database session for interaction with the ORM models

        Attributes:
            orch_settings (BackendSettings): Configuration object containing connection details.
            connection (Engine): SQLAlchemy engine instance for database connectivity.
            session (Session): SQLAlchemy session object for interacting with the database.
        """
        self.orch_settings = BackendSettings()
        self.connection = create_engine(self.orch_settings.connection_string)
        SQLModel.metadata.create_all(bind=self.connection)

        self.session = Session(self.connection)

    def get_ctl_column_metadata(self, dataset_id: int) -> list[CtlColumnMetadata]:
        """
        Retrieve metadata for columns associated with a specific dataset.

        This method queries the database to fetch all column metadata entries
        for the given dataset ID, sorted by their sequence number.

        Args:
            dataset_id (int): The unique identifier of the dataset.

        Returns:
            list[CtlColumnMetadata]: A list of CtlColumnMetadata objects representing
                the columns associated with the specified dataset.
        """
        query = (
            select(CtlColumnMetadata)
            .where(CtlColumnMetadata.dataset_id == dataset_id)
            .order_by(CtlColumnMetadata.column_sequence_number)
        )

        result = self.session.exec(query).all()
        return result

    def insert_ctl_column_metadata(self, column_metadata: CtlColumnMetadata) -> None:
        """
        Inserts column metadata details into the database.

        Args:
            column_metadata (CtlColumnMetadata): Metadata for the column to be inserted.

        Returns:
            None

        Raises:
            SQLAlchemyError: If a database error occurs during query execution.
        """
        self.session.add(column_metadata)
        self.session.commit()

    def get_ctl_api_connection_details(
        self, dataset_id: int
    ) -> list[ctlApiConnectionsDtl]:
        """
        Retrieves API connection details associated with a specific pre-ingestion dataset ID.

        This method queries the database to fetch all API connection details linked to the provided dataset ID. The results are ordered by sequence number to ensure deterministic ordering.

        Args:
            dataset_id (int): The pre-ingestion dataset ID used to filter API connection details.

        Returns:
            list[ctlApiConnectionsDtl]: A list of API connection detail objects, sorted by seq_no in ascending order.
        """
        query = (
            select(ctlApiConnectionsDtl)
            .where(ctlApiConnectionsDtl.pre_ingestion_dataset_id == dataset_id)
            .order_by(ctlApiConnectionsDtl.seq_no)
        )

        result = self.session.exec(query).all()
        return result

    def insert_ctl_api_connection_details(
        self, api_details: ctlApiConnectionsDtl
    ) -> None:
        """
        Inserts API connection details into the database using SQLAlchemy's session.

        Args:
            api_details (ctlApiConnectionsDtl): An instance of ctlApiConnectionsDtl containing the API connection details to be inserted.

        Returns:
            None: The function does not return a value.
        """
        self.session.add(api_details)
        self.session.commit()

    def get_ctl_data_acquisition_detail(
        self, process_id: int
    ) -> list[ctlDataAcquisitionDetail]:
        """
        Retrieve data acquisition details for a specific process from the database.

        This method queries the database to retrieve all `ctlDataAcquisitionDetail` records associated with the provided `process_id`.

        Args:
            process_id (int): The unique identifier of the process for which to retrieve data acquisition details.

        Returns:
            list[ctlDataAcquisitionDetail]: A list of `ctlDataAcquisitionDetail` objects matching the given `process_id`. Returns an empty list if no records are found.

        Raises:
            SQLAlchemyError: If a database error occurs during query execution.
        """
        query = select(ctlDataAcquisitionDetail).where(
            ctlDataAcquisitionDetail.process_id == process_id
        )

        result = self.session.exec(query).all()
        return result

    def insert_ctl_data_acquisition_connection_master(
        self, data_acquisition_connection_detail: ctlDataAcquisitionConnectionMaster
    ) -> None:
        """
        Insert a new data acquisition connection master record into the database.

        This method adds the provided `ctlDataAcquisitionConnectionMaster` object to the current
        database session and commits the transaction, persisting the connection details to the database.

        Args:
            data_acquisition_connection_detail (ctlDataAcquisitionConnectionMaster):
                An instance of the `ctlDataAcquisitionConnectionMaster` model containing
                the data acquisition connection information to be inserted.

        Returns:
            None
        """

        self.session.add(data_acquisition_connection_detail)
        self.session.commit()

    def get_ctl_data_acquisition_connection_master(
        self,
        outbound_source_platform: str,
        credentials_identifier: str,
    ) -> ctlDataAcquisitionConnectionMaster:
        """Retrieve a data acquisition connection master by outbound source platform and system.

        Args:
            outbound_source_platform (str): The platform of the outbound source.
            credentials_identifier (str): The system of the outbound source.

        Returns:
            ctlDataAcquisitionConnectionMaster: An instance of the matching data acquisition
                connection master, or None if no match is found.
        """
        query = select(ctlDataAcquisitionConnectionMaster).where(
            (
                ctlDataAcquisitionConnectionMaster.outbound_source_platform
                == outbound_source_platform
            )
            & (
                ctlDataAcquisitionConnectionMaster.credentials_identifier
                == credentials_identifier
            )
        )

        result = self.session.exec(query).first()
        return result

    def insert_ctl_data_acquisition_detail(
        self, data_acquisition: ctlDataAcquisitionDetail
    ) -> None:
        """
        Insert a new data acquisition detail record into the database.

        This method adds the provided `ctlDataAcquisitionDetail` object to the current database session
        and commits the transaction, persisting the data acquisition record to the database.

        Args:
            data_acquisition (ctlDataAcquisitionDetail): An instance of the `ctlDataAcquisitionDetail` model
                containing the data acquisition details to be inserted.

        Returns:
            None
        """

        self.session.add(data_acquisition)
        self.session.commit()

    def get_log_data_acquisition_detail(
        self,
        process_id: int,
        dataset_id: int,
        status: Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"],
    ) -> list[logDataAcquisitionDetail]:
        """
        Retrieves log details for data acquisition processes based on process ID, dataset ID, and status filter.

        Args:
            process_id (int): Unique identifier for the data acquisition process.
            dataset_id (int): Pre-ingestion dataset identifier associated with the log entry.
            status (Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"]): Filter by the current status of the data acquisition process.

        Returns:
            (list[logDataAcquisitionDetail]): List of log entries matching the specified criteria, containing details about data acquisition operations.

        Raises:
            SQLAlchemyError: If database operation fails during query execution.
        """
        query = select(logDataAcquisitionDetail).where(
            (logDataAcquisitionDetail.process_id == process_id)
            & (logDataAcquisitionDetail.pre_ingestion_dataset_id == dataset_id)
            & (logDataAcquisitionDetail.status == status)
        )

        result = self.session.exec(query).all()
        return result

    def insert_log_data_acquisition_detail(
        self, log_data_acquisition: logDataAcquisitionDetail
    ):
        self.session.add(log_data_acquisition)
        self.session.commit()

    def get_log_raw_process_dtl(
        self,
        process_id: int,
        dataset_id: int,
        status: Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"] = "SUCCEEDED",
    ) -> list[logRawProcessDtl]:
        """Retrieve log raw process details based on process ID, dataset ID, and status.

        Args:
            process_id (int): Unique identifier for the process.
            dataset_id (int): Unique identifier for the dataset.
            status (Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"], optional):
                Filter by status. Defaults to "SUCCEEDED".

        Returns:
            List of log raw process details objects, ordered by batch_id in ascending order. Filters records where file_status matches the specified status.

        Examples:
            >>> get_log_raw_process_dtl(process_id=123, dataset_id=456, status="FAILED")
        """
        query = (
            select(logRawProcessDtl)
            .where(
                (logRawProcessDtl.process_id == process_id)
                & (logRawProcessDtl.dataset_id == dataset_id)
                & (logRawProcessDtl.file_status == status)
            )
            .order_by(logRawProcessDtl.batch_id.asc())
        )

        result = self.session.exec(query).all()
        return result

    def insert_log_raw_process_detail(self, log_raw_process_dtl: logRawProcessDtl):
        self.session.add(log_raw_process_dtl)
        self.session.commit()

    def insert_dataset_master(self, dataset_master: ctlDatasetMaster):
        self.session.add(dataset_master)
        self.session.commit()

    def get_dataset_master(
        self,
        process_id: int,
        dataset_type: Literal["BRONZE", "SILVER", "GOLD"],
        dataset_id: Optional[int] = None,
    ) -> Union[list[ctlDatasetMaster], ctlDatasetMaster]:
        """Retrieve dataset master records based on process ID and dataset type.

        Args:
            process_id (int): The ID of the process associated with the dataset.
            dataset_type (Literal["BRONZE", "SILVER", "GOLD"]): The type of dataset to filter by.
            dataset_id (Optional[int], default=None): Optional dataset ID to fetch a specific record. If provided,
                a single record is returned; otherwise, all matching records are returned.

        Returns:
            A list of dataset master records if no dataset_id is provided, or a single dataset master record if dataset_id is specified. The results are ordered by dataset_id in ascending order.

        Note:
            Uses SQLAlchemy's select statement to query the ctlDatasetMaster table. Results are ordered by dataset_id.asc().
        """
        if dataset_id is not None:
            query = (
                select(ctlDatasetMaster)
                .where(
                    (ctlDatasetMaster.process_id == process_id)
                    & (ctlDatasetMaster.dataset_id == dataset_id)
                    & (ctlDatasetMaster.dataset_type == dataset_type)
                )
                .order_by(ctlDatasetMaster.dataset_id.asc())
            )
            result = self.session.exec(query).first()
            return result
        else:
            query = (
                select(ctlDatasetMaster)
                .where(
                    (ctlDatasetMaster.process_id == process_id)
                    & (ctlDatasetMaster.dataset_type == dataset_type)
                )
                .order_by(ctlDatasetMaster.dataset_id.asc())
            )
            result = self.session.exec(query).all()
            return result

    def get_data_standardisation_unprocessed_files(
        self, process_id: int, dataset_id: int
    ) -> list[logRawProcessDtl]:
        """Retrieve unprocessed files for data standardisation that have succeeded in raw processing.

        Args:
            process_id (int): Identifier for the process.
            dataset_id (int): Identifier for the dataset.

        Returns:
            (list[logRawProcessDtl]): List of logRawProcessDtl entries where the source_file
                has not been processed in data standardisation, and the raw processing
                status is "SUCCEEDED", filtered by the given process_id and dataset_id.
                Results are ordered by batch_id ascending.

        Description:
            This method queries for files that have completed raw processing successfully
            but have not yet been handled by data standardisation. It excludes files
            already marked as succeeded in the logDataStandardisationDtl table. The results
            are sorted by batch ID to ensure consistent ordering.
        """
        subquery = select(logDataStandardisationDtl.source_file).where(
            logDataStandardisationDtl.status == "SUCCEEDED"
        )

        query = (
            select(logRawProcessDtl)
            .filter(
                ~logRawProcessDtl.source_file.in_(subquery),
                logRawProcessDtl.process_id == process_id,
                logRawProcessDtl.dataset_id == dataset_id,
                logRawProcessDtl.file_status == "SUCCEEDED",
            )
            .order_by(logRawProcessDtl.batch_id.asc())
        )
        results = self.session.exec(query).all()
        return results

    def insert_data_standardisation_log(
        self, log_data_standardisation: logDataStandardisationDtl
    ):
        self.session.add(log_data_standardisation)
        self.session.commit()

    def get_data_standard_dtl(
        self,
        dataset_id: Optional[int] = None,
    ) -> list[ctlDataStandardisationDtl]:
        """Retrieve standardization details for a specific dataset.

        Queries the database to retrieve all records from the ctlDataStandardisationDtl table where the dataset_id matches the provided value. If no dataset_id is provided, returns all records.

        Args:
            dataset_id (Optional[int]): The ID of the dataset to filter results. Defaults to None.

        Returns:
            (list[ctlDataStandardisationDtl]): A list of standardization details objects matching the dataset_id.
        """
        query = select(ctlDataStandardisationDtl).where(
            ctlDataStandardisationDtl.dataset_id == dataset_id
        )
        result = self.session.exec(query).all()
        return result

    def get_dqm_unprocessed_files(
        self, process_id: int, dataset_id: int
    ) -> list[logDqmDtl]:
        """
        Retrieve a list of successfully processed data standardization files that have not yet been processed by DQM.

        This method queries the database to find entries from `logDataStandardisationDtl` that:
        - Belong to the specified `process_id` and `dataset_id`
        - Have a status of "SUCCEEDED"
        - Do not appear in the `logDqmDtl` table with a status of "SUCCEEDED"

        The results are ordered by ascending `batch_id`.

        Args:
            process_id (int): The ID of the process to filter files.
            dataset_id (int): The ID of the dataset to filter files.

        Returns:
            (List[logDqmDtl]): A list of log objects representing unprocessed files in the context of DQM.
        """

        subquery = select(logDqmDtl.source_file).where(logDqmDtl.status == "SUCCEEDED")

        query = (
            select(logDataStandardisationDtl)
            .filter(
                ~logDataStandardisationDtl.source_file.in_(subquery),
                logDataStandardisationDtl.process_id == process_id,
                logDataStandardisationDtl.dataset_id == dataset_id,
                logDataStandardisationDtl.status == "SUCCEEDED",
            )
            .order_by(logDataStandardisationDtl.batch_id.asc())
        )
        results = self.session.exec(query).all()
        return results

    def get_dqm_detail(self, process_id: int, dataset_id: int) -> list[ctlDqmMasterDtl]:
        """
        Retrieve DQM (Data Quality Management) master detail records for the specified process and dataset.

        This method queries the database to fetch all entries from `ctlDqmMasterDtl` that match the given
        `process_id` and `dataset_id`.

        Args:
            process_id (int): The unique identifier of the process.
            dataset_id (int): The unique identifier of the dataset.

        Returns:
            (List[ctlDqmMasterDtl]): A list of DQM master detail records matching the provided criteria.
        """
        query = select(ctlDqmMasterDtl).where(
            (ctlDqmMasterDtl.dataset_id == dataset_id)
            & (ctlDqmMasterDtl.process_id == process_id)
        )
        result = self.session.exec(query).all()
        return result

    def insert_log_dqm(self, log_dqm: logDqmDtl) -> None:
        """
        Insert a new DQM (Data Quality Management) log entry into the database.

        This method adds the provided `logDqmDtl` object to the current database session and commits the transaction,
        effectively persisting the log record to the database.

        Args:
            log_dqm (logDqmDtl): An instance of the `logDqmDtl` model containing the DQM log data to be inserted.

        Returns:
            None
        """

        self.session.add(log_dqm)
        self.session.commit()

    def get_transformation_dependency_master(
        self, process_id: int, dataset_id: int
    ) -> list[ctlTransformationDependencyMaster]:
        """
        Retrieve transformation dependency master records for the specified process and dataset.

        This method queries the database to fetch all entries from `ctlTransformationDependencyMaster`
        that match the provided `process_id` and `dataset_id`. The results are ordered by the
        `transformation_step` field in ascending order.

        Args:
            process_id (int): The ID of the process associated with the transformation dependencies.
            dataset_id (int): The ID of the dataset associated with the transformation dependencies.

        Returns:
            (List[ctlTransformationDependencyMaster]): A list of transformation dependency records
                                                       sorted by transformation step.
        """

        query = (
            select(ctlTransformationDependencyMaster)
            .where(
                (ctlTransformationDependencyMaster.process_id == process_id)
                & (ctlTransformationDependencyMaster.dataset_id == dataset_id)
            )
            .order_by(ctlTransformationDependencyMaster.transformation_step)
        )
        results = self.session.exec(query).all()
        return results

    def get_unprocessed_transformation_files(
        self, process_id, dataset_id
    ) -> list[logTransformationDtl]:
        """
        Retrieve a list of files that have not yet been successfully processed by the transformation step.

        This method identifies files in the `logDqmDtl` table that:
        - Belong to the specified `process_id` and `dataset_id`
        - Have not appeared in the `logTransformationDtl` table with a status of "SUCCEEDED"

        These files are considered unprocessed from a transformation perspective and may require further processing.

        Args:
            process_id (int): The ID of the process to filter files.
            dataset_id (int): The ID of the dataset to filter files.

        Returns:
            (list[logTransformationDtl]): A list of transformation detail objects representing unprocessed files.
        """

        subquery = select(logTransformationDtl.source_file).where(
            logTransformationDtl.status == "SUCCEEDED"
        )
        query = select(logDqmDtl).filter(
            ~logDqmDtl.source_file.in_(subquery),
            logDqmDtl.process_id == process_id,
            logDqmDtl.dataset_id == dataset_id,
        )
        results = self.session.exec(query).all()
        return results

    def insert_log_transformation(
        self, log_transformation: logTransformationDtl
    ) -> None:
        """
        Insert a new transformation log entry into the database.

        This method adds the provided `logTransformationDtl` object to the current database session
        and commits the transaction, persisting the transformation log record to the database.

        Args:
            log_transformation (logTransformationDtl): An instance of the `logTransformationDtl` model
                containing the transformation log data to be inserted.

        Returns:
            None
        """

        self.session.add(log_transformation)
        self.session.commit()

    def get_transformation_dqm_unprocessed_files(
        self, process_id, dataset_id
    ) -> list[logTransformationDtl]:
        """
        Retrieve transformation files that have not yet been successfully processed by DQM.

        This method identifies entries in the `logTransformationDtl` table that:
        - Belong to the specified `process_id` and `dataset_id`
        - Have a status of "SUCCEEDED"
        - Do not appear in the `logDqmDtl` table with a status of "SUCCEEDED" for the same dataset

        The results are ordered by ascending `batch_id`.

        Args:
            process_id (int): The ID of the process to filter transformation files.
            dataset_id (int): The ID of the dataset to filter transformation files.

        Returns:
            (List[logTransformationDtl]): A list of transformation log records that are unprocessed by DQM.
        """

        subquery = select(logDqmDtl.source_file).where(
            (logDqmDtl.status == "SUCCEEDED") & (logDqmDtl.dataset_id == dataset_id)
        )

        query = (
            select(logTransformationDtl)
            .filter(
                ~logTransformationDtl.source_file.in_(subquery),
                logTransformationDtl.process_id == process_id,
                logTransformationDtl.dataset_id == dataset_id,
                logTransformationDtl.status == "SUCCEEDED",
            )
            .order_by(logTransformationDtl.batch_id.asc())
        )
        results = self.session.exec(query).all()
        return results

    def get_gold_datasets(self) -> list[ctlDatasetMaster]:
        """
        Retrieve all GOLD-type datasets from the dataset master table.

        This method queries the `ctlDatasetMaster` table to fetch records where the
        `dataset_type` is "GOLD". The results are ordered by `dataset_id` in ascending order.

        Returns:
            (List[ctlDatasetMaster]): A list of dataset master records representing GOLD-type datasets.
        """
        query = (
            select(ctlDatasetMaster)
            .where(ctlDatasetMaster.dataset_type == "GOLD")
            .order_by(ctlDatasetMaster.dataset_id)
        )

        results = self.session.exec(query).all()
        return results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.session.close()