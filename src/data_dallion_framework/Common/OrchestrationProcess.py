from pathlib import Path
from json import loads
from threading import Lock

from pydantic import computed_field, model_validator, Field
from pydantic_settings import BaseSettings
from typing import Literal, Optional, Union
from sqlmodel import SQLModel, create_engine, Session, select
from sqlalchemy import Engine
from sqlalchemy.orm import sessionmaker

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
from data_dallion_framework.Common.Models.TransformationDependencyMaster import (
    ctlTransformationDependencyMaster,
)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class BackendSettings(BaseSettings):
    """Configure backend database settings for the datacraft framework."""

    sqlalchemy_url: Optional[str] = Field(default=None, alias="database_url")
    database_type: Literal["mysql", "postgresql", "sqlite", "mariadb"] = Field(
        default="sqlite", alias="db_type"
    )
    database: str = Field(default="nextgen_framework_configuration", alias="db_name")
    connect_args: Optional[str] = Field(default=None, alias="db_connect_args")
    user: Optional[str] = Field(default=None, alias="db_user")
    password: Optional[str] = Field(default=None, alias="db_password")
    hostname: Optional[str] = Field(default="localhost", alias="db_host")
    port: Optional[int] = Field(default=None, alias="db_port")
    datacraft_framework_home: Optional[str] = Field(
        default=str(Path.home() / "datacraft_framework")
    )

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
        if self.sqlalchemy_url:
            return self.sqlalchemy_url
        if self.database_type == "mysql":
            return f"mysql+pymysql://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        elif self.database_type == "postgresql":
            return f"postgresql+psycopg2://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        elif self.database_type == "sqlite":
            db_path = Path(self.datacraft_framework_home) / f"{self.database}.db"
            return f"sqlite:///{db_path}"
        elif self.database_type == "mariadb":
            return f"mariadb+mariadbconnector://{self.user}:{self.password}@{self.hostname}:{self.port}/{self.database}"
        raise ValueError(f"Unsupported database type: {self.database_type}")


# ---------------------------------------------------------------------------
# Engine singleton — created once, shared across all OrchestrationProcess
# instances. Thread-safe via double-checked locking.
# ---------------------------------------------------------------------------

_engine: Optional[Engine] = None
_SessionFactory: Optional[sessionmaker] = None
_engine_lock = Lock()


def _get_session_factory() -> sessionmaker:
    """
    Return the module-level session factory, initialising the engine and
    schema exactly once across the entire process lifetime.
    """
    global _engine, _SessionFactory

    if _SessionFactory is None:
        with _engine_lock:
            if _SessionFactory is None:  # second check inside the lock
                settings = BackendSettings()
                connect_args = loads(settings.connect_args) if settings.connect_args else {}
                _engine = create_engine(settings.connection_string, connect_args=connect_args)
                SQLModel.metadata.create_all(bind=_engine)
                _SessionFactory = sessionmaker(bind=_engine, class_=Session)

    return _SessionFactory


# ---------------------------------------------------------------------------
# OrchestrationProcess
# ---------------------------------------------------------------------------

class OrchestrationProcess:
    """
    Thin context-manager wrapper around a SQLModel session.

    The heavy work (engine creation, schema init) is done once at the module
    level, so repeated instantiation is cheap — just opening a session.

    Usage::

        with OrchestrationProcess() as orch:
            results = orch.get_dataset_master(process_id=1, dataset_type="BRONZE")
    """

    def __init__(self) -> None:
        self.session: Session = _get_session_factory()()

    # ------------------------------------------------------------------
    # Context-manager protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "OrchestrationProcess":
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        # Only close the *session* — the engine is shared and must not be
        # disposed here.
        self.session.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _add_and_commit(self, obj) -> None:
        """Persist a single ORM object and commit."""
        self.session.add(obj)
        self.session.commit()

    # ------------------------------------------------------------------
    # Column metadata
    # ------------------------------------------------------------------

    def get_ctl_column_metadata(self, dataset_id: int) -> list[CtlColumnMetadata]:
        return self.session.exec(
            select(CtlColumnMetadata)
            .where(CtlColumnMetadata.dataset_id == dataset_id)
            .order_by(CtlColumnMetadata.column_sequence_number)
        ).all()

    def insert_ctl_column_metadata(self, column_metadata: CtlColumnMetadata) -> None:
        self._add_and_commit(column_metadata)

    # ------------------------------------------------------------------
    # API connections
    # ------------------------------------------------------------------

    def get_ctl_api_connection_details(self, dataset_id: int) -> list[ctlApiConnectionsDtl]:
        return self.session.exec(
            select(ctlApiConnectionsDtl)
            .where(ctlApiConnectionsDtl.pre_ingestion_dataset_id == dataset_id)
            .order_by(ctlApiConnectionsDtl.seq_no)
        ).all()

    def insert_ctl_api_connection_details(self, api_details: ctlApiConnectionsDtl) -> None:
        self._add_and_commit(api_details)

    # ------------------------------------------------------------------
    # Data acquisition
    # ------------------------------------------------------------------

    def get_ctl_data_acquisition_detail(
        self,
        process_id: int,
        pre_ingestion_dataset_id: Optional[int] = None,
    ) -> Union[list[ctlDataAcquisitionDetail], ctlDataAcquisitionDetail]:
        query = select(ctlDataAcquisitionDetail).where(
            ctlDataAcquisitionDetail.process_id == process_id
        )
        if pre_ingestion_dataset_id is None:
            return self.session.exec(query).all()

        query = query.where(
            ctlDataAcquisitionDetail.pre_ingestion_dataset_id == pre_ingestion_dataset_id
        )
        return self.session.exec(query).first()

    def insert_ctl_data_acquisition_detail(
        self, data_acquisition: ctlDataAcquisitionDetail
    ) -> None:
        self._add_and_commit(data_acquisition)

    def get_ctl_data_acquisition_connection_master(
        self,
        outbound_source_platform: str,
        credentials_identifier: str,
    ) -> ctlDataAcquisitionConnectionMaster:
        return self.session.exec(
            select(ctlDataAcquisitionConnectionMaster).where(
                ctlDataAcquisitionConnectionMaster.outbound_source_platform == outbound_source_platform,
                ctlDataAcquisitionConnectionMaster.credentials_identifier == credentials_identifier,
            )
        ).first()

    def insert_ctl_data_acquisition_connection_master(
        self, data_acquisition_connection_detail: ctlDataAcquisitionConnectionMaster
    ) -> None:
        self._add_and_commit(data_acquisition_connection_detail)

    # ------------------------------------------------------------------
    # Acquisition logs
    # ------------------------------------------------------------------

    def get_log_data_acquisition_detail(
        self,
        process_id: int,
        dataset_id: int,
        status: Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"],
    ) -> list[logDataAcquisitionDetail]:
        return self.session.exec(
            select(logDataAcquisitionDetail).where(
                logDataAcquisitionDetail.process_id == process_id,
                logDataAcquisitionDetail.pre_ingestion_dataset_id == dataset_id,
                logDataAcquisitionDetail.status == status,
            )
        ).all()

    def insert_log_data_acquisition_detail(
        self, log_data_acquisition: logDataAcquisitionDetail
    ) -> None:
        self._add_and_commit(log_data_acquisition)

    # ------------------------------------------------------------------
    # Raw process logs
    # ------------------------------------------------------------------

    def get_log_raw_process_dtl(
        self,
        process_id: int,
        dataset_id: int,
        status: Literal["SUCCEEDED", "FAILED", "IN-PROGRESS"] = "SUCCEEDED",
    ) -> list[logRawProcessDtl]:
        return self.session.exec(
            select(logRawProcessDtl)
            .where(
                logRawProcessDtl.process_id == process_id,
                logRawProcessDtl.dataset_id == dataset_id,
                logRawProcessDtl.file_status == status,
            )
            .order_by(logRawProcessDtl.batch_id.asc())
        ).all()

    def insert_log_raw_process_detail(self, log_raw_process_dtl: logRawProcessDtl) -> None:
        self._add_and_commit(log_raw_process_dtl)

    # ------------------------------------------------------------------
    # Dataset master
    # ------------------------------------------------------------------

    def insert_dataset_master(self, dataset_master: ctlDatasetMaster) -> None:
        self._add_and_commit(dataset_master)

    def get_dataset_master(
        self,
        process_id: int,
        dataset_type: Literal["BRONZE", "SILVER", "GOLD"],
        dataset_id: Optional[int] = None,
    ) -> Union[list[ctlDatasetMaster], ctlDatasetMaster]:
        query = (
            select(ctlDatasetMaster)
            .where(
                ctlDatasetMaster.process_id == process_id,
                ctlDatasetMaster.dataset_type == dataset_type,
            )
            .order_by(ctlDatasetMaster.dataset_id.asc())
        )
        if dataset_id is not None:
            query = query.where(ctlDatasetMaster.dataset_id == dataset_id)
            return self.session.exec(query).first()
        return self.session.exec(query).all()

    def get_gold_datasets(self) -> list[ctlDatasetMaster]:
        return self.session.exec(
            select(ctlDatasetMaster)
            .where(ctlDatasetMaster.dataset_type == "GOLD")
            .order_by(ctlDatasetMaster.dataset_id)
        ).all()

    # ------------------------------------------------------------------
    # Data standardisation
    # ------------------------------------------------------------------

    def get_data_standardisation_unprocessed_files(
        self, process_id: int, dataset_id: int
    ) -> list[logRawProcessDtl]:
        already_done = select(logDataStandardisationDtl.source_file).where(
            logDataStandardisationDtl.status == "SUCCEEDED"
        )
        return self.session.exec(
            select(logRawProcessDtl)
            .filter(
                ~logRawProcessDtl.source_file.in_(already_done),
                logRawProcessDtl.process_id == process_id,
                logRawProcessDtl.dataset_id == dataset_id,
                logRawProcessDtl.file_status == "SUCCEEDED",
            )
            .order_by(logRawProcessDtl.batch_id.asc())
        ).all()

    def insert_data_standardisation_log(
        self, log_data_standardisation: logDataStandardisationDtl
    ) -> None:
        self._add_and_commit(log_data_standardisation)

    def get_data_standard_dtl(
        self, dataset_id: Optional[int] = None
    ) -> list[ctlDataStandardisationDtl]:
        return self.session.exec(
            select(ctlDataStandardisationDtl).where(
                ctlDataStandardisationDtl.dataset_id == dataset_id
            )
        ).all()

    # ------------------------------------------------------------------
    # DQM
    # ------------------------------------------------------------------

    def get_dqm_unprocessed_files(
        self, process_id: int, dataset_id: int
    ) -> list[logDataStandardisationDtl]:
        already_done = select(logDqmDtl.source_file).where(logDqmDtl.status == "SUCCEEDED")
        return self.session.exec(
            select(logDataStandardisationDtl)
            .filter(
                ~logDataStandardisationDtl.source_file.in_(already_done),
                logDataStandardisationDtl.process_id == process_id,
                logDataStandardisationDtl.dataset_id == dataset_id,
                logDataStandardisationDtl.status == "SUCCEEDED",
            )
            .order_by(logDataStandardisationDtl.batch_id.asc())
        ).all()

    def get_dqm_detail(self, process_id: int, dataset_id: int) -> list[ctlDqmMasterDtl]:
        return self.session.exec(
            select(ctlDqmMasterDtl).where(
                ctlDqmMasterDtl.dataset_id == dataset_id,
                ctlDqmMasterDtl.process_id == process_id,
            )
        ).all()

    def insert_log_dqm(self, log_dqm: logDqmDtl) -> None:
        self._add_and_commit(log_dqm)

    # ------------------------------------------------------------------
    # Transformation
    # ------------------------------------------------------------------

    def get_transformation_dependency_master(
        self, process_id: int, dataset_id: int
    ) -> list[ctlTransformationDependencyMaster]:
        return self.session.exec(
            select(ctlTransformationDependencyMaster)
            .where(
                ctlTransformationDependencyMaster.process_id == process_id,
                ctlTransformationDependencyMaster.dataset_id == dataset_id,
            )
            .order_by(ctlTransformationDependencyMaster.transformation_sequence)
        ).all()

    def get_unprocessed_transformation_files(
        self, process_id: int, dataset_id: int
    ) -> list[logDqmDtl]:
        already_done = select(logTransformationDtl.source_file).where(
            logTransformationDtl.status == "SUCCEEDED"
        )
        return self.session.exec(
            select(logDqmDtl).filter(
                ~logDqmDtl.source_file.in_(already_done),
                logDqmDtl.process_id == process_id,
                logDqmDtl.dataset_id == dataset_id,
            )
        ).all()

    def insert_log_transformation(self, log_transformation: logTransformationDtl) -> None:
        self._add_and_commit(log_transformation)

    def get_transformation_dqm_unprocessed_files(
        self, process_id: int, dataset_id: int
    ) -> list[logTransformationDtl]:
        already_done = select(logDqmDtl.source_file).where(
            logDqmDtl.status == "SUCCEEDED",
            logDqmDtl.dataset_id == dataset_id,
        )
        return self.session.exec(
            select(logTransformationDtl)
            .filter(
                ~logTransformationDtl.source_file.in_(already_done),
                logTransformationDtl.process_id == process_id,
                logTransformationDtl.dataset_id == dataset_id,
                logTransformationDtl.status == "SUCCEEDED",
            )
            .order_by(logTransformationDtl.batch_id.asc())
        ).all()