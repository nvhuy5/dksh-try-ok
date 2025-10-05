import config_loader
from models.class_models import DocumentType, StepOutput, StatusEnum
from models.traceability_models import ServiceLog, LogType
from utils import log_helpers, read_n_write_s3
from pathlib import Path
import logging
import re
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = f"Workflow Node - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def write_raw_to_s3(self, source_file_path: str) -> StepOutput:
    # === Try to retrieve all traceability attributes when an object created
    self.request_id = get_context_value("request_id")
    self.project_name = get_context_value("project_name")
    self.sap_masterdata = get_context_value("sap_masterdata")
    self.traceability_context_values = {
        key: val
        for key in [
            "file_path",
            "workflow_name",
            "workflow_id",
            "document_number",
            "document_type",
        ]
        if (val := get_context_value(key)) is not None
    }
    logger.debug(
        f"Function: {__name__}\n"
        f"RequestID: {self.request_id}\n"
        f"TraceabilityContext: {self.traceability_context_values}"
    )

    # Check document type is Master data type
    source_s3_raw_master_data = get_source_bucket(self.document_type, self.project_name)
    destination_s3_raw_master_name = get_destination_bucket(self.project_name, self.sap_masterdata)
    file_name = Path(source_file_path).name
    stem = Path(source_file_path).stem
    destination_key = f"master_data/{stem}/{file_name}"
 
    try:
        logger.info(
            "Preparing to write raw master data to s3",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )
        result = read_n_write_s3.copy_object_between_buckets(
            source_bucket=source_s3_raw_master_data,
            source_key=source_file_path,
            dest_bucket=destination_s3_raw_master_name,
            dest_key=destination_key,
        )
           
        version_prefix = f"versioning/{stem}/"
        existing_keys = read_n_write_s3.list_objects_with_prefix(
            bucket_name= destination_s3_raw_master_name,
            prefix= version_prefix
        )
        version_number = 1
        if existing_keys:  # pragma: no cover  # NOSONAR
            numbers = []
            for key in existing_keys:
                match = re.search(rf"versioning/{re.escape(stem)}/(\d{{3}})/", key)
                if match:
                    numbers.append(int(match.group(1)))
            if numbers:
                version_number = max(numbers) + 1
 
        version_folder = f"{version_number:03d}"
        version_key = f"{version_prefix}{version_folder}/{file_name}"
 
        read_n_write_s3.copy_object_between_buckets(
        source_bucket=source_s3_raw_master_data,
        source_key=source_file_path,
        dest_bucket=destination_s3_raw_master_name,
        dest_key=version_key,
    )
 
        logger.info(
            "write_raw_to_s3 completed.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )
 
        return StepOutput(
            output=result,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
 
    except Exception as e:
        logger.error(
            f"Exception in write_raw_to_s3: {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
            exc_info=True,
            )
 
        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[f"Exception in write_raw_to_s3: {e}"],
        )


def get_source_bucket(document_type, project_name) -> str | None:
    """Resolve source bucket for master data based on project."""
    source_s3_raw_master_data = None
    if document_type == DocumentType.MASTER_DATA:
        if project_name and project_name.upper() == "DKSH_TW":
            source_s3_raw_master_data = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_tw"
            )
        elif project_name and project_name.upper() == "DKSH_VN":
            source_s3_raw_master_data = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_vn"
            )
        else:
            logger.warning(
                f"Unsupported project_name={project_name} for MASTER_DATA"
            )
    else:
        logger.warning(
            f"Unsupported document_type={document_type} with project_name={project_name}"
        )
    return source_s3_raw_master_data


def get_destination_bucket(project_name, sap_masterdata) -> str | None:
    """Resolve destination bucket based on project or sap_masterdata flag."""
    destination_s3_raw_master_name = None
    if sap_masterdata:
        destination_s3_raw_master_name = config_loader.get_config_value(
            "s3_buckets", "sap_masterdata_bucket"
        )
    elif project_name and project_name.upper() == "DKSH_TW":
        destination_s3_raw_master_name = config_loader.get_config_value(
            "s3_buckets", "tw_masterdata_bucket"
        )
    elif project_name and project_name.upper() == "DKSH_VN":
        destination_s3_raw_master_name = config_loader.get_config_value(
            "s3_buckets", "vn_masterdata_bucket"
        )
    else:
        logger.warning(
            f"Unsupported project_name={project_name}, sap_masterdata={sap_masterdata}"
        )
    return destination_s3_raw_master_name
