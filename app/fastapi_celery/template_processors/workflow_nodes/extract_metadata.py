import time
import logging
import traceback
from datetime import datetime, timezone
from utils import log_helpers, ext_extraction
from models.traceability_models import ServiceLog, LogType
from models.class_models import StatusEnum, StepOutput
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = f"Workflow Node - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def extract_metadata(self) -> bool:
    """Extracts metadata from a file and stores it in the instance.

    Attempts to process the file at `self.file_path` using `FileExtensionProcessor`
    to extract document type and file metadata. Logs errors if they occur.

    Returns:
        bool: True if metadata extraction is successful, False otherwise.
    """
    try:
        # === Try to retrieve all traceability attributes when an object created
        self.request_id = get_context_value("request_id")
        self.traceability_context_values = {
            key: val
            for key in ["workflow_name", "workflow_id", "document_number"]
            if (val := get_context_value(key)) is not None
        }
        logger.debug(
            f"Function: {__name__}\n"
            f"RequestID: {self.request_id}\n"
            f"TraceabilityContext: {self.traceability_context_values}"
        )

        file_processor = ext_extraction.FileExtensionProcessor(self.file_path)

        # Publish document_type for traceability
        self.document_type = file_processor.document_type
        self.target_bucket_name = file_processor.target_bucket_name
        self.file_record = {
            "file_path": self.file_path,
            "file_path_parent": file_processor.file_path_parent,
            "file_name": file_processor.file_name,
            "file_extension": file_processor.file_extension,
            "proceed_at": datetime.fromtimestamp(
                time.time_ns() / 1e9, timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }
        logger.info(
            f"Metadata of file {self.file_path}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "file_path": self.file_path,
                **self.traceability_context_values,
                "document_type": getattr(self, "document_type", None),
                "traceability": self.request_id,
            },
        )

        return StepOutput(
            output=True, step_status=StatusEnum.SUCCESS, step_failure_message=None
        )
    except FileNotFoundError as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[extract_metadata] File not found: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.ERROR,
                "file_path": self.file_path,
                **self.traceability_context_values,
                "document_type": getattr(self, "document_type", None),
                "traceability": self.request_id,
            },
            exc_info=True,
        )
    except ValueError as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[extract_metadata] Value error: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.ERROR,
                "file_path": self.file_path,
                **self.traceability_context_values,
                "document_type": getattr(self, "document_type", None),
                "traceability": self.request_id,
            },
            exc_info=True,
        )
    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[extract_metadata] Unexpected error: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.ERROR,
                "file_path": self.file_path,
                **self.traceability_context_values,
                "document_type": getattr(self, "document_type", None),
                "traceability": self.request_id,
            },
            exc_info=True,
        )

    return StepOutput(
        output=False,
        step_status=StatusEnum.FAILED,
        step_failure_message=[
            short_tb if "short_tb" in locals() else "Unknown error occurred."
        ],
    )
