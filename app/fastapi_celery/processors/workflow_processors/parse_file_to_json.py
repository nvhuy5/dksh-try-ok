import logging
import traceback
from utils import log_helpers
from models.class_models import StatusEnum, StepOutput
from app.fastapi_celery.models.tracking_models import ServiceLog, LogType
from processors.processor_registry import ProcessorRegistry
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = f"Workflow Processor - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


async def parse_file_to_json(self) -> StepOutput:  # pragma: no cover  # NOSONAR
    """Parses a file to JSON based on its document type and extension.

    Uses the appropriate processor from `POFileProcessorRegistry` or
    `MasterdataProcessorRegistry` based on `self.document_type` and file extension.
    Logs errors for unsupported types, extensions, or parsing failures.

    Returns:
        StepOutput: Parsed JSON data if successful, None otherwise.
    """
    try:
        # === Try to retrieve all traceability attributes when an object created
        self.request_id = get_context_value("request_id")
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

        # Handle document type processor for Master Data and PO Data
        processor_instance = await ProcessorRegistry.get_processor_for_file(self)
        json_data = processor_instance.parse_file_to_json()

        return StepOutput(
            output=json_data,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:  # pragma: no cover  # NOSONAR
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[parse_file_to_json] Failed to parse file: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.DOCUMENT_PARSER,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
            exc_info=True,
        )

        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[short_tb],
        )
