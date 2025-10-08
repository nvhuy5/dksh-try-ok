import logging
import traceback
from utils import log_helpers
from models.class_models import StatusEnum, StepOutput
from models.tracking_models import ServiceLog, LogType
from processors.processor_registry import ProcessorRegistry

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
                "data": self.tracking_model,
            },
            exc_info=True,
        )

        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[short_tb],
        )
