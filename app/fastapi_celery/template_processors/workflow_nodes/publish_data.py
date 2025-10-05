import logging
from utils import log_helpers
from models.class_models import GenericStepResult, StatusEnum, StepOutput

# ===
# Set up logging
logger_name = f"Workflow Node - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def publish_data(self):
    # This method is intentionally left blank.
    # Subclasses or future implementations should override this to provide validation logic.
    return StepOutput(
        output=GenericStepResult(
            step_status="2",
            message="publish failed due to missing required fields."
        ),
        step_status=StatusEnum.FAILED,
        step_failure_message=["publish failed due to missing required fields."]
    )
