import logging
import json
import re
from typing import List, Dict, Any
import pandas as pd

from utils import log_helpers
from connections.be_connection import BEConnector
from models.class_models import (
    ApiUrl,
    DocumentType,
    StatusEnum,
    StepOutput,
    PODataParsed,
)
from models.tracking_models import ServiceLog, LogType
from utils.middlewares.request_context import get_context_value

# === logging setup ===
logger_name = f"Workflow Processor - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})


class TemplateValidation:
    """
    Perform template data validation for PODataParsed against schema from API.
    """

    def __init__(self, po_json: PODataParsed):
        self.po_json = po_json
        self.items = po_json.items if isinstance(po_json.items, list) else [po_json.items]

        self.request_id = get_context_value("request_id")
        self.traceability_context_values = {
            key: val
            for key in ["file_path", "workflow_name", "workflow_id", "document_number"]
            if (val := get_context_value(key)) is not None
        }

    def _check_required(self, val: Any, required: bool, allow_empty: bool, col_key: str, idx: int) -> str | None:
        if required and not allow_empty and (val is None or str(val).strip() == ""):
            return f"Row {idx}: Field '{col_key}' is required but empty"
        return None

    def _check_max_length(self, val: Any, max_length: int | None, col_key: str, idx: int) -> str | None:
        if max_length and len(str(val)) > int(max_length):
            return f"Row {idx}: Field '{col_key}' exceeds maxLength {max_length}"
        return None

    def _check_regex(self, val: Any, regex: str | None, col_key: str, idx: int) -> str | None:
        if regex and not re.fullmatch(regex, str(val)):
            return f"Row {idx}: Field '{col_key}'='{val}' does not match regex {regex}"
        return None

    def _check_dtype(self, val: Any, dtype: str | None, col_key: str, idx: int) -> str | None:
        if dtype == "Number" and not re.fullmatch(r"-?\d+(\.\d+)?", str(val)):
            return f"Row {idx}: Field '{col_key}'='{val}' is not a valid number"
        if dtype == "Date":
            try:
                pd.to_datetime(val, errors="raise")
            except Exception:
                return f"Row {idx}: Field '{col_key}'='{val}' is not a valid date"
        return None

    def _validate_cell(self, val: Any, col_def: Dict[str, Any], col_key: str, idx: int) -> List[str]:
        metadata = json.loads(col_def.get("metadata", "{}"))
        errors = []

        # Required check
        err = self._check_required(val, metadata.get("required", False), metadata.get("allowEmpty", True), col_key, idx)
        if err:
            errors.append(err)
            return errors  # nếu required fail thì bỏ qua check khác

        if val is None or str(val).strip() == "":
            return errors  # empty allowed → skip

        # Max length check
        if (err := self._check_max_length(val, metadata.get("maxLength"), col_key, idx)):
            errors.append(err)

        # Regex check
        if (err := self._check_regex(val, metadata.get("regex"), col_key, idx)):
            errors.append(err)

        # Data type check
        if (err := self._check_dtype(val, col_def.get("dataType"), col_key, idx)):
            errors.append(err)

        return errors

    def data_validation(self, schema_columns: List[Dict[str, Any]]) -> PODataParsed:
        """
        Validate PO items against schema columns definition.
        """
        errors = []

        for col_def in schema_columns:
            field_name = col_def.get("order")
            col_key = f"col_{field_name}"

            for idx, row in enumerate(self.items, start=2):
                val = row.get(col_key)
                errors.extend(self._validate_cell(val, col_def, col_key, idx))

        if errors:
            logger.error(
                f"Template format validation failed with {len(errors)} error(s)",
                extra=self._log_extra(log_type=LogType.ERROR),
            )
            return self.po_json.model_copy(
                update={"step_status": StatusEnum.FAILED, "messages": errors}
            )

        logger.info(
            f"{__name__} successfully executed!",
            extra=self._log_extra(log_type=LogType.TASK),
        )
        return self.po_json.model_copy(
            update={"step_status": StatusEnum.SUCCESS, "messages": None}
        )

    def _log_extra(self, log_type: LogType) -> dict:
        return {
            "service": ServiceLog.METADATA_VALIDATION,
            "log_type": log_type,
            **self.traceability_context_values,
            "document_type": DocumentType.ORDER,
            "traceability": self.request_id,
        }


# === orchestration ===
async def template_format_validation(self, input_data: StepOutput) -> StepOutput:
    request_id = get_context_value("request_id")
    traceability_context_values = {
        key: val
        for key in ["file_path", "workflow_name", "workflow_id", "document_number"]
        if (val := get_context_value(key)) is not None
    }

    # Step 1: call template-parse API
    template_parse_resp = await BEConnector(
        ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
        params={"workflowStepId": self.workflow_step_ids.get("TEMPLATE_FILE_PARSE")},
    ).get()

    if not template_parse_resp or not isinstance(template_parse_resp, list):
        logger.error(
            "Failed to call template-parse API",
            extra={
                "service": ServiceLog.VALIDATION,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "document_type": DocumentType.ORDER,
                "traceability": request_id,
            },
        )
        failed_output = input_data.output.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": ["Failed to call template-parse API"]}
        )
        return StepOutput(
            output=failed_output,
            step_status=StatusEnum.FAILED,
            step_failure_message=failed_output.messages,
        )

    template_id = template_parse_resp[0]["templateFileParse"]["id"]

    # Step 2: call template-format-validation API
    format_validation_full_url = f"{ApiUrl.TEMPLATE_FORMAT_VALIDATION.full_url()}/{template_id}"
    validate_resp = await BEConnector(format_validation_full_url).get()

    schema_columns = []
    if isinstance(validate_resp, dict):
        if "data" in validate_resp and isinstance(validate_resp["data"], dict):
            schema_columns = validate_resp["data"].get("columns", [])
        elif "columns" in validate_resp:
            schema_columns = validate_resp.get("columns", [])

    if not schema_columns:
        logger.error(
            "Schema columns not found in API response",
            extra={
                "service": ServiceLog.VALIDATION,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "document_type": DocumentType.ORDER,
                "traceability": request_id,
            },
        )
        failed_output = input_data.output.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": ["Schema columns not found in API response"]}
        )
        return StepOutput(
            output=failed_output,
            step_status=StatusEnum.FAILED,
            step_failure_message=failed_output.messages,
        )

    # Step 3: run validation
    po_validation = TemplateValidation(po_json=input_data.output)
    validation_result = po_validation.data_validation(schema_columns=schema_columns)

    # Step 4: wrap into StepOutput
    return StepOutput(
        output=validation_result,
        step_status=(
            StatusEnum.SUCCESS
            if validation_result.step_status == StatusEnum.SUCCESS
            else StatusEnum.FAILED
        ),
        step_failure_message=(
            None if validation_result.step_status == StatusEnum.SUCCESS else validation_result.messages
        ),
    )
