import sys
import asyncio
import logging
import json
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, List
import contextvars
from celery import shared_task
from celery.exceptions import Retry, MaxRetriesExceededError
from pydantic import BaseModel

from .step_handler import execute_step, raise_if_failed
from connections.be_connection import BEConnector
from connections.redis_connection import RedisConnector
from template_processors.file_processor import FileProcessor, STEP_DEFINITIONS
from template_processors.common import template_utils
from models.class_models import (
    WorkflowModel,
    ApiUrl,
    StatusEnum,
    WorkflowSession,
    StartStep,
    DocumentType,
    StepOutput,
    StepDefinition,
)
from models.traceability_models import ServiceLog, LogType
from utils import log_helpers, read_n_write_s3
import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values
from utils.ext_extraction import FileExtensionProcessor
from datetime import datetime

# === Setup logging ===
logger_name = "Celery Task Execution"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})

# === Load config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))
types_list = json.loads(config_loader.get_config_value("support_types", "types"))


@shared_task(bind=True, retry_kwargs={"max_retries": 3})
def task_execute(
    self,
    file_path: str,
    celery_id: str,
    project_name: str,
    source: str,
    rerun_attempt: Optional[int] = None,
) -> str:
    """
    Entry point Celery task (sync). Internally executes async logic using asyncio.run.
    Celery task entry point (synchronous wrapper).

    Args:
        self: The Celery task instance.
        file_path (str): The path to the file to be processed.
        celery_id (str): The Celery task ID for logging and tracking.

    Returns:
        str: Status message indicating success or failure of the task.
    """
    try:
        # Initialize the traceability context
        set_context_values(
            request_id=celery_id,
            file_path=file_path,
            project_name=project_name,
            source=source,
        )
        # Copy context and run asyncio task inside it
        ctx = contextvars.copy_context()
        ctx.run(lambda: asyncio.run(handle_task(file_path, celery_id, rerun_attempt)))
        # ===
        traceability_context_values = {
            key: val
            for key in [
                "workflow_name",
                "workflow_id",
                "document_number",
                "document_type",
            ]
            if (val := get_context_value(key)) is not None
        }

        logger.info(
            f"[{celery_id}] Starting task execution (sync wrapper)",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.TASK,
                "file_path": file_path,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )
        return "Task completed successfully"
    except Retry:
        logger.warning(f"[{celery_id}] Task is retrying...")
        raise
    except Exception as e:
        retry_count = self.request.retries
        max_retries = self.max_retries or 3
        # ===
        traceability_context_values = {
            key: val
            for key in [
                "workflow_name",
                "workflow_id",
                "document_number",
                "document_type",
            ]
            if (val := get_context_value(key)) is not None
        }

        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[{celery_id}] Task execution failed (attempt {retry_count}/{max_retries}): {e}\n{short_tb}",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.ERROR,
                "file_path": file_path,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )
        try:
            raise self.retry(exc=e, countdown=5)
        except MaxRetriesExceededError:
            logger.critical(
                f"[{celery_id}] Maximum retries exceeded for task.",
                extra={
                    "service": ServiceLog.TASK_EXECUTION,
                    "log_type": LogType.ERROR,
                    "file_path": file_path,
                    **traceability_context_values,
                    "traceability": celery_id,
                },
            )
            raise


async def handle_task(
    file_path: str, celery_id: str, rerun_attempt: Optional[int] = None
) -> Dict[str, Any]:
    """
    Asynchronously handles the file processing workflow.

    Args:
        file_path (str): The path to the file to be processed.
        celery_id (str): The Celery task ID for logging and tracking.

    Returns:
        Dict[str, Any]: This function performs processing and return a dict of value extracted.
    """
    # === Initialization & metadata extraction ===
    redis_utils, file_processor, context, request_id, traceability_context_values, body_data = await _init_context_and_metadata(
        file_path, celery_id, rerun_attempt
    )
 
    # === Fetch workflow and update context ===
    workflow_model = await _fetch_workflow_and_set_context(
        file_processor, celery_id, file_path, traceability_context_values, body_data, context
    )
    if not workflow_model:
        return context
    
    # === Start workflow session ===
    workflow_session = await _start_workflow_session(
        workflow_model, celery_id, file_path, context, request_id
    )
    if not workflow_session:
        return context   # session fail → stop here
    
    # Update Redis for WorkflowID and TaskID to track
    redis_utils.store_workflow_id(
        task_id=celery_id, workflow_id=workflow_model.id, status=StatusEnum.PROCESSING
    )

    # === Process all workflow steps ===
    step_names = await _process_workflow_steps(
        workflow_model,
        workflow_session,
        file_processor,
        celery_id,
        rerun_attempt,
        context,
        redis_utils,
        request_id,
        traceability_context_values,
        file_path,
    )
    
    # === Finish workflow session ===
    await _finish_workflow(
        workflow_model,
        workflow_session,
        file_processor,
        celery_id,
        context,
        step_names,
        request_id,
        file_path,
        redis_utils
    )

    return context

# -------------------------------------------------------------
#                      Helper functions
# -------------------------------------------------------------

async def _init_context_and_metadata(file_path, celery_id, rerun_attempt):
    logger.info(
        f"[{celery_id}] Start processing file: {file_path}"
        + (f" | Rerun attempt: {rerun_attempt}" if rerun_attempt is not None else ""),
        extra={
            "service": ServiceLog.TASK_EXECUTION,
            "log_type": LogType.TASK,
            "file_path": file_path,
            "workflow_id": None,
            "workflow_name": None,
            "document_number": None,
            "document_type": None,
            "traceability": celery_id,
        },
    )
    redis_utils = RedisConnector()
    file_processor = FileProcessor(file_path=file_path)
    file_processor.extract_metadata()

    request_id = get_context_value("request_id")
    traceability_context_values = {
        key: val
        for key in ["workflow_name", "workflow_id", "document_number", "document_type"]
        if (val := get_context_value(key)) is not None
    }
    logger.debug(
        f"Function: {__name__}\n"
        f"RequestID: {request_id}\n"
        f"TraceabilityContext: {traceability_context_values}"
    )

    body_data = {
        "filePath": file_processor.file_record["file_path_parent"],
        "fileName": file_processor.file_record["file_name"],
        "fileExtension": file_processor.file_record["file_extension"],
        "project": get_context_value("project_name"),
        "source": get_context_value("source"),
    }
    logger.info(
        f"File path ({file_path}) parsed result:\n{body_data}\n"
        f"The document type: {file_processor.document_type}",
        extra={
            "service": ServiceLog.DOCUMENT_PARSER,
            "log_type": LogType.TASK,
            "file_path": file_path,
            **traceability_context_values,
            "traceability": celery_id,
        },
    )
    context = {
        "input_data": None,
        "file_path": file_path,
        "celery_id": celery_id,
    }
    return redis_utils, file_processor, context, request_id, traceability_context_values, body_data


async def _fetch_workflow_and_set_context(file_processor, celery_id, file_path, traceability_context_values, body_data, context):
    workflow = BEConnector(ApiUrl.WORKFLOW_FILTER.full_url(), body_data=body_data)
    workflow_response = await workflow.post()
    if not workflow_response:
        logger.error(
            f"[{celery_id}] Failed to fetch workflow for file: {file_path}",
            extra={
                "service": ServiceLog.DATABASE,
                "log_type": LogType.ERROR,
                "file_path": file_path,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )
        return
    logger.info(
        f"Workflow details:\n{workflow_response}",
        extra={
            "service": ServiceLog.DATABASE,
            "log_type": LogType.ACCESS,
            "file_path": file_path,
            **traceability_context_values,
            "traceability": celery_id,
        },
    )
    workflow_model = WorkflowModel(**workflow_response)
    # Save workflow_detail to context for use in output
    context["step_detail"] = []
    context["workflow_detail"] = {
        "filter_api": workflow_response,
        "metadata_api": [],
    }

    # Log raw model field
    logger.info(
        f"[DEBUG] API returned sapMasterData from model: "
        f"{workflow_model.sapMasterData} -> set to {bool(workflow_model.sapMasterData)}"
    )
    set_context_values(
        workflow_name=workflow_model.name,
        workflow_id=workflow_model.id,
        sap_masterdata=bool(workflow_model.sapMasterData),
    )
    file_processor.target_bucket_name = FileExtensionProcessor._get_target_bucket(
        file_processor.document_type
    )
    return workflow_model

async def _start_workflow_session(workflow_model, celery_id, file_path, context, request_id):
    # === Start session ===
    traceability_context_values = {
        key: val
        for key in ["workflow_name", "workflow_id", "document_number", "document_type"]
        if (val := get_context_value(key)) is not None
    }
    logger.debug(
        "Start session updates...:\n"
        f"Function: {__name__}\n"
        f"RequestID: {request_id}\n"
        f"TraceabilityContext: {traceability_context_values}"
    )

    session_connector = BEConnector(
        ApiUrl.WORKFLOW_SESSION_START.full_url(),
        {
            "workflowId": workflow_model.id,
            "celeryId": celery_id,
            "filePath": file_path,
        },
    )
    start_session_response = await session_connector.post()
    if not start_session_response:
        logger.error(
            f"[{celery_id}] Failed to create workflow session.",
            extra={
                "service": ServiceLog.DATABASE,
                "log_type": LogType.ERROR,
                "file_path": file_path,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )
        return
    logger.info(
        f"Session details:\n{start_session_response}",
        extra={
            "service": ServiceLog.DATABASE,
            "log_type": LogType.ACCESS,
            "file_path": file_path,
            **traceability_context_values,
            "traceability": celery_id,
        },
    )
    workflow_session = WorkflowSession(**start_session_response)

    # Save workflow_detail to context for use in output
    context["workflow_detail"]["metadata_api"].append(
        {
            "url": "/api/workflow/session/start",
            "request": {
                "workflowId": workflow_model.id,
                "celeryId": celery_id,
                "filePath": file_path,
            },
            "response": start_session_response,
        }
    )
    return workflow_session


async def _process_workflow_steps(
    workflow_model,
    workflow_session,
    file_processor,
    celery_id,
    rerun_attempt,
    context,
    redis_utils,
    request_id,
    traceability_context_values,
    file_path,
):
    # === Process steps ===
    # Sort steps in ascending order by stepOrder
    sorted_steps = sorted(workflow_model.workflowSteps, key=lambda step: step.stepOrder)
    step_names = [step.stepName for step in sorted_steps]
    for step in sorted_steps:
        # 1. Log step start
        _log_step_start(step, celery_id, context["file_path"], traceability_context_values)

        # 2. Prepare step configuration (S3 path, workflow_step_ids, etc.)
        step_config, s3_key_prefix = _prepare_step_config(step, file_processor, celery_id, context)
        
        # 3. Resolve metadata API and get config API data
        await _resolve_step_api(step, file_processor, context, step_config)

        # Start step
        # Start updating Redis for StepID to track
        logger.info(f"Start tracking StepID: {step.workflowStepId}...")
        redis_utils.store_step_status(
            task_id=celery_id,
            step_name=step.stepName,
            status="InProgress",
            step_id=step.workflowStepId,
        )
        step_response = await BEConnector(
            ApiUrl.WORKFLOW_STEP_START.full_url(),
            {"sessionId": workflow_session.id, "stepId": step.workflowStepId},
        ).post()

        # 4. Execute step, handle result, finish step, save metadata and S3 output
        await _execute_and_finish_step(
                step,
                workflow_model,
                workflow_session,
                file_processor,
                celery_id,
                rerun_attempt,
                context,
                redis_utils,
                step_config,
                s3_key_prefix,
                request_id,
                step_response,
                file_path
            )

    return step_names


def _log_step_start(step, celery_id, file_path, traceability_context_values):
    logger.info(
        f"[{celery_id}] Starting step: {step.stepName}",
        extra={
            "service": ServiceLog.DATA_TRANSFORM,
            "log_type": LogType.TASK,
            "file_path": file_path,
            **traceability_context_values,
            "traceability": celery_id,
        },
    )


def build_s3_key_prefix(materialized_step_data_loc: str, celery_id: str, step_name: str, workflow_detail) -> str:

    """
    Build S3 prefix for both Processor workflow and Master Data workflow.

    - Processor workflow: {materialized_step_data_loc}/{folderName}/{customerFolderName}/{yyyyMMdd}/{celery_id}/{step_name}
    - Master data workflow: {materialized_step_data_loc}/{fileName}/{yyyyMMdd}/{celery_id}/{step_name}
    """
    filter_api = workflow_detail.get("filter_api", {})
    metadata_api = workflow_detail.get("metadata_api", [])
    is_master_data = filter_api.get("isMasterDataWorkflow", False)
    date_str = datetime.now().strftime("%Y%m%d")

    if is_master_data:
        file_path = metadata_api[0].get("request", {}).get("filePath") if metadata_api else None
        if not file_path:
            logger.error(
                f"Missing filePath in metadata_api. metadata_api={metadata_api}"
            )
        file_name = Path(file_path).stem
        prefix_part = file_name
    else:
        folder = filter_api.get("folderName")
        customer = filter_api.get("customerFolderName")
        if not folder or not customer:
            logger.error(
                f"Missing folderName or customerFolderName in filter_api. "
                f"filter_api={filter_api}"
            )
        prefix_part = f"{folder}/{customer}"

    return f"{materialized_step_data_loc}/{prefix_part}/{date_str}/{celery_id}/{step_name}"

def _prepare_step_config(step, file_processor, celery_id, context):
        step_config = STEP_DEFINITIONS.get(step.stepName)
        materialized_step_data_loc = config_loader.get_config_value(
            "s3_buckets", "materialized_step_data_loc"
        )
        workflow_detail = context.get("workflow_detail", {})
        s3_key_prefix = build_s3_key_prefix(materialized_step_data_loc, celery_id, step.stepName, workflow_detail)

        if not step_config:
            raise ValueError(f"No step configuration found for step '{step.stepName}'.")

        # NEW - assign stepId to file_processor
        if not hasattr(file_processor, "workflow_step_ids"):
            file_processor.workflow_step_ids = {}

        file_processor.workflow_step_ids[step.stepName] = step.workflowStepId

        return step_config, s3_key_prefix


async def _resolve_step_api(step, file_processor, context, step_config):    
    # Save step detail to context for use in output
    # Resolve metadata for API call
    config_api = []
    parser_step_id = file_processor.workflow_step_ids.get(
        "MASTER_DATA_FILE_PARSER"
    ) or file_processor.workflow_step_ids.get("TEMPLATE_FILE_PARSE")

    master_data_load_items = (
        context.get(step_config.data_input).output.items
        if hasattr(step_config, "data_input")
        and context.get(step_config.data_input)
        else None
    )
    logger.info(
        f"master_data_load items: {master_data_load_items}, "
        f"type: {type(master_data_load_items)}"
    )
    config_api_ctx = {
        "file_name": file_processor.file_record["file_name"],
        "file_name_without_ext": file_processor.file_record["file_name"].removesuffix(
            file_processor.file_record["file_extension"]
        ),
        "workflowStepId": parser_step_id,
        "templateFileParseId": None,
        "items": master_data_load_items,
    }
    config_metadata = resolve_api_call_from_step(
        step.stepName,
        context=config_api_ctx,
    )
    if not config_metadata:
        logger.warning(f"No API resolution for step: {step.stepName}")
        config_api = {}
    else:
        if "runner" in config_metadata:
            # run_chain now uses BEConnector
            config_api = await config_metadata["runner"](config_api_ctx)
        else:
            url = config_metadata["url"]
            method = config_metadata["method"]
            params = config_metadata["params"]
            body = config_metadata["body"]
            config_api_connector = BEConnector(url, params=params, body_data=body)
            if method == "get":
                config_api = await config_api_connector.get()
            elif method == "post":
                config_api = await config_api_connector.post()

    context["step_detail"].append(
        {
            "step": {
                "workflowStepId": step.workflowStepId,
                "stepName": step.stepName,
                "stepOrder": step.stepOrder,
                "stepConfiguration": step.stepConfiguration,
            },
            "config_api": config_api,
            "metadata_api": [],
        }
    )


async def _execute_and_finish_step(
    step,
    workflow_model,
    workflow_session,
    file_processor,
    celery_id,
    rerun_attempt,
    context,
    redis_utils,
    step_config,
    s3_key_prefix,
    request_id,
    step_response,
    file_path
):
    start_step_response_model = StartStep(**step_response)

    # Update step detail to S3 output
    context["step_detail"][step.stepOrder - 1]["metadata_api"].append(
        {
            "url": "/api/workflow/step/start",
            "request": {
                "sessionId": workflow_session.id,
                "stepId": step.workflowStepId,
            },
            "response": start_step_response_model,
        }
    )

    # Execute step (context is updated internally)
    try:
        step_result = await execute_step(
            file_processor, step, context, celery_id, rerun_attempt
        )
        # === Try to retrieve all traceability attributes again
        traceability_context_values = {
            key: val
            for key in [
                "workflow_name",
                "workflow_id",
                "document_number",
                "document_type",
            ]
            if (val := get_context_value(key)) is not None
        }
        logger.debug(
            f"Start step {step.stepName} updates...:\n"
            f"Function: {__name__}\n"
            f"RequestID: {request_id}\n"
            f"TraceabilityContext: {traceability_context_values}"
        )

    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        redis_utils.store_workflow_id(
            task_id=celery_id,
            workflow_id=workflow_model.id,
            status=StatusEnum.FAILED,
        )
        logger.exception(
            f"[{celery_id}] Step {step.stepName} failed: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.ERROR,
                "file_path": file_path,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )
        raise RuntimeError(f"Step {step.stepName} failed") from e

    # Finish step
    if step_result.step_status == StatusEnum.FAILED:
        redis_utils.store_workflow_id(
            task_id=celery_id,
            workflow_id=workflow_model.id,
            status=StatusEnum.FAILED,
        )
        redis_utils.store_step_status(
            task_id=celery_id,
            step_name=step.stepName,
            status="Failed",
            step_id=step.workflowStepId,
        )
        step_output = (
            file_processor.current_output_path
            if step_config.store_materialized_data
            else None
        )
        err_msg = "; ".join(step_result.step_failure_message or ["Unknown error"])
        finish_step_response_model = await BEConnector(
            ApiUrl.WORKFLOW_STEP_FINISH.full_url(),
            {
                "workflowHistoryId": start_step_response_model.workflowHistoryId,
                "code": StatusEnum.FAILED,          
                "message": err_msg,
                "dataInput": "input_data",
                "dataOutput": step_output,
            },
        ).post()

        context["step_detail"][step.stepOrder - 1]["metadata_api"].append(
            {
                "url": "/api/workflow/step/finish",
                "request": {
                    "workflowHistoryId": start_step_response_model.workflowHistoryId,
                    "code": StatusEnum.FAILED,
                    "message": err_msg,
                    "dataInput": "input_data",
                    "dataOutput": step_output,
                },
                "response": finish_step_response_model,
            }
        )

    else:
        redis_utils.store_step_status(
            task_id=celery_id,
            step_name=step.stepName,
            status="Done",
            step_id=step.workflowStepId,
        )
        step_output = (
            file_processor.current_output_path
            if step_config.store_materialized_data
            else None
        )
        finish_step_response_model = await BEConnector(
            ApiUrl.WORKFLOW_STEP_FINISH.full_url(),
            {
                "workflowHistoryId": start_step_response_model.workflowHistoryId,
                "code": StatusEnum.SUCCESS,
                "message": "",
                "dataInput": "input_data",
                "dataOutput": step_output,
            },
        ).post()

        context["step_detail"][step.stepOrder - 1]["metadata_api"].append(
            {
                "url": "/api/workflow/step/finish",
                "request": {
                    "workflowHistoryId": start_step_response_model.workflowHistoryId,
                    "code": StatusEnum.SUCCESS,
                    "message": "",
                    "dataInput": "input_data",
                    "dataOutput": step_output,
                },
                "response": finish_step_response_model,
            }
        )

    # === Inject step_detail into result output (support both BaseModel and dict)
    _inject_metadata_into_step_result_output(
        step_result, context, file_processor.document_type
    )

    # Handle logic to store materialized data after every step
    step_result_in_s3 = file_processor.check_step_result_exists_in_s3(
        task_id=celery_id,
        step_name=step.stepName,
        rerun_attempt=rerun_attempt,
    )
    step_needs_storing = store_step_result_to_s3(
        step_config=step_config, step_result_in_s3=step_result_in_s3
    )

    if step_needs_storing:
        logger.info(f"{step.stepName} - step_result type: {type(step_result)}")
        file_base = file_processor.file_record["file_name"].removesuffix(
            file_processor.file_record["file_extension"]
        )
        json_output_path = f"{s3_key_prefix}/{file_base}.json"

        # Update the inner output (MasterDataParsed)
        updated_output = step_result.output.copy(
            update={"json_output": json_output_path}
        )

        # Update the step_result (StepOutput) with the new output
        step_result = step_result.copy(update={"output": updated_output})
        # Write step result to S3
        file_processor.write_json_to_s3(
            step_result, s3_key_prefix=s3_key_prefix, rerun_attempt=rerun_attempt
        )

        logger.info(
            f"Stored step data output to S3 at {s3_key_prefix}.",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": celery_id,
            },
        )

    # Check the current status, and raise ValueError if the status is failed
    raise_if_failed(step_result, step.stepName)


async def _finish_workflow(
    workflow_model,
    workflow_session,
    file_processor,
    celery_id,
    context,
    step_names,
    request_id,
    file_path,
    redis_utils
):
    # === Finish session ===
    # Update Redis status after done
    redis_utils.store_workflow_id(
        task_id=celery_id, workflow_id=workflow_model.id, status=StatusEnum.SUCCESS
    )
    finish_session_response = await BEConnector(
        ApiUrl.WORKFLOW_SESSION_FINISH.full_url(),
        {"id": workflow_session.id, "code": StatusEnum.SUCCESS, "message": ""},
    ).post()

    # Update workflow_detail for the final step output
    context["workflow_detail"]["metadata_api"].append(
        {
            "url": "/api/workflow/session/finish",
            "request": {
                "id": workflow_session.id,
                "code": StatusEnum.SUCCESS,
                "message": "",
            },
            "response": finish_session_response,
        }
    )

    # Update the output of parsed master data and write the raw master data to S3 only
    update_masterdata_proceed_output(
        file_processor=file_processor, context=context, step_names=step_names
    )
    file_processor.write_raw_to_s3(file_path)

    # === Try to retrieve all traceability attributes again
    traceability_context_values = {
        key: val
        for key in ["workflow_name", "workflow_id", "document_number", "document_type"]
        if (val := get_context_value(key)) is not None
    }
    logger.debug(
        f"Finish session updates...:\n"
        f"Function: {__name__}\n"
        f"RequestID: {request_id}\n"
        f"TraceabilityContext: {traceability_context_values}"
    )

    logger.info(
        f"[{celery_id}] Finished processing file: {file_path}",
        extra={
            "service": ServiceLog.TASK_EXECUTION,
            "log_type": LogType.TASK,
            "file_path": file_path,
            **traceability_context_values,
            "traceability": celery_id,
        },
    )

def _inject_metadata_into_step_result_output(
    step_result: StepOutput, context: dict, document_type: DocumentType
):
    step_detail = context.get("step_detail")
    workflow_detail = context.get("workflow_detail")

    if (
        not step_detail
        or not hasattr(step_result, "output")
        or step_result.output is None
    ):
        return  # Nothing to inject

    output = step_result.output
    if isinstance(output, BaseModel):
        # If output is a Pydantic model, update with new fields
        try:
            step_result.output = output.copy(
                update={"step_detail": step_detail, "workflow_detail": workflow_detail}
            )
        except Exception as e:
            logger.warning(f"Failed to update BaseModel output with metadata: {e}")

    elif isinstance(output, dict):
        # If output is a dict, parse it first then update
        try:
            parsed_output = template_utils.parse_data(
                document_type=document_type,
                data=output["json_data"].output,
            )
            logger.info(f"parsed_output: {parsed_output}")
            step_result.output = parsed_output.copy(
                update={"step_detail": step_detail, "workflow_detail": workflow_detail}
            )
        except Exception as e:
            logger.warning(f"Failed to parse and update dict output with metadata: {e}")

    else:
        # Unsupported output type for metadata injection
        logger.warning(
            f"[inject_metadata] Unsupported type for step_result.output: {type(output)}. "
            "Cannot inject step_detail/workflow_detail."
        )


def resolve_api_call_from_step(
    step: str, context: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Resolves the API URL, HTTP method, query parameters, and request body
    based on the workflow step.

    Args:
        step (str): The workflow step name.
        context (dict): External data (expects keys like 'file_name', 'workflowStepId').

    Returns:
        dict: A dictionary with keys: url (ApiUrl), method (str), params (dict), body (dict|None),
              or None if no matching step is found or context is invalid.
    """
    step_name = step.upper()

    step_map = {
        "FILE_PARSE": {
            "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
            "method": "get",
            "required_context": ["workflowStepId"],
            "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
            "body": None,
        },
        "VALIDATE_HEADER": {
            "url": ApiUrl.MASTERDATA_HEADER_VALIDATION,
            "method": "get",
            "required_context": ["file_name"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": None,
        },
        "VALIDATE_DATA": {
            "url": ApiUrl.MASTERDATA_COLUMN_VALIDATION,
            "method": "get",
            "required_context": ["file_name"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": None,
        },
        "MASTER_DATA_LOAD": {
            "url": ApiUrl.MASTER_DATA_LOAD_DATA,
            "method": "post",
            "required_context": ["file_name_without_ext", "items"],
            "params": None,
            "body": lambda ctx: {
                "fileName": ctx["file_name_without_ext"],
                "data": ctx["items"],
            },
        },
        "TEMPLATE_DATA_MAPPING": {
            # Instead of direct url, we provide a runner for multiple dependent requests
            "runner": lambda ctx: run_chain(
                ctx,
                [
                    {
                        "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
                        "method": "get",
                        "required_context": ["workflowStepId"],
                        "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                        "body": None,
                        # store templateFileParseId from first response
                        "extract": lambda resp, ctx: ctx.update(
                            {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                        ),
                    },
                    {
                        "url": lambda ctx: f"{ApiUrl.DATA_MAPPING.full_url()}?templateFileParseId={ctx['templateFileParseId']}",
                        "method": "get",
                        "required_context": ["templateFileParseId"],
                        "params": lambda ctx: {
                            "templateFileParseId": ctx["templateFileParseId"]
                        },
                        "body": None,
                    },
                ],
            )
        },
        "TEMPLATE_FORMAT_VALIDATION": {
            # Instead of direct url, we provide a runner for multiple dependent requests
            "runner": lambda ctx: run_chain(
                ctx,
                [
                    {
                        "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
                        "method": "get",
                        "required_context": ["workflowStepId"],
                        "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                        "body": None,
                        # store templateFileParseId from first response
                        "extract": lambda resp, ctx: ctx.update(
                            {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                        ),
                    },
                    {
                        "url": lambda ctx: f"{ApiUrl.TEMPLATE_FORMAT_VALIDATION.full_url()}/{ctx['templateFileParseId']}",
                        "method": "get",
                        "required_context": ["templateFileParseId"],
                        "params": lambda _: {},
                        "body": None,
                    },
                ],
            )
        },
    }

    for key, config in step_map.items():
        if key in step_name:
            # Case 1: multi-step runner
            if "runner" in config:
                return {"runner": config["runner"]}

            # Check required context keys
            missing_keys = [k for k in config["required_context"] if k not in context]
            if missing_keys:
                raise RuntimeError(
                    f"Missing context keys for step '{step}': {missing_keys}"
                )

            return {
                "url": (
                    config["url"](context)
                    if callable(config["url"])
                    else config["url"].full_url()
                ),
                "method": config["method"],
                "params": (
                    config["params"](context)
                    if callable(config["params"])
                    else config["params"]
                ),
                "body": (
                    config["body"](context)
                    if callable(config["body"])
                    else config["body"]
                ),
            }

    return None


def update_masterdata_proceed_output(
    file_processor: FileProcessor, step_names: List[str], context: dict
):
    if not set(step_names).intersection({"MASTER_DATA_LOAD_DATA", "write_json_to_s3"}):
        return

    logger.info("Start reading back the created JSON files...")
    timestamp = file_processor.current_time
    target_bucket = file_processor.target_bucket_name
    file_base = file_processor.file_record["file_name"].removesuffix(
        file_processor.file_record["file_extension"]
    )

    if (
        not file_processor.file_record["file_name"]
        or not file_processor.file_record["file_extension"]
    ):
        logger.warning("File name or extension missing. Skipping output resolution.")
        return

    # Remove extension and construct S3 key
    process_key = f"process_data/{file_base}/{file_base}_{timestamp}.json"

    try:
        raw_data = read_n_write_s3.read_json_from_s3(
            bucket_name=target_bucket,
            object_name=process_key,
        )
    except Exception as e:
        logger.exception(f"Failed to read processed JSON from S3: {e}")
        return

    parsed_data = template_utils.parse_data(
        document_type=file_processor.document_type,
        data=raw_data,
    )

    logger.info(
        "Session Finish - Parsed data loaded",
        extra={
            "data_preview": json.dumps(parsed_data, default=str)[:1000],
            "type": str(type(parsed_data)),
        },
    )

    # Inject metadata and save updated result
    updated_data = parsed_data.model_copy(
        update={
            "json_output": process_key,
            "workflow_detail": context.get("workflow_detail", {}),
        }
    ).model_dump(exclude_none=False)

    try:
        read_n_write_s3.write_json_to_s3(
            json_data=updated_data,
            file_record=file_processor.file_record,
            bucket_name=target_bucket,
        )
        logger.info(f"Updated processed output written to S3: {process_key}")
    except Exception as e:
        logger.exception(f"Failed to write updated result to S3: {e}")


def store_step_result_to_s3(
    step_config: StepDefinition, step_result_in_s3: Optional[StepOutput]
) -> bool:
    """Determines if the step result needs to be stored in S3 based on the step configuration"""
    if not step_config.store_materialized_data:
        return False

    if not step_result_in_s3:
        return True

    if hasattr(step_result_in_s3, "step_status"):
        # MasterData
        return step_result_in_s3.step_status == "2"
    else:
        # PO
        return True


async def run_chain(context: Dict[str, Any], steps: List[Dict[str, Any]]):
    """
    Executes a sequence of dependent API calls asynchronously using BEConnector.
    Each step may update context with values needed by later steps.
    """
    results = []
    for step in steps:
        url = step["url"](context) if callable(step["url"]) else step["url"].full_url()
        missing_keys = [k for k in step["required_context"] if k not in context]
        if missing_keys:
            raise RuntimeError(f"Missing context keys: {missing_keys}")

        # Use your BEConnector instead of requests
        logger.info(f"Running chain step: {url} with step {step}\ncontext: {context}")
        connector = BEConnector(url, params=step["params"](context))
        resp = await connector.get()
        results.append(resp)
        logger.info(f"Chain step response from {url}:\n{resp}")

        if "extract" in step:
            step["extract"](resp, context)

    # return the last response
    return results[-1]
