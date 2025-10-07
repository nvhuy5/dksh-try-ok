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
from dataclasses import asdict

from models.body_models import (
    WorkflowFilterBody,
    WorkflowSessionStartBody,
    WorkflowStepStartBody,
)

from .step_handler import execute_step, raise_if_failed
from connections.be_connection import BEConnector
from connections.redis_connection import RedisConnector
from processors.processor_base import ProcessorBase
from processors.helpers import template_helper
from models.class_models import (
    ContextData,
    FilePathRequest,
    StepDetail,
    StepDetailConfig,
    WorkflowDetailConfig,
    WorkflowModel,
    ApiUrl,
    StatusEnum,
    WorkflowSession,
    StartStep,
    DocumentType,
    StepOutput,
    StepDefinition,
    WorkflowStep,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
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
def task_execute(self, data: dict) -> str:
    """
    Celery task entry point (synchronous wrapper).

    This function serves as the entry point for a Celery task. It synchronously
    runs the internal asynchronous workflow using `asyncio.run`.

    Args:
        data (FilePathRequest): Request object containing the file path and related metadata.

    Returns:
        str: Status message indicating whether the task succeeded or failed.
    """
    try:
        file_request = FilePathRequest(**data)
        tracking_model = TrackingModel.from_data_request(file_request)
        logger.info(
            f"[{tracking_model.request_id}] Starting task execution",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.TASK,
                "data": tracking_model,
            },
        )
        ctx = contextvars.copy_context()
        ctx.run(lambda: handle_task(tracking_model))
        # ctx.run(lambda: asyncio.run(handle_task(tracking_model)))
        
        return "Task completed"
    except Retry:
        logger.warning(f"[{tracking_model.request_id}] Task is retrying...")
        raise
    except Exception as e:
        retry_count = self.request.retries
        max_retries = self.max_retries or 3
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[{tracking_model.request_id}] Task execution failed (attempt {retry_count}/{max_retries}): {e}\n{short_tb}",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.ERROR,
                "data": tracking_model,
            },
        )
        try:
            raise self.retry(exc=e, countdown=5)
        except MaxRetriesExceededError:
            logger.critical(
                f"[{tracking_model.request_id}] Maximum retries exceeded for task.",
                extra={
                    "service": ServiceLog.TASK_EXECUTION,
                    "log_type": LogType.ERROR,
                    "data": tracking_model,
                },
            )
            raise


def handle_task(tracking_model: TrackingModel) -> Dict[str, Any]:
    """
    Asynchronously handles the file processing workflow.
    Executes the main asynchronous logic for processing and tracking a file task.

    Args:
        tracking_model (TrackingModel): Model containing file path, Celery task ID,
            and other tracking information.

    Returns:
        Dict[str, Any]: Dictionary containing extracted or processed data results.
    """

    # === Pre-Processing ===
    logger.info(
        f"[{tracking_model.request_id}] Start processing file"
        + (
            f" | Rerun attempt: {tracking_model.rerun_attempt}"
            if tracking_model.rerun_attempt is not None
            else ""
        ),
        extra={
            "service": ServiceLog.TASK_EXECUTION,
            "log_type": LogType.TASK,
            "data": tracking_model,
        },
    )

    redis_connector = RedisConnector()
    file_processor = ProcessorBase(tracking_model)

    logger.info(
        f"[{tracking_model.request_id}] base processor result:\n",
        extra={
            "service": ServiceLog.METADATA_EXTRACTION,
            "log_type": LogType.TASK,
            "data": file_processor,
        },
    )

    context_data = ContextData(request_id=tracking_model.request_id)

    # === Fetch workflow ===
    workflow_model = get_workflow_filter(
        context_data=context_data,
        file_processor=file_processor,
        tracking_model=tracking_model,
    )

    logger.info(
        f"[{tracking_model.request_id}] Workflow detail:\n",
        extra={
            "service": ServiceLog.DATABASE,
            "log_type": LogType.ACCESS,
            "data": workflow_model,
        },
    )
    
    if not workflow_model:
        return context_data

    # === Start session ===
    start_session_model = call_workflow_session_start(
        context_data=context_data,
        tracking_model=tracking_model,
    )

    logger.info(
        f"[{tracking_model.request_id}] Workflow Session Start:\n",
        extra={
            "service": ServiceLog.DATABASE,
            "log_type": LogType.ACCESS,
            "data": start_session_model,
        },
    )

    if not start_session_model:
        return context_data

    # === Update Redis ===
    redis_connector.store_workflow_id(
        task_id=tracking_model.request_id,
        workflow_id=workflow_model.id,
        status=StatusEnum.PROCESSING.name,
    )

    try:
        # === Process steps ===
        # Sort steps in ascending order by stepOrder
        sorted_steps = sorted(workflow_model.workflowSteps, key=lambda step: step.stepOrder)
        for step in sorted_steps:
            # === Start step ===
            start_step_model = call_workflow_step_start(
                context_data=context_data,
                step=step,
            )

            # === Update Redis ===
            redis_connector.store_step_status(
                task_id=tracking_model.request_id,
                step_name=step.stepName,
                status=StatusEnum.PROCESSING.name,
                step_id=step.workflowStepId,
            )

            # === Execute step ===
            step_result = execute_step(file_processor, context_data, step)

            # === Update Redis ===
            redis_connector.store_step_status(
                task_id=tracking_model.request_id,
                step_name=step.stepName,
                status=StatusEnum(step_result.step_status).name,
                step_id=step.workflowStepId,
            )

            # === Finish step ===
            finish_step_model = call_workflow_step_finish(
                context_data=context_data,
                step=step,
                step_result=step_result
            )
            
            update_step_result_output(step_result, context_data, file_processor.document_type)
            
            if not context_data.is_done:
                logger.info(f"{step.stepName} - step_result type: {type(step_result)}")
                file_base = str(file_processor.file_record["file_name"]).removesuffix(file_processor.file_record["file_extension"])
                context_data.s3_key_prefix = f"{context_data.s3_key_prefix}/{file_base}.json"
                
                # Update the inner output (MasterDataParsed)
                updated_output = step_result.output.copy(
                    update={"json_output": context_data.s3_key_prefix}
                )
                
                # Update the step_result (StepOutput) with the new output
                step_result = step_result.model_copy(update={"output": updated_output})
                # Write step result to S3
                file_processor.write_json_to_s3(step_result, s3_key_prefix=context_data.s3_key_prefix, rerun_attempt=tracking_model.rerun_attempt)
                
                logger.info(
                    f"[{tracking_model.request_id}] Stored step data output to S3 at {context_data.s3_key_prefix}.",
                    extra={
                        "service": ServiceLog.DATA_TRANSFORM,
                        "log_type": LogType.TASK,
                        "data": tracking_model,
                    },
                )

        # === Update Redis ===
        redis_connector.store_workflow_id(
            task_id=tracking_model.request_id,
            workflow_id=workflow_model.id,
            status=StatusEnum.SUCCESS.name,
        )

        # === Finish session ===
        _ = call_workflow_session_finish(
            context_data=context_data,
            tracking_model=tracking_model,
        )

        return context_data

    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        # === Update Redis ===
        redis_connector.store_workflow_id(
            task_id=tracking_model.request_id,
            workflow_id=workflow_model.id,
            status=StatusEnum.FAILED.name,
        )
        logger.exception(
            f"[{tracking_model.request_id}] Step {step.stepName} failed: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.ERROR,
                "data": tracking_model,
            },
        )

        # call_workflow_step_finish

        # call_workflow_session_finish

        raise RuntimeError(f"Step {step.stepName} failed") from e


def get_workflow_filter(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
):
    logger.info(f"[{tracking_model.request_id}] Start workflow filter")
    body_data = asdict(WorkflowFilterBody(
        filePath=file_processor.file_record["file_path_parent"],
        fileName=file_processor.file_record["file_name"],
        fileExtension=file_processor.file_record["file_extension"],
        project=tracking_model.project_name,
        source=tracking_model.source_name,
    ))
    workflow_response = BEConnector(
        ApiUrl.WORKFLOW_FILTER.full_url(), 
        body_data=body_data
    ).post()
    if not workflow_response:
        raise RuntimeError(f"[{tracking_model.request_id}] Failed to fetch workflow")

    workflow_model = WorkflowModel(**workflow_response)
    if not workflow_model:
        raise RuntimeError(
            f"[{tracking_model.request_id}] Failed to initialize WorkflowModel from response"
        )
        
    context_data.workflow_detail = WorkflowDetailConfig()
    context_data.workflow_detail.filter_api.url = ApiUrl.WORKFLOW_FILTER.full_url()
    context_data.workflow_detail.filter_api.request = body_data
    context_data.workflow_detail.filter_api.response = workflow_model

    tracking_model.workflow_id = workflow_model.id
    tracking_model.workflow_name = workflow_model.name
    tracking_model.sap_masterdata = bool(workflow_model.sapMasterData)

    return workflow_model


def call_workflow_session_start(
    context_data: ContextData,
    tracking_model: TrackingModel,
):
    logger.info(f"[{tracking_model.request_id}] Start session")
    body_data = asdict(WorkflowSessionStartBody(
        workflowId=tracking_model.workflow_id,
        celeryId=tracking_model.request_id,
        filePath=tracking_model.file_path,
    ))
    session_response = BEConnector(
        ApiUrl.WORKFLOW_SESSION_START.full_url(), 
        body_data=body_data
    ).post()
    if not session_response:
        raise RuntimeError(
            f"[{tracking_model.request_id}] Failed to fetch workflow_session_start"
        )

    start_session_model = WorkflowSession(**session_response)
    if not start_session_model:
        raise RuntimeError(
            f"[{tracking_model.request_id}] Failed to initialize WorkflowSession from response"
        )

    context_data.workflow_detail.metadata_api.session_start_api.url = ApiUrl.WORKFLOW_SESSION_START.full_url()
    context_data.workflow_detail.metadata_api.session_start_api.request = body_data
    context_data.workflow_detail.metadata_api.session_start_api.response = start_session_model

    return start_session_model


def call_workflow_session_finish(
    context_data: ContextData,
    tracking_model: TrackingModel,
):
    
    logger.info(f"[{tracking_model.request_id}] Finish session")
    body_data = {
        "id": context_data.workflow_detail.metadata_api.session_start_api.response.id, 
        "code": StatusEnum.SUCCESS, 
        "message": ""
    }
    session_response = BEConnector(
        ApiUrl.WORKFLOW_SESSION_FINISH.full_url(), 
        body_data=body_data
    ).post()
    if not session_response:
        raise RuntimeError(
            f"[{tracking_model.request_id}] Failed to fetch workflow_session_finish"
        )

    context_data.workflow_detail.metadata_api.session_finish_api.url = ApiUrl.WORKFLOW_SESSION_START.full_url()
    context_data.workflow_detail.metadata_api.session_finish_api.request = body_data
    context_data.workflow_detail.metadata_api.session_finish_api.response = session_response

    return session_response


def call_workflow_step_start(
    context_data: ContextData,
    step: WorkflowStep,
):

    logger.info(f"[{context_data.request_id}] Starting step: {step.stepName}")
    body_data = asdict(WorkflowStepStartBody(
        sessionId=context_data.workflow_detail.metadata_api.session_start_api.response.id,
        stepId=step.workflowStepId,
    ))
    start_step_response = BEConnector(
        ApiUrl.WORKFLOW_STEP_START.full_url(), body_data
    ).post()
    if not start_step_response:
        raise RuntimeError(
            f"[{context_data.request_id}] Failed to fetch workflow_step_start"
        )

    start_step_model = StartStep(**start_step_response)
    if not start_step_model:
        raise RuntimeError(
            f"[{context_data.request_id}] Failed to initialize StartStep from response"
        )

    if not context_data.step_detail:
        context_data.step_detail = []

    if len(context_data.step_detail) < step.stepOrder:
        step_item = StepDetail()
        step_item.step = step
        step_item.config_api = None
        step_item.metadata_api = StepDetailConfig()
        context_data.step_detail.append(step_item)

    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_start_api.url = ApiUrl.WORKFLOW_STEP_START.full_url()
    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_start_api.request = body_data
    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_start_api.response = start_step_model
    return start_step_model


def call_workflow_step_finish(
    context_data: ContextData,
    step: WorkflowStep,
    step_result: StepOutput
):

    logger.info(f"[{context_data.request_id}] Finish step: {step.stepName}")
    err_msg = "; ".join(step_result.step_failure_message or ["Unknown error"])
    body_data = {
        "workflowHistoryId": context_data.step_detail[
            step.stepOrder - 1
        ].metadata_api.Step_start_api.response.workflowHistoryId,
        "code": StatusEnum(step_result.step_status).value,
        "message": err_msg if step_result.step_status == StatusEnum.FAILED else "",
        "dataInput": "data_input",
        "dataOutput": f"{context_data.s3_key_prefix}/",
    }
    finish_step_response = BEConnector(ApiUrl.WORKFLOW_STEP_FINISH.full_url(), body_data=body_data).post()
    
    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_finish_api.url = ApiUrl.WORKFLOW_STEP_FINISH.full_url()
    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_finish_api.request = body_data
    context_data.step_detail[step.stepOrder - 1].metadata_api.Step_finish_api.response = finish_step_response
    
    return finish_step_response


def update_step_result_output(step_result: StepOutput, context_data: ContextData, document_type: DocumentType):
    step_detail = context_data.step_detail
    workflow_detail = context_data.workflow_detail

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
            step_result.output = output.model_copy(
                update={"step_detail": step_detail, "workflow_detail": workflow_detail}
            )
        except Exception as e:
            logger.warning(f"Failed to update BaseModel output with metadata: {e}")

    elif isinstance(output, dict):
        # If output is a dict, parse it first then update
        try:
            parsed_output = template_helper.parse_data(
                document_type=document_type,
                data=output["json_data"].output,
            )
            logger.info(f"parsed_output: {parsed_output}")
            step_result.output = parsed_output.model_copy(
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