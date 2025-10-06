import json
import traceback
import logging
import asyncio
from pydantic import BaseModel
from connections.be_connection import BEConnector
from processors.processor_nodes import PROCESS_DEFINITIONS
from processors.processor_base import ProcessorBase
from processors.helpers import template_helper
from models.class_models import (
    ApiUrl,
    ContextData,
    StepDetail,
    WorkflowStep,
    StepDefinition,
    StatusEnum,
    StepOutput,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from typing import Dict, Any, Callable, List, Optional
from utils import log_helpers
import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values
from datetime import datetime

# ===
# Set up logging
logger_name = "Step Handler"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def get_model_dump_if_possible(obj: Any) -> dict | Any:
    """
    Returns the model dump dictionary from obj.output if both obj and obj.output are instances of BaseModel.
    Otherwise, returns the original object.

    Parameters:
        obj (Any): Expected to be a BaseModel instance with an attribute 'output' also a BaseModel.

    Returns:
        dict: The result of obj.output.model_dump() if conditions are met.
        Any: The original obj if conditions are not met.
    """
    if obj and isinstance(obj, BaseModel) and isinstance(obj.output, BaseModel):
        return obj.output.model_dump()
    return obj


def raise_if_failed(result: BaseModel, step_name: str) -> None:
    """
    Raises RuntimeError if the result indicates failure.
    """
    if not isinstance(result, BaseModel):
        return
    if result.step_status != StatusEnum.FAILED:
        return

    logger.error(
        f"step_status: {result.step_status}\nstep_failure_message: {result.step_failure_message}"
    )
    raise RuntimeError(
        f"Step '{step_name}' failed to complete!\n{result.step_failure_message}"
    )


def has_args(step_config: StepDefinition) -> bool:
    """
    Check if the step configuration has non-empty 'args' attribute.

    Args:
        step_config (object): Step configuration object, expected to have 'args' attribute.

    Returns:
        bool: True if 'args' attribute exists and is non-empty, False otherwise.
    """
    return hasattr(step_config, "args") and step_config.args


def resolve_args(step_config: StepDefinition, context: dict, step_name: str) -> tuple:
    """
    Resolve and prepare argument list for a step function based on step configuration and context.

    Args:
        step_config (object): Step configuration object with 'args' or 'data_input' attributes.
        context (dict): Dictionary containing current context data.
        step_name (str): Name of the step, used for logging.

    Returns:
        list: List of arguments to be passed to the step function.
    """
    args = []
    kwargs = {}

    if has_args(step_config):
        args = [context[arg] for arg in step_config.args]
        logger.info(f"[resolve_args] using args for {step_name}: {step_config.args}")
        context["input_data"] = args[0] if len(args) == 1 else args
    elif hasattr(step_config, "data_input") and step_config.data_input:
        args = [context.get(step_config.data_input)]
        logger.info(
            f"[resolve_args] using args for {step_name}: {step_config.data_input}"
        )

    # === Keyword arguments
    if hasattr(step_config, "kwargs") and step_config.kwargs:
        kwargs = {
            arg_name: (
                context[arg_key]
                if isinstance(arg_key, str) and arg_key in context
                else arg_key
            )
            for arg_name, arg_key in step_config.kwargs.items()
        }
        logger.info(
            f"[resolve_args] using keyword args for {step_name}: {json.dumps(kwargs, default=str)}"
        )

    return args, kwargs


def extract_to_wrapper(
    func: Callable[[Dict[str, Any], Dict[str, Any], str, str], None],
) -> Callable[[Dict[str, Any], Dict[str, Any], str, str], None]:
    """
    Decorator that wraps a data extraction function with error handling and logging.

    Args:
        context (Dict[str, Any]): The shared context within the job.
        result (Dict[str, Any]): The output data from the previous step.
        ctx_key (str): The key to assign the value to in `context`.
        result_key (str): The key to retrieve the value from in `result`.

    Returns:
        Callable: Wrapped function with exception handling and logging.
    """

    def wrapper(
        context: Dict[str, Any], result: Dict[str, Any], ctx_key: str, result_key: str
    ) -> None:
        try:
            request_id = get_context_value("request_id")
            traceability_context_values = {
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
            return func(context, result, ctx_key, result_key)
        except Exception as e:
            context[ctx_key] = None
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.warning(
                f"Failed to extract '{result_key}' to context['{ctx_key}']: {e}\n{short_tb}",
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.ERROR,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )

    return wrapper


@extract_to_wrapper
def extract(
    context: Dict[str, Any], result: Dict[str, Any], ctx_key: str, result_key: str
) -> None:
    """
    Extracts a value from `result[result_key]` and assigns it to `context[ctx_key]`.

    Returns:
        None
    """
    context[ctx_key] = result[result_key]


# Suppress Cognitive Complexity warning due to step-specific business logic  # NOSONAR
async def execute_step(file_processor: ProcessorBase, context_data: ContextData, step: WorkflowStep) -> StepOutput:
    """
    Executes a single workflow step using the given file processor.

    The function determines the correct processor method for the step,
    runs it (awaiting if asynchronous), updates the shared context with
    results, and saves outputs to S3 when required.

    Args:
        file_processor (ProcessorBase): Processor instance that executes the step.
        context_data (ContextData): Shared data and intermediate results between steps.
        step (WorkflowStep): Definition and metadata of the current workflow step.

    Returns:
        StepOutput: The processed result of the current step.
    """

    step_name = step.stepName
    logger.info(f"Starting execute step: [{step_name}]")

    try:
        step_config = PROCESS_DEFINITIONS.get(step_name)
        if not step_config:
            raise ValueError(f"The step [{step_name}] is not yet defined")
        
        s3_key_prefix = build_s3_key_prefix(file_processor, context_data, step, step_config)

        items = (
            context_data.items
            if hasattr(step_config, "data_input")
            and context_data.get(step_config.data_input)
            else None
        )

        config_api_ctx = {
            "file_name": file_processor.file_record["file_name"],
            "file_name_without_ext": str(file_processor.file_record["file_name"]).removesuffix(
                file_processor.file_record["file_extension"]
            ),
            "workflowStepId": step.workflowStepId,
            "templateFileParseId": None,
            "items": items,
        }

        build_api = build_api_request(step_name, config_api_ctx)

        if not build_api:
            logger.warning(f"No API resolution for step: {step_name}")
            config_api = {}
        else:
            if "runner" in build_api:
                # run_chain now uses BEConnector
                config_api = await build_api["runner"](config_api_ctx)
            else:
                url = build_api["url"]
                method = build_api["method"]
                params = build_api["params"]
                body = build_api["body"]
                config_api_connector = BEConnector(url, params=params, body_data=body)
                if method == "get":
                    config_api = await config_api_connector.get()
                elif method == "post":
                    config_api = await config_api_connector.post()

        context_data.step_detail[step.stepOrder - 1].config_api = config_api

        is_done = False
        step_result = file_processor.check_step_result_exists_in_s3(
            task_id=context_data.request_id,
            step_name=step_name,
            rerun_attempt=file_processor.tracking_model.rerun_attempt,
        )

        if step_result:
            if hasattr(step_result, "step_status"):
                # MasterDataParsed has step_status
                is_done = step_result.step_status == "1"
            else:
                # PODataParsed does not have step_status
                is_done = False

        logger.info(
            f"Step '{step.stepName}' already_done: {is_done}, "
            f"result_step: {step_result}"
        )

        if is_done:
            if step_config.require_data_output:
                context_data["s3_key_prefix"] = s3_key_prefix

            logger.info(
                f"[SKIP] Step '{step.stepName}' already has materialized data in S3. Skipping execution.",
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.TASK,
                    "data": file_processor.tracking_model,
                },
            )

            # Save output
            output_key = step_config.data_output
            if output_key:
                step_output_data = StepOutput(
                    output=template_helper.parse_data(
                        file_processor.document_type, data=step_result
                    ),
                    step_status=StatusEnum.SUCCESS,
                    step_failure_message=None,
                )
                context_data[output_key] = step_output_data
                return step_output_data
            else:
                return

        logger.info(
            f"Executing step: {step_name}, already_done: {is_done}"
            + (f" | Rerun attempt: {file_processor.tracking_model.rerun_attempt}" if file_processor.tracking_model.rerun_attempt is not None else ""),
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.TASK,
                "data": file_processor.tracking_model,
            },
        )

        try:
            method_name = step_config.function_name
            method = getattr(file_processor, method_name, None)

            if method is None or not callable(method):
                raise AttributeError(
                    f"Function '{method_name}' not found in FileProcessor."
                )

            # Resolve args
            args, kwargs = resolve_args(step_config, context_data, step_name)
            log_for_args = (
                json.dumps(step_config.args, default=str)
                if step_config.args
                else step_config.data_input
            )
            logger.info(
                (
                    f"Calling {method_name} with args: {log_for_args} - kwargs: {json.dumps(kwargs, default=str)}"
                    if log_for_args
                    else f"Calling {method_name} with no args provided!"
                ),
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.TASK,
                    "data": file_processor.tracking_model,
                },
            )

            # Call method (await if coroutine)
            call_kwargs = kwargs or {}
            result = (
                await method(*args, **call_kwargs)
                if asyncio.iscoroutinefunction(method)
                else method(*args, **call_kwargs)
            )

            logger.info(
                f"Step '{step_name}' executed successfully.",
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.TASK,
                    "data": file_processor.tracking_model,
                },
            )

            # Save output
            output_key = step_config.data_output
            if output_key:
                context_data[output_key] = result

            # Extract specific subfields into context for further usage
            extract_map = step_config.extract_to or {}
            logger.info(f"Extracted map for further usage: {extract_map}")

            # === Step 1: Filter and publish only valid traceability keys ===
            valid_keys = TrackingModel.model_fields.keys()
            result_dump = get_model_dump_if_possible(result)
            filtered_context_data = {
                ctx_key: result_dump[result_key]
                for ctx_key, result_key in extract_map.items()
                if ctx_key in valid_keys and result_key in result_dump
            }
            
            set_context_values(**filtered_context_data)

            # === Step 2: Refresh context values ===
            traceability_context_values = {
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
                f"Update extract_to attribute...\n"
                f"Function: {__name__}\n"
                f"RequestID: {context_data.request_id}\n"
                f"TraceabilityContext: {traceability_context_values}"
            )

            # === Step 3: Attempt to extract all values into `context` ===
            for ctx_key, result_key in extract_map.items():
                extract(context_data, result_dump, ctx_key, result_key)

            return result

        except AttributeError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"[Missing step]: {str(e)}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.ERROR,
                    "data": file_processor.tracking_model,
                },
            )
            raise
        except Exception as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.exception(
                f"Exception during step '{step_name}': {str(e)}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATA_TRANSFORM,
                    "log_type": LogType.ERROR,
                    "data": file_processor.tracking_model,
                },
            )
            raise


        # alias_entry = PROCESS_DEFINITIONS.get(step_name, {"function_name": step_name})
        # method_name = alias_entry["function_name"]
        # requires_input = alias_entry.get("data_input", False)
        # method = getattr(file_processor, method_name, None)

        # if method is None or not callable(method):
        #     raise AttributeError(
        #         f"Function '{method_name}' not found in FileProcessor."
        #     )

        # if asyncio.iscoroutinefunction(method):
        #     result = await method(data_input) if requires_input else await method()
        # else:
        #     result = method(data_input) if requires_input else method()

        # logger.info(f"Step '{step_name}' executed successfully.")
        # return result

    except AttributeError as e:
        logger.error(f"[Missing step]: {str(e)}")
        return None
    except Exception as e:
        logger.exception(f"Exception during step '{step_name}': {str(e)}")
        return None


def build_s3_key_prefix(
    file_processor: ProcessorBase,
    context_data: ContextData,
    step: WorkflowStep,
    step_config: StepDefinition,
) -> str:
    """
    Build the S3 key prefix for storing processed or master data files.

    The prefix structure differs based on whether the workflow is for
    master data or a standard processing workflow.

    Examples:
        - Processor workflow:
          {target_store_data}/{folderName}/{customerFolderName}/{yyyyMMdd}/{celery_id}/{step_name}
        - Master data workflow:
          {target_store_data}/{fileName}/{yyyyMMdd}/{celery_id}/{step_name}

    Args:
        file_processor (ProcessorBase): The processor instance containing file and tracking info.
        context_data (ContextData): Context object holding workflow and API response data.
        step (WorkflowStep): The current workflow step being executed.
        step_config (StepDefinition): Step configuration model, defines storage settings.

    Returns:
        str: Constructed S3 key prefix path.
    """
    filter_api = context_data.workflow_detail.filter_api
    metadata_api = context_data.workflow_detail.metadata_api
    is_master_data = filter_api.request.get("isMasterDataWorkflow", False)
    date_str = datetime.now().strftime("%Y%m%d")

    if is_master_data:
        prefix_part = file_processor.file_record.file_name
    else:
        folder = filter_api.response.get("folderName")
        customer = filter_api.response.get("customerFolderName")
        if not folder or not customer:
            logger.error(
                "Missing 'folderName' or 'customerFolderName' in filter_api response. "
                f"filter_api={filter_api}"
            )
        prefix_part = f"{folder}/{customer}"

    return (
        f"{step_config.target_store_data}/"
        f"{prefix_part}/{date_str}/"
        f"{file_processor.tracking_model.request_id}/"
        f"{step.stepName}"
    )


def build_api_request(step_name: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Build API request or runner config for a workflow step.

    Args:
        step_name: Workflow step name.
        context: Context data (e.g., file_name, workflowStepId).

    Returns:
        API config dict or runner callable, or None if not matched.
    """
    step_name_upper = step_name.upper()

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
            "required_context": ["file_name", "items"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": lambda ctx: {
                "fileName": ctx["file_name"],
                "data": ctx["items"],
            },
        },
        "TEMPLATE_DATA_MAPPING": {
            # Instead of direct url, we provide a runner for multiple dependent requests
            "runner": lambda ctx: execute_api_chain(
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
            "runner": lambda ctx: execute_api_chain(
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
        if key in step_name_upper:
            # Case 1: multi-step runner
            if "runner" in config:
                return {"runner": config["runner"]}

            # Check required context keys
            missing_keys = [k for k in config["required_context"] if k not in context]
            if missing_keys:
                raise RuntimeError(
                    f"Missing context keys for step '{step_name_upper}': {missing_keys}"
                )

            return {
                "url": (
                    config["url"](context)
                    if callable(config["url"])
                    else config["url"].full_url()
                ),
                "method": config["method"],
                "params": config["params"](context),
                "body": (
                    config["body"](context)
                    if callable(config["body"])
                    else config["body"]
                ),
            }

    return None


async def execute_api_chain(context: Dict[str, Any], steps: List[Dict[str, Any]]):
    """
    Execute dependent API calls sequentially.

    Args:
        context: Shared data across steps.
        steps: List of step configs.

    Returns:
        Response from the last API call.
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
