import json
import traceback
import logging
import asyncio
from pydantic import BaseModel
from template_processors.file_processor import FileProcessor, STEP_DEFINITIONS
from template_processors.common import template_utils
from models.class_models import WorkflowStep, StepDefinition, StatusEnum, StepOutput
from models.traceability_models import ServiceLog, LogType, TraceabilityContextModel
from typing import Dict, Any, Callable, Optional
from utils import log_helpers
import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values

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
async def execute_step(
    file_processor: FileProcessor,
    step: WorkflowStep,
    context: dict,
    task_id: str,
    rerun_attempt: Optional[int] = None,
) -> StepOutput:
    """
    Execute a workflow step by calling the corresponding FileProcessor method.

    Resolves arguments, runs the step (awaits if async), updates context with results,
    extracts subfields, and saves materialized data to S3 if needed.

    Args:
        file_processor (FileProcessor): Handles file processing methods.
        step (WorkflowStep): Metadata of the workflow step.
        context (dict): Shared context data across steps.
        task_id (str): Task identifier for logging and S3 keys.

    Returns:
        StepOutput: Result of the step execution, or None on error.
    """
    # ===
    request_id = get_context_value("request_id") or task_id
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
        f"Function: {__name__}\n"
        f"RequestID: {request_id}\n"
        f"TraceabilityContext: {traceability_context_values}"
    )

    step_name = step.stepName

    # === Check if this step has already been completed in S3 ===
    step_config = STEP_DEFINITIONS.get(step_name)
    if not step_config:
        raise ValueError(f"No step configuration found for step '{step_name}'.")

    already_done = False  
    step_result_in_s3 = file_processor.check_step_result_exists_in_s3(
        task_id=task_id,
        step_name=step.stepName,
        rerun_attempt=rerun_attempt,
    )
    if step_result_in_s3:
        if hasattr(step_result_in_s3, "step_status"):
            # MasterDataParsed has step_status
            already_done = step_result_in_s3.step_status == "1"
        else:
            # PODataParsed does not have step_status
            already_done = False

    logger.info(
        f"Step '{step.stepName}' already_done: {already_done}, "
        f"step_result_in_s3: {step_result_in_s3}"
    )
    if already_done:
        if step_config.store_materialized_data:
            materialized_step_data_loc = config_loader.get_config_value(
                "s3_buckets", "materialized_step_data_loc"
            )
            s3_key_prefix = f"{materialized_step_data_loc}/{task_id}/{step_name}"
            context["materialized_step_data_loc"] = s3_key_prefix

        logger.info(
            f"[SKIP] Step '{step.stepName}' already has materialized data in S3. Skipping execution.",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.TASK,
                "traceability": task_id,
                **traceability_context_values,
            },
        )

        # Save output
        output_key = step_config.data_output
        if output_key:
            step_output_data = StepOutput(
                output=template_utils.parse_data(
                    file_processor.document_type, data=step_result_in_s3
                ),
                step_status=StatusEnum.SUCCESS,
                step_failure_message=None,
            )
            context[output_key] = step_output_data
            return step_output_data
        else:
            return

    logger.info(
        f"Executing step: {step_name}, already_done: {already_done}"
        + (f" | Rerun attempt: {rerun_attempt}" if rerun_attempt is not None else ""),
        extra={
            "service": ServiceLog.DATA_TRANSFORM,
            "log_type": LogType.TASK,
            **traceability_context_values,
            "traceability": task_id,
        },
    )

    try:
        step_config = STEP_DEFINITIONS.get(step_name)
        if not step_config:
            raise ValueError(f"No step configuration found for step '{step_name}'.")

        method_name = step_config.function_name
        method = getattr(file_processor, method_name, None)

        if method is None or not callable(method):
            raise AttributeError(
                f"Function '{method_name}' not found in FileProcessor."
            )

        # Resolve args
        args, kwargs = resolve_args(step_config, context, step_name)
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
                **traceability_context_values,
                "traceability": task_id,
            },
        )

        # Call method (await if coroutine)
        call_kwargs = kwargs or {}
        result = (
            await method(*args, **call_kwargs)
            if asyncio.iscoroutinefunction(method)
            else method(*args, **call_kwargs)
        )

        # === Try to retrieve all traceability attributes again
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
            f"Finish step {step_name} updates...\n"
            f"Function: {__name__}\n"
            f"RequestID: {request_id}\n"
            f"TraceabilityContext: {traceability_context_values}"
        )

        logger.info(
            f"Step '{step_name}' executed successfully.",
            extra={
                "service": ServiceLog.DATA_TRANSFORM,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": task_id,
            },
        )

        # Save output
        output_key = step_config.data_output
        if output_key:
            context[output_key] = result

        # Extract specific subfields into context for further usage
        extract_map = step_config.extract_to or {}
        logger.info(f"Extracted map for further usage: {extract_map}")

        # === Step 1: Filter and publish only valid traceability keys ===
        valid_keys = TraceabilityContextModel.model_fields.keys()
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
            f"RequestID: {request_id}\n"
            f"TraceabilityContext: {traceability_context_values}"
        )

        # === Step 3: Attempt to extract all values into `context` ===
        for ctx_key, result_key in extract_map.items():
            extract(context, result_dump, ctx_key, result_key)

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
                **traceability_context_values,
                "traceability": task_id,
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
                **traceability_context_values,
                "traceability": task_id,
            },
        )
        raise
