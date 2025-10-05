# Standard Library Imports
from datetime import datetime
import logging
from utils import log_helpers
import importlib
import inspect
import types
from models.class_models import StepDefinition
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = "File Procesor Routers"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===

WORKFLOW_MODULES = [
    "extract_metadata",
    "mapping",
    "parse_file_to_json",
    "publish_data",
    "template_validation",
    "write_json_to_s3",
    # === Masterdata only ===
    "masterdata_validation",
    "write_raw_to_s3",
    # === Masterdata only ===
]

STEP_DEFINITIONS = {
    "TEMPLATE_FILE_PARSE": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="parsed_data",
        extract_to={
            "document_number": "po_number",  # PO_NUMBER
            "document_type": "document_type",  # master_data or order
        },
        # Write data to S3 after the step
        store_materialized_data=True,
    ),
    "TEMPLATE_DATA_MAPPING": StepDefinition(
        function_name="template_data_mapping",
        data_input="parsed_data",
        data_output="mapped_data",
        store_materialized_data=True,
    ),
    "TEMPLATE_FORMAT_VALIDATION": StepDefinition(
        function_name="template_format_validation",
        data_input="parsed_data",
        data_output="data_validation",
        store_materialized_data=True,
    ),
    "write_json_to_s3": StepDefinition(
        function_name="write_json_to_s3",
        data_input="parsed_data",
        data_output="s3_result",
        store_materialized_data=True,
    ),
    "publish_data": StepDefinition(
        function_name="publish_data",
        data_output="publish_data_result",
        store_materialized_data=True,
    ),
    # === Masterdata only ===
    "MASTER_DATA_FILE_PARSER": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="master_data_parsed",
        store_materialized_data=True,
    ),
    "MASTER_DATA_VALIDATE_HEADER": StepDefinition(
        function_name="masterdata_header_validation",
        data_input="master_data_parsed",
        data_output="masterdata_header_validation",
        store_materialized_data=True,
    ),
    "MASTER_DATA_VALIDATE_DATA": StepDefinition(
        function_name="masterdata_data_validation",
        data_input="master_data_parsed",
        data_output="masterdata_data_validation",
        store_materialized_data=True,
    ),
    "MASTER_DATA_LOAD_DATA": StepDefinition(
        function_name="write_json_to_s3",
        data_input="masterdata_data_validation",
        data_output="s3_result",
        # Custom flag to customize the object path to S3
        # The value will be handled in the function via **kwargs arg
        kwargs={"customized_object_name": True},
        store_materialized_data=True,
    ),
    # === Masterdata only ===
}


class FileProcessor:
    """
    Loads modules defined in the `workflow_nodes` package.

    Each function name in WORKFLOW_MODULES corresponds to a Python file in the `workflow_nodes/` directory, such as:
    - `extract_metadata` -> `workflow_nodes/extract_metadata.py`
    - `mapping` -> `workflow_nodes/mapping.py`
    - `parse_file_to_json` -> `workflow_nodes/parse_file_to_json.py`
    - etc.
    """

    def __init__(self, file_path: str):
        """Initializes the FileProcessor with a file path.

        Args:
            file_path (str): Path to the file to be processed.
        """
        self.file_path = file_path
        self.file_record = {}
        self.source_type = None
        self.document_type = None
        self.target_bucket_name = None
        self.current_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

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

        self._register_workflow_functions()

    def _register_workflow_functions(self) -> None:
        """
        Dynamically registers workflow functions as instance methods.

        Imports modules from `template_processors.workflow_nodes` based on
        `WORKFLOW_MODULES` and binds their functions to the instance. Logs
        successful registrations or warnings for missing modules.
        """
        base_module = "template_processors.workflow_nodes"

        for module_name in WORKFLOW_MODULES:
            try:
                module = importlib.import_module(f"{base_module}.{module_name}")
                for name, func in inspect.getmembers(module, inspect.isfunction):
                    bound_method = types.MethodType(func, self)
                    setattr(self, name, bound_method)
                    logger.debug(f"Registered method: {name} from {module_name}")
            except ModuleNotFoundError:
                logger.warning(f"Module {module_name} not found in workflow_nodes.")
