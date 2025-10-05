import logging
from typing import Optional, Type
from template_processors.template_processor import TemplateProcessor
from connections.be_connection import BEConnector
from models.class_models import ApiUrl, DocumentType
from utils import log_helpers
from utils.middlewares.request_context import get_context_value

# === Logging ===
logger_name = "ProcessorRegistry"
log_helpers.logging_config(logger_name)
logger = log_helpers.ValidatingLoggerAdapter(logging.getLogger(logger_name), {})


class ProcessorRegistry:
    """
    Central registry to fetch processor mapping from BE based on workflowStepId.
    """

    # Static mapping of BE template codes to TemplateProcessor enums
    code_to_processor = {
        # PO specific templates
        "TXT_001_TEMPLATE": TemplateProcessor.TXT_001_TEMPLATE,
        "TXT_002_TEMPLATE": TemplateProcessor.TXT_002_TEMPLATE,
        "TXT_003_TEMPLATE": TemplateProcessor.TXT_003_TEMPLATE,
        "TXT_004_TEMPLATE": TemplateProcessor.TXT_004_TEMPLATE,
        "XLS_001_TEMPLATE": TemplateProcessor.XLS_001_TEMPLATE,
        "XLS_002_TEMPLATE": TemplateProcessor.XLS_002_TEMPLATE,
        "XLSX_001_TEMPLATE": TemplateProcessor.XLSX_001_TEMPLATE,
        "XLSX_002_TEMPLATE": TemplateProcessor.XLSX_002_TEMPLATE,
        "XML_001_TEMPLATE": TemplateProcessor.XML_001_TEMPLATE,
        "CSV_001_TEMPLATE": TemplateProcessor.CSV_001_TEMPLATE,
        "CSV_002_TEMPLATE": TemplateProcessor.CSV_002_TEMPLATE,
        "CSV_003_TEMPLATE": TemplateProcessor.CSV_003_TEMPLATE,
        "CSV_004_TEMPLATE": TemplateProcessor.CSV_004_TEMPLATE,
        "PDF_001_TEMPLATE" : TemplateProcessor.PDF_001_TEMPLATE,
        "PDF_002_TEMPLATE" : TemplateProcessor.PDF_002_TEMPLATE,
        "PDF_003_TEMPLATE" : TemplateProcessor.PDF_003_TEMPLATE,
        "PDF_004_TEMPLATE" : TemplateProcessor.PDF_004_TEMPLATE,
        "PDF_005_TEMPLATE" : TemplateProcessor.PDF_005_TEMPLATE,
        "PDF_006_TEMPLATE" : TemplateProcessor.PDF_006_TEMPLATE,
        "PDF_007_TEMPLATE" : TemplateProcessor.PDF_007_TEMPLATE,
        "PDF_008_TEMPLATE" : TemplateProcessor.PDF_008_TEMPLATE,
        # Masterdata specific templates
        "TXT_MASTERDATA_TEMPLATE": TemplateProcessor.TXT_MASTERADATA_TEMPLATE,
        "EXCEL_MASTERDATA_TEMPLATE": TemplateProcessor.EXCEL_MASTERADATA_TEMPLATE,
    }

    @classmethod
    async def get_processor_for_file(cls, file_processor) -> TemplateProcessor:
        """
        Calls BE to get processor mapping for this file, returns processor instance.

        Args:
            file_processor: The FileProcessor object.

        Returns:
            TemplateProcessor instance.
        """
        request_id = get_context_value("request_id") or "unknown"

        try:
            if file_processor.document_type == DocumentType.ORDER:
                step_id = file_processor.workflow_step_ids.get("TEMPLATE_FILE_PARSE")
            else: 
                step_id = file_processor.workflow_step_ids.get("MASTER_DATA_FILE_PARSER")
            
            if not step_id:
                logger.error(f"[{request_id}] Missing current_step_id in context")
                raise RuntimeError("Missing workflowStepId in context")

            # === Call BE ===
            connector = BEConnector(
                ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
                params={"workflowStepId": step_id},
            )
            response = await connector.get()

            try:
                # If the response is a dictionary with a "data" key as expected
                if isinstance(response, dict):
                    template_info = response.get("data", [{}])[0].get(
                        "templateFileParse", {}
                    )
                    template_code = template_info.get("code")

                # If the response is a list (in case BE returns a raw list)
                elif isinstance(response, list):
                    template_info = response[0].get("templateFileParse", {})
                    template_code = template_info.get("code")
                else:
                    raise RuntimeError("Invalid response format from BE.")

                if not template_code:
                    raise RuntimeError("Invalid BE response structure: missing template code")

            except (KeyError, IndexError, TypeError):
                raise RuntimeError(
                    "Invalid BE response structure: missing template code."
                )

            processor_enum = cls._map_code_to_processor(template_code)
            if not processor_enum:
                logger.error(
                    f"[{request_id}] Unknown template code from BE: {template_code}",
                    extra={
                        "service": "ProcessorRegistry",
                        "log_type": "ERROR",
                        "traceability": request_id,
                    },
                )
                raise RuntimeError(f"Unknown template code: {template_code}")

            logger.info(
                f"[{request_id}] Resolved processor: {processor_enum.name}",
                extra={
                    "service": "ProcessorRegistry",
                    "log_type": "INFO",
                    "traceability": request_id,
                },
            )

            return processor_enum.create_instance(file_path=file_processor.file_path)

        except Exception as e:
            logger.error(
                f"[{request_id}] Error in get_processor_for_file: {str(e)}",
                extra={
                    "service": "ProcessorRegistry",
                    "log_type": "ERROR",
                    "traceability": request_id,
                },
            )
            raise

    @classmethod
    def _map_code_to_processor(cls, code: str) -> Optional[Type[TemplateProcessor]]:
        return cls.code_to_processor.get(code)
