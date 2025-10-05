import logging
import traceback
from pathlib import Path
from models.class_models import SourceType, MasterDataParsed, StatusEnum
from utils import log_helpers, ext_extraction

# ===
# Set up logging
logger_name = "Excel Processor"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


class BaseMasterProcessor:
    """Processor for handling master data files.

    Initializes with a file path and source type, parses the file into JSON format,
    and uploads the result to S3.
    """

    def __init__(self, file_path: Path, source: SourceType = SourceType.S3):
        """Initialize the master data processor with a file path and source type.

        Args:
            file_path (Path): The path to the master data file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        self.file_path = file_path
        self.file_object = None
        self.source = source

    # Text to json
    def parse_file_to_json(self) -> MasterDataParsed:
        """Parse the master data file into a JSON-compatible dictionary.

        Reads the file content from local or S3, splits it into blocks,
        extracts headers and items, and uploads the result to S3.

        Returns:
            Dict[str, Any]: A dictionary containing the original file path,
                headers (Dict[str, List[str]]), items (Dict[str, List[Dict[str, str]]]),
                and capacity (str).
        """
        try:
            self.file_object = ext_extraction.FileExtensionProcessor(
                file_path=self.file_path, source=self.source
            )
            file_object = self.file_object
            document_type = file_object._get_document_type()
            capacity = file_object._get_file_capacity()
            original_file_path = self.file_path

            text = self._read_file_content(file_object)
            headers, items = self._parse_text_blocks(text)

            return MasterDataParsed(
                original_file_path=original_file_path,
                headers=headers,
                document_type=document_type,
                items=items,
                step_status=StatusEnum.SUCCESS,
                message=None,
                capacity=capacity,
            )

        except Exception as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            print(f"Error while parsing file to JSON: {e}")
            return MasterDataParsed(
                original_file_path=self.file_path,
                headers=[],
                document_type=getattr(self, "document_type", None),
                items=[],
                step_status=StatusEnum.FAILED,
                message=short_tb,
                capacity=getattr(self, "capacity", None),
            )

    def _read_file_content(self, file_object) -> str:
        if file_object.source == "local":
            with open(file_object.file_path, "r", encoding="utf-8") as f:
                return f.read()
        else:
            file_object.object_buffer.seek(0)
            return file_object.object_buffer.read().decode("utf-8")

    def _parse_text_blocks(self, text: str) -> tuple[dict, dict]:
        headers = {}
        items = {}

        blocks = text.strip().split("# Table: ")
        for block in blocks:
            if not block.strip():
                continue

            lines = block.strip().splitlines()
            if len(lines) < 2:
                continue  # Invalid block

            table_name = lines[0].strip()
            table_headers = [col.strip() for col in lines[1].split("|")]
            headers[table_name] = table_headers

            table_items = []
            for row in lines[2:]:
                values = [v.strip() for v in row.split("|")]
                # Only include rows that match header length
                if len(values) == len(table_headers):
                    item = dict(zip(table_headers, values))
                    table_items.append(item)

            items[table_name] = table_items

        return headers, items
