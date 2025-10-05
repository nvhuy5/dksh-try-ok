import pytest
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi_celery.template_processors.file_processors import (
    excel_processor,
    pdf_processor,
    txt_processor,
)
from fastapi_celery.utils.ext_extraction import FileExtensionProcessor
from fastapi_celery.template_processors.file_processor import FileProcessor
from fastapi_celery.models.class_models import DocumentType, PODataParsed, StepOutput, StatusEnum
import importlib
from fastapi_celery.utils.middlewares import request_context
from fastapi_celery.models.traceability_models import TraceabilityContextModel


class TestFileProcessor(unittest.IsolatedAsyncioTestCase):
    """Test case for the FileProcessor class.

    Verifies the functionality of extract_metadata, parse_file_to_json,
    and write_json_to_s3 methods under various conditions.
    """

    def setUp(self) -> None:
        """Set up the test environment.

        Initializes a FileProcessor instance with a test file path.

        Returns:
            None
        """
        self.file_path = "/tmp/testfile.pdf"
        self.processor = FileProcessor(self.file_path)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.extract_metadata.get_context_value"
    )
    @patch(
        "fastapi_celery.template_processors.workflow_nodes.extract_metadata.ext_extraction.FileExtensionProcessor"
    )
    def test_extract_metadata_success(
        self, mock_ext_processor: MagicMock, mock_get_context_value: MagicMock
    ) -> None:
        """Test successful extraction of metadata.

        Mocks FileExtensionProcessor to return valid metadata and verifies the result.

        Args:
            mock_ext_processor (MagicMock): Mocked FileExtensionProcessor instance.

        Returns:
            None
        """

        def side_effect(key):
            values = {
                "request_id": "test-request-id",
                "workflow_id": "test-workflow",
                "document_number": "DOC123",
            }
            return values.get(key, None)

        mock_get_context_value.side_effect = side_effect
        assert mock_get_context_value("request_id") == "test-request-id"

        mock_instance = MagicMock()
        mock_instance.file_path_parent = "/tmp"
        mock_instance.file_name = "testfile.pdf"
        mock_instance.file_extension = ".pdf"
        mock_instance.document_type = "order"
        mock_ext_processor.return_value = mock_instance

        request_context.traceability_context.set(
            TraceabilityContextModel(
                request_id="test-request-id",
                workflow_id="test-workflow",
                document_number="DOC123",
            )
        )
        from fastapi_celery.template_processors import (
            file_processor as file_processor_reload,
        )

        importlib.reload(file_processor_reload)

        processor = file_processor_reload.FileProcessor("/tmp/testfile.pdf")
        result = processor.extract_metadata()
        self.assertTrue(result)
        self.assertIn("file_path", processor.file_record)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=FileNotFoundError("not found"),
    )
    def test_extract_metadata_file_not_found(
        self, mock_ext_processor: MagicMock
    ) -> None:
        """Test metadata extraction failure due to FileNotFoundError.

        Verifies that the method returns False when the file is not found.

        Args:
            mock_ext_processor (MagicMock): Mocked FileExtensionProcessor instance.

        Returns:
            None
        """
        result = self.processor.extract_metadata()
        self.assertFalse(result.output)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=ValueError("bad value"),
    )
    def test_extract_metadata_value_error(self, mock_ext_processor: MagicMock) -> None:
        """Test metadata extraction failure due to ValueError.

        Verifies that the method returns False when a ValueError occurs.

        Args:
            mock_ext_processor (MagicMock): Mocked FileExtensionProcessor instance.

        Returns:
            None
        """
        result = self.processor.extract_metadata()
        self.assertFalse(result.output)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.extract_metadata.ext_extraction.FileExtensionProcessor",
        side_effect=Exception("unknown"),
    )
    def test_extract_metadata_exception(self, mock_ext_processor: MagicMock) -> None:
        """Test metadata extraction failure due to an unexpected Exception.

        Verifies that the method returns False when an unexpected error occurs.

        Args:
            mock_ext_processor (MagicMock): Mocked FileExtensionProcessor instance.

        Returns:
            None
        """
        result = self.processor.extract_metadata()
        self.assertFalse(result.output)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.parse_file_to_json.ProcessorRegistry.get_processor_for_file"
    )
    async def test_parse_file_to_json_success(self, mock_get_processor: AsyncMock) -> None:
        """Test successful parsing of file to JSON.

        Mocks the processor instance to return parsed JSON data and verifies the result.

        Args:
            mock_create_instance (MagicMock): Mocked create_instance method.

        Returns:
            None
        """
        self.processor.file_record = {"file_extension": ".pdf"}
        self.processor.document_type = DocumentType.ORDER

        mock_instance = MagicMock()
        expected_result = PODataParsed(
            original_file_path="dummy/path/to/file.pdf",
            document_type=DocumentType.ORDER,
            po_number="PO12345",
            items=[{"item_id": 1, "desc": "Sample item"}],
            step_status= StatusEnum.SUCCESS,
            messages= None,
            metadata=None,
            capacity="1000",
        )
        mock_instance.parse_file_to_json.return_value = expected_result
        mock_get_processor.return_value = mock_instance

        result = await self.processor.parse_file_to_json()
        self.assertEqual(result.output, expected_result)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.parse_file_to_json.ProcessorRegistry.get_processor_for_file",
        side_effect=RuntimeError(["Unsupported file extension"]),
    )
    async def test_parse_file_to_json_unsupported_extension(self, mock_registry) -> None:
        """Test parsing failure for unsupported file extension.

        Verifies that the method returns None for an unsupported extension.
        """
        self.processor.file_record = {"file_extension": ".exe"}

        result = await self.processor.parse_file_to_json()

        self.assertIsNone(result.output)
        self.assertEqual(result.step_status.name, "FAILED")
        assert any("Unsupported file extension" in msg for msg in result.step_failure_message)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.parse_file_to_json.ProcessorRegistry.get_processor_for_file",
        side_effect=Exception(["boom"]),
    )
    async def test_parse_file_to_json_exception(self, mock_get_processor: MagicMock) -> None:
        """Test parsing failure due to an unexpected Exception.

        Verifies that the method returns None when an error occurs.

        Args:
            mock_create_instance (MagicMock): Mocked create_instance method.

        Returns:
            None
        """
        self.processor.file_record = {"file_extension": ".pdf"}

        result = await self.processor.parse_file_to_json()
        self.assertIsNotNone(result)
        self.assertIsNone(result.output)
        self.assertEqual(result.step_status.name, "FAILED")
        assert any("boom" in msg for msg in result.step_failure_message)

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.read_n_write_s3.write_json_to_s3"
    )
    def test_write_json_to_s3_success(self, mock_write_json: MagicMock) -> None:
        """Test successful writing of JSON data to S3.

        Mocks the S3 write operation and verifies the returned S3 path.

        Args:
            mock_write_json (MagicMock): Mocked write_json_to_s3 function.

        Returns:
            None
        """
        self.processor.file_record = {"some": "meta"}
        self.processor.document_type = DocumentType.ORDER
        mock_write_json.return_value = {"s3_path": "s3://bucket/file.json"}
        result = self.processor.write_json_to_s3({"json": "data"})
        self.assertEqual(type(result).__name__, "StepOutput")

    @patch(
        "fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.read_n_write_s3.write_json_to_s3",
        side_effect=Exception(["write failed"]),
    )
    def test_write_json_to_s3_exception(self, mock_write_json: MagicMock) -> None:
        """Test writing failure due to an unexpected Exception.

        Verifies that the method returns None when an error occurs.

        Args:
            mock_write_json (MagicMock): Mocked write_json_to_s3 function.

        Returns:
            None
        """
        self.processor.file_record = {"some": "meta"}
        self.processor.document_type = DocumentType.ORDER
        result = self.processor.write_json_to_s3({"po_number": "123", "json": "data"})
        self.assertIsNone(result.output)

    @patch("fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.get_context_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.config_loader.get_config_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.read_n_write_s3.read_json_from_s3")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.list_all_attempt_objects")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_json_to_s3.aws_connection.S3Connector")
    def test_check_step_result_exists_in_s3_successful_status_1(
        self,
        mock_s3_connector,
        mock_list_all_attempts,
        mock_read_json,
        mock_get_config,
        mock_get_context,
    ):
        # Mock config values
        mock_get_config.side_effect = lambda section, key: {
            ("s3_buckets", "datahub_s3_process_data"): "bucket-name",
            ("s3_buckets", "materialized_step_data_loc"): "materialized/data"
        }[(section, key)]

        # Mock context values
        mock_get_context.side_effect = lambda key: {
            "request_id": "req-id",
            "file_path": "/tmp/testfile.pdf",
            "workflow_name": "wf-name",
            "workflow_id": "wf-id",
            "document_number": "DOC123",
        }.get(key)

        # Mock S3 client and list_all_attempt_objects
        mock_s3_client = MagicMock()
        mock_s3_connector.return_value.client = mock_s3_client
        mock_list_all_attempts.return_value = [
            "materialized/data/task-id/step-name/testfile_rerun_1.json"
        ]

        # Mock S3 file contents
        mock_read_json.return_value = {
            "step_status": "1",
            "original_file_path": "/tmp/testfile.pdf",
            "document_type": "order",
            "po_number": "PO12345",
            "items": [{"item_id": "1", "desc": "Widget"}],
            "metadata": {"source": "test"},
            "capacity": "full"
        }

        # Assign necessary file_record and document_type
        self.processor.file_record = {
            "object_name": "testfile.pdf",
            "file_name": "testfile.pdf"
        }
        self.processor.document_type = DocumentType.ORDER

        # Run check
        result = self.processor.check_step_result_exists_in_s3(
            task_id="task-id", step_name="step-name", rerun_attempt=2
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.po_number, "PO12345")
        mock_read_json.assert_called_once()


# Pytest fixture to get the base path pointing to samples directory
@pytest.fixture
def base_path() -> Path:
    """Provide the base path to the samples directory.

    Returns:
        Path: The path to the samples directory relative to the test file.
    """
    return Path(__file__).parent / "samples"


def test_valid_extension(base_path: Path) -> None:
    """Test extraction of a valid file extension.

    Verifies that the file extension is correctly identified for a valid file.

    Args:
        base_path (Path): The base path to the samples directory.

    Returns:
        None
    """
    file_path = base_path / "0C-RLBH75-K0.pdf"
    file_processor = FileExtensionProcessor(file_path, source="local")
    result = file_processor.file_extension
    assert result == ".pdf"


# === TXT format files === #


@pytest.mark.parametrize(
    "file_name, field_name, expected_value",
    [
        ("PO202404007116.txt", "傳真號碼", "02 -87526100"),
        ("PO202404007116.txt", "需求單號", "MR202404015887"),
    ],
)
def test_po202404007116_txt(
    base_path: Path, file_name: str, field_name: str, expected_value: str
) -> None:
    """
    Test extraction of dynamic fields from the extracted PO data (傳真號碼, 需求單號).
    Verifies that specific fields are correctly extracted from the TXT file.

    Args:
        base_path (Path): The base path to the samples directory.
        file_name (str): The name of the TXT file to test.
        field_name (str): The field to check in the extracted data.
        expected_value (str): The expected value for the field.

    Returns:
        None
    """
    file_path = base_path / file_name
    result = txt_processor.TXTProcessor(
        file_path=file_path, source="local"
    ).parse_file_to_json()

    assert result, "Extraction result is empty."
    assert (
        field_name in result.items
    ), f"Field '{field_name}' not found in extracted result."
    assert (
        result.items[field_name] == expected_value
    ), f"Mismatch for {field_name}: {result.items[field_name]} != {expected_value}"


# === Excel format files === #


@pytest.mark.parametrize(
    "file_name, expected_order_id, expected_store_name",
    [
        ("0808三友WX.xls", "1411733659", "7117南港中信店"),
        ("0808三友WX.xls", "1415308842", "7153澎湖澎坊三號港店"),
    ],
)
def test_excel_itemvalue(
    base_path: Path, file_name: str, expected_order_id: str, expected_store_name: str
) -> None:
    """Test extraction of order ID and store name from an Excel file.

    Verifies that specific order IDs and corresponding store names are correctly extracted.

    Args:
        base_path (Path): The base path to the samples directory.
        file_name (str): The name of the Excel file to test.
        expected_order_id (str): The expected order ID to find.
        expected_store_name (str): The expected store name corresponding to the order ID.

    Returns:
        None
    """
    file_path = base_path / file_name
    result = excel_processor.ExcelProcessor(
        file_path=file_path, source="local"
    ).parse_file_to_json()
    items = result.items

    matched_item = next(
        (item for item in items if item["訂單編號"] == expected_order_id), None
    )
    assert (
        matched_item is not None
    ), f"Expected to find item with 訂單編號={expected_order_id}, but not found."
    assert matched_item["店號/店名"] == expected_store_name
