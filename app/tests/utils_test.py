import io
import json
import pytest
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
import os
import logging
import traceback
from fastapi_celery.utils import log_helpers
from fastapi_celery.connections import aws_connection
from fastapi_celery.models.class_models import SourceType, DocumentType, PODataParsed, StatusEnum
from fastapi_celery.utils.ext_extraction import FileExtensionProcessor
from fastapi_celery.utils.log_helpers import (
    logging_config,
    ValidatingLoggerAdapter,
    ServiceLog,
    LogType,
)
from fastapi_celery.template_processors.common import pdf_helpers
from fastapi_celery.models.traceability_models import LogType, ServiceLog
from fastapi_celery.utils.read_n_write_s3 import any_json_in_s3_prefix, _s3_connectors, read_json_from_s3


logger_name = "Extension Detection"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)

# Constants used in tests
OS_PATH_ISFILE = "os.path.isfile"
TYPES_STRING = '[".pdf", ".txt", ".csv"]'
BUCKET_NAME = "test-bucket"

# Define the mock configurations


@pytest.fixture
def mock_config() -> MagicMock:
    """Provide a mock for config_loader.get_config_value.

    Mocks the retrieval of configuration values, returning TYPES_STRING for 'types'
    and BUCKET_NAME for other keys.

    Returns:
        MagicMock: The mocked config_loader.get_config_value function.
    """
    with patch(
        "fastapi_celery.utils.ext_extraction.config_loader.get_config_value"
    ) as mock_config_value:
        mock_config_value.side_effect = lambda section, key: (
            TYPES_STRING if key == "types" else BUCKET_NAME
        )
        yield mock_config_value


# Mock for logging to avoid actual logs being written during tests


@pytest.fixture
def mock_logger() -> MagicMock:
    """Provide a mock for the logger to prevent actual logging during tests.

    Returns:
        MagicMock: The mocked logger instance.
    """
    with patch("fastapi_celery.utils.ext_extraction.logger") as mock_logger:
        yield mock_logger


# === Test Cases ===


def test_file_extension_processor_local_file(
    mock_config: MagicMock, mock_logger: MagicMock
) -> None:
    """Test FileExtensionProcessor with a local file.

    Verifies that file metadata (name, path parent, extension) is correctly extracted
    for a local file that exists.

    Args:
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    file_path = "path/to/local_file.txt"

    # Mock for os.path.isfile to simulate file existence
    with patch(OS_PATH_ISFILE, return_value=True):
        processor = FileExtensionProcessor(file_path=file_path, source=SourceType.LOCAL)

        assert processor.file_name == "local_file.txt"
        assert os.path.normpath(str(processor.file_path_parent)) == os.path.normpath(
            "path/to/"
        )
        assert processor.file_extension == ".txt"


def test_file_extension_processor_local_file_not_found(
    mock_config: MagicMock, mock_logger: MagicMock
) -> None:
    """Test FileExtensionProcessor when the local file is not found.

    Verifies that a FileNotFoundError is raised when the local file does not exist.

    Args:
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    file_path = "path/to/non_existent_file.txt"

    # Mock for os.path.isfile to simulate file non-existence (False)
    with patch(OS_PATH_ISFILE, return_value=False):
        with pytest.raises(
            FileNotFoundError, match=f"Local file '{file_path}' does not exist."
        ):
            FileExtensionProcessor(file_path=file_path, source=SourceType.LOCAL)


def test_file_extension_processor_invalid_extension(
    mock_config: MagicMock, mock_logger: MagicMock
) -> None:
    """Test FileExtensionProcessor with an invalid file extension.

    Verifies that a FileNotFoundError is raised for a file with an unsupported extension.

    Args:
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    file_path = "path/to/unsupported_file.xyz"

    # Mock for os.path.isfile to simulate file non-existence (False)
    with patch(OS_PATH_ISFILE, return_value=False):
        with pytest.raises(FileNotFoundError):
            FileExtensionProcessor(file_path=file_path, source=SourceType.LOCAL)


# === Test for _get_document_type ===
# Patch the necessary methods and classes to mock the S3 behavior
@patch(
    "fastapi_celery.utils.ext_extraction.read_n_write_s3.get_object",
    return_value=b"dummy content",
)
@patch("fastapi_celery.utils.ext_extraction.aws_connection.S3Connector")
def test_get_document_type_s3(
    mock_s3_connector: MagicMock,
    mock_get_object: MagicMock,
    mock_config: MagicMock,
    mock_logger: MagicMock,
) -> None:
    """Test _get_document_type for an S3 file classified as ORDER.

    Verifies that the document type is correctly identified as ORDER for an S3 file.

    Args:
        mock_s3_connector (MagicMock): Mocked S3Connector class.
        mock_get_object (MagicMock): Mocked get_object function.
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    mock_client = MagicMock()
    mock_s3_connector.return_value.client = mock_client
    mock_client.head_object.return_value = {"ContentLength": 1024 * 1024 * 5}  # 5 MB

    file_path = "bucket_name/folder/data.csv"
    processor = FileExtensionProcessor(file_path=file_path, source=SourceType.S3)

    document_type = processor._get_document_type()
    assert document_type == DocumentType.ORDER


@patch(
    "fastapi_celery.utils.ext_extraction.read_n_write_s3.get_object",
    return_value=b"dummy content",
)
@patch("fastapi_celery.utils.ext_extraction.aws_connection.S3Connector")
def test_get_document_type_master_data(
    mock_s3_connector: MagicMock,
    mock_get_object: MagicMock,
    mock_config: MagicMock,
    mock_logger: MagicMock,
) -> None:
    """Test _get_document_type for an S3 file classified as MASTER_DATA.

    Verifies that the document type is correctly identified as MASTER_DATA for an S3 file.

    Args:
        mock_s3_connector (MagicMock): Mocked S3Connector class.
        mock_get_object (MagicMock): Mocked get_object function.
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    mock_client = MagicMock()
    mock_s3_connector.return_value.client = mock_client
    mock_client.head_object.return_value = {"ContentLength": 1024 * 1024 * 5}  # 5 MB

    file_path = "DKSH_SFTP/MASTER_DATA/file.csv"
    processor = FileExtensionProcessor(file_path=file_path, source=SourceType.S3)

    document_type = processor._get_document_type()
    assert document_type == DocumentType.MASTER_DATA


@patch(
    "fastapi_celery.utils.ext_extraction.read_n_write_s3.get_object",
    return_value=b"dummy content",
)
@patch("fastapi_celery.utils.ext_extraction.aws_connection.S3Connector")
@patch(OS_PATH_ISFILE, return_value=True)
def test_get_document_type_order_local(
    mock_s3_connector: MagicMock,
    mock_get_object: MagicMock,
    mock_config: MagicMock,
    mock_logger: MagicMock,
) -> None:
    """Test _get_document_type for a local file classified as ORDER.

    Verifies that the document type is correctly identified as ORDER for a local file.

    Args:
        mock_s3_connector (MagicMock): Mocked S3Connector class.
        mock_get_object (MagicMock): Mocked get_object function.
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_logger (MagicMock): Mocked logger instance.

    Returns:
        None
    """
    file_path = "NOT_MASTER_DATA\\SAP_Master_data.txt"
    processor = FileExtensionProcessor(file_path=file_path, source=SourceType.LOCAL)

    document_type = processor._get_document_type()
    assert document_type == DocumentType.ORDER


# === Test for _load_s3_file ===
@patch("fastapi_celery.utils.ext_extraction.read_n_write_s3.get_object")
@patch("fastapi_celery.utils.ext_extraction.aws_connection.S3Connector")
def test_load_s3_file_success(
    mock_s3_connector_cls: MagicMock, mock_get_object: MagicMock
) -> None:
    """Test successful loading of a file from S3.

    Verifies that the file is loaded correctly from S3, and metadata is set properly.

    Args:
        mock_s3_connector_cls (MagicMock): Mocked S3Connector class.
        mock_get_object (MagicMock): Mocked get_object function.

    Returns:
        None
    """
    file_path = "path/to/file.txt"

    # Setup mock connector and client
    mock_client = MagicMock()
    mock_client.head_object.return_value = {"ContentLength": 1024 * 1024 * 10}
    mock_s3_connector = MagicMock()
    mock_s3_connector.client = mock_client
    mock_s3_connector_cls.return_value = mock_s3_connector

    # Mock buffer return
    mock_buffer = MagicMock()
    mock_get_object.return_value = mock_buffer

    processor = FileExtensionProcessor(file_path=file_path, source=SourceType.S3)

    assert processor.object_buffer == mock_buffer
    assert processor.file_name == Path(file_path).name
    assert processor.file_path_parent == str(Path(file_path).parent) + "/"


@patch("fastapi_celery.utils.ext_extraction.read_n_write_s3.get_object")
@patch("fastapi_celery.utils.ext_extraction.aws_connection.S3Connector")
def test_load_s3_file_not_found(
    mock_s3_connector_cls: MagicMock, mock_get_object: MagicMock
) -> None:
    """Test loading a non-existent file from S3.

    Verifies that a FileNotFoundError is raised when the file cannot be loaded from S3.

    Args:
        mock_s3_connector_cls (MagicMock): Mocked S3Connector class.
        mock_get_object (MagicMock): Mocked get_object function.

    Returns:
        None
    """
    file_path = "path/to/nonexistent_file.txt"

    # Set up the mock client to simulate failure in head_object
    mock_client = MagicMock()
    mock_client.head_object.side_effect = Exception("Simulated head_object failure")

    # Set up the S3Connector to return the mocked client
    mock_s3_connector = MagicMock()
    mock_s3_connector.client = mock_client
    mock_s3_connector_cls.return_value = mock_s3_connector

    # Run the test
    with pytest.raises(
        FileNotFoundError, match=f"File '{file_path}' could not be loaded from S3"
    ):
        FileExtensionProcessor(file_path=file_path, source=SourceType.S3)


# === Log helper ===


@pytest.mark.parametrize("mock_has_handlers", [False])
@patch("logging.getLogger")
def test_logging_config(mock_get_logger: MagicMock, mock_has_handlers: bool) -> None:
    """Test the logging_config function.

    Verifies that logging_config correctly sets up a logger with handlers
    when no handlers are present initially.

    Args:
        mock_get_logger (MagicMock): Mocked logging.getLogger function.
        mock_has_handlers (bool): Whether the logger already has handlers.

    Returns:
        None
    """
    mock_log = mock_get_logger.return_value
    mock_log.hasHandlers.return_value = mock_has_handlers

    # Call the function under test
    logging_config("test_logger")

    # Assertions to ensure your logging configuration logic is being tested
    assert mock_log.handlers
    mock_log.handlers.clear.assert_not_called()


def test_logger_adapter_validation_and_process():
    adapter = ValidatingLoggerAdapter(logger=None, extra={})

    # --- Direct validation tests ---
    # Valid enum members get converted to strings
    extra = {
        "service": ServiceLog.FILE_PROCESSOR,
        "log_type": LogType.ERROR,
    }
    validated = adapter.validate_log_fields(extra.copy())
    assert validated["service"] == "file-processor"
    assert validated["log_type"] == "error"

    # Valid strings that match enums get converted to strings as well
    extra = {
        "service": "database",
        "log_type": "access",
    }
    validated = adapter.validate_log_fields(extra.copy())
    assert validated["service"] == "database"
    assert validated["log_type"] == "access"

    # Invalid service raises ValueError
    with pytest.raises(ValueError):
        adapter.validate_log_fields({"service": "invalid-service"})

    # Invalid log_type raises ValueError
    with pytest.raises(ValueError):
        adapter.validate_log_fields({"log_type": "invalid-log-type"})

    # --- process() method tests ---
    msg = "test message"
    kwargs = {
        "extra": {
            "service": ServiceLog.FILE_PROCESSOR,
            "log_type": LogType.ERROR,
        }
    }

    new_msg, new_kwargs = adapter.process(msg, kwargs)
    assert new_kwargs["extra"]["service"] == "file-processor"
    assert new_kwargs["extra"]["log_type"] == "error"

    # Test invalid fields silently fail and do not raise in process()
    kwargs_invalid = {"extra": {"service": "invalid", "log_type": "invalid"}}
    new_msg, new_kwargs = adapter.process(msg, kwargs_invalid)
    # Should silently ignore validation error and keep extra as a dict
    assert isinstance(new_kwargs["extra"], dict)


class TestAnyJsonInS3Prefix(unittest.TestCase):
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_any_json_in_s3_prefix_found(self, mock_s3_connector):
        # Mock S3 client and paginator
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_page_iterator = [
            {"Contents": [{"Key": "folder/file1.txt"}, {"Key": "folder/data.json"}]},
            {"Contents": [{"Key": "folder/file2.csv"}]}
        ]

        mock_paginator.paginate.return_value = mock_page_iterator
        mock_client.get_paginator.return_value = mock_paginator
        mock_s3_connector.return_value.client = mock_client

        # Clear any previous connectors
        _s3_connectors.clear()
        result = any_json_in_s3_prefix("my-bucket", "folder/")
        self.assertTrue(result)

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_any_json_in_s3_prefix_not_found(self, mock_s3_connector):
        # Mock S3 client and paginator with no JSON files
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_page_iterator = [
            {"Contents": [{"Key": "folder/file1.txt"}, {"Key": "folder/file2.csv"}]}
        ]

        mock_paginator.paginate.return_value = mock_page_iterator
        mock_client.get_paginator.return_value = mock_paginator
        mock_s3_connector.return_value.client = mock_client

        # Clear any previous connectors
        _s3_connectors.clear()
        result = any_json_in_s3_prefix("my-bucket", "folder/")
        self.assertFalse(result)


class TestReadJsonFromS3(unittest.TestCase):
    @patch("fastapi_celery.utils.read_n_write_s3.get_object")
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_read_json_success(self, mock_s3_connector, mock_get_object):
        # Mock S3Connector client and bucket_name
        mock_client = MagicMock()
        mock_s3_connector.return_value.client = mock_client
        mock_s3_connector.return_value.bucket_name = "my-bucket"
        _s3_connectors.clear()

        # Prepare JSON content to return as BytesIO
        sample_data = {"key": "value"}
        json_bytes = io.BytesIO(json.dumps(sample_data).encode("utf-8"))
        mock_get_object.return_value = json_bytes

        result = read_json_from_s3("my-bucket", "path/to/object.json")
        self.assertEqual(result, sample_data)

    @patch("fastapi_celery.utils.read_n_write_s3.get_object")
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_read_json_object_not_found(self, mock_s3_connector, mock_get_object):
        mock_client = MagicMock()
        mock_s3_connector.return_value.client = mock_client
        mock_s3_connector.return_value.bucket_name = "my-bucket"
        _s3_connectors.clear()

        # Simulate object not found
        mock_get_object.return_value = None

        result = read_json_from_s3("my-bucket", "missing.json")
        self.assertIsNone(result)

    @patch("fastapi_celery.utils.read_n_write_s3.get_object")
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_read_json_parse_error(self, mock_s3_connector, mock_get_object):
        mock_client = MagicMock()
        mock_s3_connector.return_value.client = mock_client
        mock_s3_connector.return_value.bucket_name = "my-bucket"
        _s3_connectors.clear()

        # Return invalid JSON content
        invalid_json_bytes = io.BytesIO(b"{invalid json}")
        mock_get_object.return_value = invalid_json_bytes

        result = read_json_from_s3("my-bucket", "invalid.json")
        self.assertIsNone(result)

class TestListObjectsWithPrefix(unittest.TestCase):
    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_list_objects_with_prefix_success(self, mock_s3_connector):
        mock_client = MagicMock()
        mock_paginator = MagicMock()
        mock_page_iterator = [
            {"Contents": [{"Key": "folder/file1.txt"}, {"Key": "folder/file2.json"}]},
            {"Contents": [{"Key": "folder/file3.csv"}]},
        ]
        mock_paginator.paginate.return_value = mock_page_iterator
        mock_client.get_paginator.return_value = mock_paginator
        mock_s3_connector.return_value.client = mock_client

        from fastapi_celery.utils import read_n_write_s3
        read_n_write_s3._s3_connectors.clear()

        keys = read_n_write_s3.list_objects_with_prefix("my-bucket", "folder/")
        self.assertEqual(
            keys, ["folder/file1.txt", "folder/file2.json", "folder/file3.csv"]
        )

    @patch("fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector")
    def test_list_objects_with_prefix_exception(self, mock_s3_connector):
        mock_s3_connector.side_effect = Exception("Simulated error")
        from fastapi_celery.utils import read_n_write_s3
        read_n_write_s3._s3_connectors.clear()

        keys = read_n_write_s3.list_objects_with_prefix("my-bucket", "folder/")
        self.assertEqual(keys, [])

def test_build_success_response_returns_valid_podata():
    """Test build_success_response should return PODataParsed with SUCCESS status"""
    result = pdf_helpers.build_success_response(
        file_path="dummy.pdf",
        document_type=DocumentType.ORDER,
        po_number="PO123",
        items=[{"id": 1}],
        metadata={"key": "value"},
        capacity="test-capacity",
    )

    assert result.original_file_path == Path("dummy.pdf")
    assert result.document_type == DocumentType.ORDER
    assert result.po_number == "PO123"
    assert result.items == [{"id": 1}]
    assert result.metadata == {"key": "value"}
    assert result.capacity == "test-capacity"
    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_build_failed_response_logs_and_sets_failed(caplog):
    """Test build_failed_response logs error and returns FAILED status"""
    exc = ValueError("Simulated error")

    with caplog.at_level(logging.ERROR):
        result = pdf_helpers.build_failed_response(
            file_path="dummy.pdf",
            document_type=DocumentType.ORDER,
            capacity="test-capacity",
            exc=exc,
        )

    assert "Simulated error" in caplog.text

    assert result.original_file_path == Path("dummy.pdf")
    assert result.document_type == DocumentType.ORDER
    assert result.po_number is None
    assert result.items == []
    assert result.metadata == {}
    assert result.capacity == "test-capacity"
    assert result.step_status == StatusEnum.FAILED
    assert any("ValueError" in msg for msg in result.messages)