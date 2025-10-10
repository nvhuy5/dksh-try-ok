import pytest
import unittest
import sys
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from tempfile import NamedTemporaryFile
from openpyxl import Workbook
from app.fastapi_celery.processors.master_processors.excel_master_processor import (
    ExcelMasterProcessor,
)
from fastapi_celery.models.class_models import (
    DocumentType,
    MasterDataParsed,
    DocumentType,
    StatusEnum,
)
from fastapi_celery.models.class_models import PathEncoder, SourceType
from app.fastapi_celery.processors.master_processors import (
    txt_master_processor,
)
from app.fastapi_celery.processors.workflow_processors.master_validation import (
    MasterValidation,
)


@pytest.fixture
def base_path() -> Path:
    """Provide the base path to the samples directory.

    Returns:
        Path: The path to the samples directory relative to the test file.
    """
    return Path(__file__).parent / "samples"


def test_masterdata_itemvalue(base_path: Path) -> None:
    """Test parsing of master data file and verify item values.

    Parses a master data file and checks the file name, capacity format,
    and ensures capacity includes 'KB'.

    Args:
        base_path (Path): The base path to the samples directory.

    Returns:
        None
    """
    file_path = base_path / "SAP_Master_data.txt"
    masterdata = txt_master_processor.TxtMasterProcessor(
        file_path, SourceType.LOCAL
    ).parse_file_to_json()

    print(masterdata)
    file_path = Path(masterdata.original_file_path)
    assert file_path.name == "SAP_Master_data.txt"
    assert isinstance(masterdata.capacity, str)
    assert "KB" in masterdata.capacity


def test_path_encoder_serializes_path_platform_aware() -> None:
    """Test PathEncoder serialization of Path objects.

    Verifies that Path objects are serialized in a platform-aware manner
    (POSIX format) when converted to JSON.

    Returns:
        None
    """
    if sys.platform.startswith("win"):
        path = Path("C:\\Users\\test\\file.txt")
        expected = '{"path": "C:/Users/test/file.txt"}'
    else:
        path = Path("/home/test/file.txt")
        expected = '{"path": "/home/test/file.txt"}'

    data = {"path": path}
    result = json.dumps(data, cls=PathEncoder)
    assert result == expected


@patch(
    "fastapi_celery.template_processors.master_data_processors."
    "txt_master_data_processor.ext_extraction.FileExtensionProcessor"
)
def test_parse_file_to_json_from_s3(mock_processor_class: MagicMock) -> None:
    """Test parsing master data file from S3 source.

    Mocks the FileExtensionProcessor to simulate an S3 source and verifies
    the parsed headers, items, and capacity.

    Args:
        mock_processor_class (MagicMock): Mocked FileExtensionProcessor class.

    Returns:
        None
    """
    mock_processor = MagicMock()
    mock_processor.source = "s3"
    mock_processor._get_file_capacity.return_value = "2.5 KB"
    mock_processor.file_name = "sample.txt"
    mock_processor.file_extension = ".txt"
    mock_processor._get_document_type.return_value = DocumentType.MASTER_DATA
    mock_processor.object_buffer.read.return_value = (
        b"# Table: Products\nid|name\n1|Apple\n2|Banana"
    )
    mock_processor.object_buffer.seek.return_value = None
    mock_processor_class.return_value = mock_processor

    processor = txt_master_processor.TxtMasterProcessor(
        file_path=Path("s3://dummy-path/sample.txt"), source=SourceType.S3
    )
    result = processor.parse_file_to_json()

    assert result.headers == {"Products": ["id", "name"]}
    assert result.items == {
        "Products": [{"id": "1", "name": "Apple"}, {"id": "2", "name": "Banana"}]
    }
    assert result.capacity == "2.5 KB"


def test_parse_file_to_json_from_real_file(base_path: Path) -> None:
    """Test parsing master data file from a local source.

    Parses a real master data file and verifies the presence and type
    of headers, items, and capacity in the result.

    Args:
        base_path (Path): The base path to the samples directory.

    Returns:
        None
    """
    file_path = base_path / "SAP_Master_data.txt"
    processor = txt_master_processor.TxtMasterProcessor(
        file_path, SourceType.LOCAL
    )
    result = processor.parse_file_to_json()

    result_dict = result.model_dump()
    assert "headers" in result_dict
    assert "items" in result_dict
    assert "capacity" in result_dict
    assert isinstance(result_dict["headers"], dict)
    assert isinstance(result_dict["items"], dict)


# MASTERDATA Validation
@pytest.fixture
def sample_masterdata_parsed():
    return MasterDataParsed(
        original_file_path=Path("/path/to/file.xlsx"),
        headers=["id", "name", "amount", "date"],
        document_type=DocumentType.MASTER_DATA,
        items=[
            {"id": "1", "name": "Item A", "amount": "100", "date": "20240101"},
            {"id": "2", "name": "Item B", "amount": "200", "date": "20240102"},
        ],
        step_status=None,
        messages=None,
        capacity="small",
    )


def test_header_validation_success(sample_masterdata_parsed):
    schema = [
        {"name": "id", "posidx": 0},
        {"name": "name", "posidx": 1},
        {"name": "amount", "posidx": 2},
        {"name": "date", "posidx": 3},
    ]

    validator = MasterValidation(sample_masterdata_parsed)
    result = validator.header_validation(schema)

    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_header_validation_mismatch(sample_masterdata_parsed):
    schema = [
        {"name": "id", "posidx": 0},
        {"name": "wrong_name", "posidx": 1},
        {"name": "amount", "posidx": 2},
        {"name": "date", "posidx": 3},
    ]

    validator = MasterValidation(sample_masterdata_parsed)
    result = validator.header_validation(schema)

    assert result.step_status == StatusEnum.FAILED
    assert "Mismatch at position 1: expected 'wrong_name', got 'name'" in result.messages


def test_data_validation_success(sample_masterdata_parsed):
    schema = [
        {"name": "id", "datatype": "int", "nullable": False},
        {"name": "name", "datatype": "string", "nullable": False, "maxlength": 10},
        {"name": "amount", "datatype": "float", "nullable": False},
        {"name": "date", "datatype": "timestamp", "nullable": False},
    ]

    validator = MasterValidation(sample_masterdata_parsed)
    result = validator.data_validation(schema)

    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_data_validation_missing_column(sample_masterdata_parsed):
    schema = [
        {"name": "nonexistent", "datatype": "string", "nullable": True},
    ]

    validator = MasterValidation(sample_masterdata_parsed)
    result = validator.data_validation(schema)

    assert result.step_status == StatusEnum.FAILED
    assert "Missing column 'nonexistent' in data." in result.messages


def test_data_validation_null_field():
    parsed = MasterDataParsed(
        original_file_path=Path("/path/to/file.xlsx"),
        headers=["id"],
        document_type=DocumentType.MASTER_DATA,
        items=[
            {"id": ""},
        ],
        step_status=None,
        messages=None,
        capacity="small",
    )

    schema = [{"name": "id", "datatype": "int", "nullable": False}]
    validator = MasterValidation(parsed)
    result = validator.data_validation(schema)

    assert result.step_status == StatusEnum.FAILED
    assert "Row 0: Field 'id' is required but missing/empty." in result.messages


def test_data_validation_type_error():
    parsed = MasterDataParsed(
        original_file_path=Path("/path/to/file.xlsx"),
        headers=["amount"],
        document_type=DocumentType.MASTER_DATA,
        items=[
            {"amount": "abc"},  # invalid float
        ],
        step_status=None,
        messages=None,
        capacity="small",
    )

    schema = [{"name": "amount", "datatype": "float", "nullable": False}]
    validator = MasterValidation(parsed)
    result = validator.data_validation(schema)

    assert result.step_status == StatusEnum.FAILED
    assert "Row 0: Field 'amount' has invalid value 'abc' for type 'float'." in result.messages


class TestExcelMasterdataProcessor(unittest.TestCase):
    def setUp(self):
        # Create a temporary Excel file
        self.temp_file = NamedTemporaryFile(suffix=".xlsx", delete=False)
        self.file_path = Path(self.temp_file.name)
        wb = Workbook()
        ws = wb.active

        # Add metadata
        ws.append(["Supplier：ABC Company"])
        ws.append(["PO Number：12345"])

        # Add table header and rows
        ws.append(["Item", "Quantity", "Price"])
        ws.append(["Apple", "10", "1.5"])
        ws.append(["Banana", "20", "0.75"])

        wb.save(self.temp_file.name)

    def tearDown(self):
        self.temp_file.close()
        self.file_path.unlink()  # Delete the temp file

    def test_parse_file_to_json(self):
        processor = ExcelMasterProcessor(
            file_path=self.file_path, source=SourceType.LOCAL
        )
        parsed_data = processor.parse_file_to_json()

        self.assertEqual(parsed_data.original_file_path, self.file_path)
        self.assertEqual(parsed_data.headers, ["Item", "Quantity", "Price"])
        self.assertEqual(len(parsed_data.items), 2)
        self.assertEqual(parsed_data.items[0]["Item"], "Apple")
        self.assertEqual(parsed_data.items[1]["Price"], "0.75")
