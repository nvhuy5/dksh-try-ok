import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path
from fastapi_celery.models.class_models import SourceType, PODataParsed
from fastapi_celery.template_processors.common.txt_file_processor import (
    BaseTXTProcessor,
)
from fastapi_celery.template_processors.file_processors.txt_processor_new import (
    Txt001Template,
    Txt002Template,
    Txt003Template,
    Txt004Template,
)


class TestTXTTemplates(unittest.TestCase):
    def test_txt_001_double_space_split(self):
        processor = Txt001Template(Path("dummy_path"), source=SourceType.LOCAL)
        lines = ["Item1  Item2  Item3", "Val1  Val2  Val3"]
        expected = [
            {"col_1": "Item1", "col_2": "Item2", "col_3": "Item3"},
            {"col_1": "Val1", "col_2": "Val2", "col_3": "Val3"},
        ]
        self.assertEqual(processor.parse_space_separated_lines(lines), expected)

    def test_txt_002_tab_split(self):
        processor = Txt002Template(Path("dummy_path"), source=SourceType.LOCAL)
        lines = ["Name\tAge\tLocation", "Alice\t30\tNY", "Bob\t25\tLA"]
        expected = [
            {"col_1": "Name", "col_2": "Age", "col_3": "Location"},
            {"col_1": "Alice", "col_2": "30", "col_3": "NY"},
            {"col_1": "Bob", "col_2": "25", "col_3": "LA"},
        ]
        self.assertEqual(processor.parse_tab_separated_lines(lines), expected)

    def test_txt_003_single_space_split(self):
        processor = Txt003Template(Path("dummy_path"), source=SourceType.LOCAL)
        lines = ["One Two Three", "1 2 3"]
        expected = [
            {"col_1": "One", "col_2": "Two", "col_3": "Three"},
            {"col_1": "1", "col_2": "2", "col_3": "3"},
        ]
        self.assertEqual(processor.parse_space_separated_lines(lines), expected)

    def test_txt_004_tabular_with_headers(self):
        processor = Txt004Template(Path("dummy_path"), source=SourceType.LOCAL)
        lines = [
            "2024.07.11  Dynamic List Display",
            "HeaderText\tBatch\tQty",
            "ItemA\tB001\t10",
            "ItemB\tB002\t20",
        ]
        expected = [
            {"HeaderText": "ItemA", "Batch": "B001", "Qty": "10"},
            {"HeaderText": "ItemB", "Batch": "B002", "Qty": "20"},
        ]
        self.assertEqual(processor.parse_tabular_data_with_headers(lines), expected)


class TestBaseTXTProcessor(unittest.TestCase):
    @patch(
        "fastapi_celery.template_processors.common.txt_file_processor.ext_extraction"
        ".FileExtensionProcessor"
    )
    def test_parse_file_to_json_with_mocked_file(self, mock_ext_processor_class):
        # === Mock Setup ===
        mock_ext_processor = MagicMock()
        mock_ext_processor._get_file_capacity.return_value = "mocked_capacity"
        mock_ext_processor._get_document_type.return_value = "order"
        mock_ext_processor.source = "local"
        mock_ext_processor.file_path = "fake/path/to/file.txt"

        # The file content we simulate
        fake_file_content = "line1\nline2\nline3"

        # Patch open to return the simulated file content
        mock_open = unittest.mock.mock_open(read_data=fake_file_content)

        # Replace the actual class instantiation with our mock
        mock_ext_processor_class.return_value = mock_ext_processor

        # === Initialize BaseTXTProcessor ===
        processor = BaseTXTProcessor(
            file_path=Path("fake/path/to/file.txt"), source=SourceType.LOCAL
        )

        # Patch the built-in open method only within the context of this test
        with patch("builtins.open", mock_open):
            # Custom line parser function
            def dummy_parser(lines):
                return [{"line": line} for line in lines]

            # === Run parse_file_to_json ===
            result: PODataParsed = processor.parse_file_to_json(dummy_parser)

        # === Assertions ===
        self.assertEqual(result.original_file_path, Path("fake/path/to/file.txt"))
        self.assertEqual(result.document_type, "order")
        self.assertEqual(result.capacity, "mocked_capacity")
        self.assertEqual(result.po_number, "3")
        self.assertEqual(len(result.items), 3)
        self.assertEqual(result.items[0], {"line": "line1"})
