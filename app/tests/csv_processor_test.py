import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from fastapi_celery.models.class_models import SourceType, PODataParsed
from fastapi_celery.processors.file_processors.csv_processor import (
    CSVProcessor,
)
from fastapi_celery.utils import ext_extraction


class TestCSVProcessor(unittest.TestCase):

    def setUp(self):
        self.dummy_path = Path("dummy.csv")
        self.sample_csv = (
            "PO Number：PO123456\n"
            "Supplier：Acme Corp\n"
            "Item,Quantity,Price\n"
            "Widget A,10,2.5\n"
            "Widget B,5,5.0\n"
        ).encode("utf-8")

    @patch(
        "fastapi_celery.template_processors.file_processors.csv_processor"
        ".PODataParsed"
    )
    @patch(
        "fastapi_celery.template_processors.file_processors.csv_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    @patch(
        "fastapi_celery.template_processors.file_processors.csv_processor"
        ".config_loader.get_env_variable"
    )
    def test_parse_file_to_json(
        self, mock_get_env, mock_file_processor, mock_podata_parsed
    ):
        mock_get_env.return_value = "："

        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = self.dummy_path
        mock_file._get_file_capacity.return_value = "small"
        mock_file._get_document_type.return_value = "order"
        mock_file_processor.return_value = mock_file

        mock_podata_parsed_instance = MagicMock()
        mock_podata_parsed.return_value = mock_podata_parsed_instance

        with patch(
            "builtins.open", new=unittest.mock.mock_open(read_data=self.sample_csv)
        ) as mock_open:
            mock_open.return_value.read.return_value = self.sample_csv

            processor = CSVProcessor(file_path=self.dummy_path, source=SourceType.LOCAL)
            result = processor.parse_file_to_json()

            self.assertEqual(result, mock_podata_parsed_instance)
            mock_podata_parsed.assert_called_once()

    @patch(
        "fastapi_celery.template_processors.file_processors.csv_processor"
        ".ext_extraction.FileExtensionProcessor"
    )
    def test_is_likely_header(self, mock_file_processor):
        mock_file = MagicMock()
        mock_file.source = "local"
        mock_file.file_path = self.dummy_path
        mock_file._get_file_capacity.return_value = "small"
        mock_file._get_document_type.return_value = "order"
        mock_file_processor.return_value = mock_file

        csv_data = "Column A,Column B,Column C\n123,456,789\n".encode("utf-8")
        with patch("builtins.open", new=unittest.mock.mock_open(read_data=csv_data)):
            mock_file.object_buffer = None
            processor = CSVProcessor(file_path=self.dummy_path, source=SourceType.LOCAL)

            header = ["Product", "Quantity", "Price"]
            not_header = ["123", "456", "789"]
            self.assertTrue(processor.is_likely_header(header))
            self.assertFalse(processor.is_likely_header(not_header))
