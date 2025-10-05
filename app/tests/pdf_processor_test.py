import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

from fastapi_celery.models.class_models import SourceType, PODataParsed, StatusEnum
from fastapi_celery.processors.file_processors.pdf_processor import (
    Pdf001Template,
    Pdf002Template,
    Pdf004Template,
    Pdf006Template,
    Pdf007Template,
    Pdf008Template
)


class TestPdf001Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("dummy1.pdf")
        self.sample_text_lines = [
            "訂購編號：PO123456",
            "客戶名稱：測試公司",
            "幣別",
            "品項",
            "-----",
            "商品1",
            "- / -",
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.fitz.open")
    def test_parse_file_to_json(self, mock_fitz_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "medium"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.get_text.return_value = "\n".join(self.sample_text_lines)

        mock_doc = MagicMock()
        mock_doc.__iter__.return_value = [mock_page]
        mock_doc.close = MagicMock()
        mock_fitz_open.return_value = mock_doc

        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertEqual(result.po_number, "PO123456")
        self.assertEqual(result.metadata["客戶名稱"], "測試公司")
        self.assertEqual(len(result.items), 1)

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mocked error"))
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertTrue(any("Mocked error" in msg for msg in result.messages))

class TestPdf002Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("dummy2.pdf")
        self.sample_text_lines = [
            "採購單號：PO20240718",
            "預約退貨時間：2024/07/18 上午11:59:00",
            "※注意事項",
            "1. 商品需完好",
            "2. 不接受已開封",
        ]
        self.sample_table = [
            ["產品編號", "品名", "數量"],
            ["A001", "筆記本", "2"],
            ["A002", "原子筆", "5"],
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_parse_file_to_json(self, mock_pdfplumber_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "small"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "\n".join(self.sample_text_lines)
        mock_page.extract_tables.return_value = [self.sample_table]

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf

        processor = Pdf002Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertEqual(result.po_number, "PO20240718")
        self.assertIn("※注意事項", result.metadata)
        self.assertEqual(len(result.items), 2)
        self.assertEqual(result.items[0]["產品編號"], "A001")

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mocked error"))
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertTrue(any("Mocked error" in msg for msg in result.messages))

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open", side_effect=Exception("PdfPlumber crash"))
    def test_extract_tables_exception(self, mock_pdfplumber):
        processor = Pdf002Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.extract_tables("dummy")
        self.assertEqual(result, [])


class TestPdf004Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("dummy4.pdf")
        self.sample_text_lines = [
            "採購單號：PO004",
            "廠商：佳佳文具",
            "S1234567A 品名A 10 盒 120.00 1200 2024/07/30 5",
            "10入/盒",
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_parse_file_to_json(self, mock_pdfplumber_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "tiny"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "\n".join(self.sample_text_lines)

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf

        processor = Pdf004Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertEqual(result.po_number, "PO004")
        self.assertEqual(result.metadata["廠商"], "佳佳文具")
        self.assertEqual(result.document_type, "order")
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0]["產品編號"], "S1234567A")

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mocked error"))
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertTrue(any("Mocked error" in msg for msg in result.messages))

    def test_build_table_from_items(self):
        processor = Pdf004Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        items = [("S1234567A 品名A 10 盒 120.00 1200 2024/07/30 5", "10入/盒")]
        result = processor.build_table_from_items(items)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["產品編號"], "S1234567A")
        self.assertEqual(result[0]["品名規格"], "品名A 10入/盒")

    def test_parse_item_lines(self):
        processor = Pdf004Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_item_lines(self.sample_text_lines)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], "S1234567A 品名A 10 盒 120.00 1200 2024/07/30 5")
        self.assertEqual(result[0][1], "10入/盒")

    def test_extract_tables_split_by_whitespace(self):
        processor = Pdf004Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        items = processor.extract_tables(self.sample_text_lines)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["產品編號"], "S1234567A")

    def test_extract_tables_exception(self):
        processor = Pdf004Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        with patch.object(processor, "parse_item_lines", side_effect=Exception("test error")):
            result = processor.extract_tables(self.sample_text_lines)
            self.assertEqual(result, [])


class TestPdf006Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("A202405220043.pdf")
        self.sample_text_lines = [
            "訂單號碼：PO006789",
            "廠商名稱：測試供應商",
            "交貨地點：台北市中正區", 
            "123456 PROD001 10 件 2025/08/01", 
            "型號X 訂單ABC 額外說明",  
            "876543 台灣大昌華嘉股份有限公司 附加資訊",
            "12345678台灣大昌華嘉股份有限公司 其他",  
            "※備註1",
            "這是一行備註",
            "※備註2",
            "另一段備註",
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_parse_file_to_json_success(self, mock_pdfplumber_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "large"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "\n".join(self.sample_text_lines)
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf

        processor = Pdf006Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertEqual(result.document_type, "order")

        loc_value = result.metadata.get("交貨地點", "")
        self.assertTrue(loc_value.startswith("台北市中正區"), f"Unexpected 交貨地點: {loc_value}")
        self.assertIn("123456 PROD001 10 件 2025/08/01", loc_value)
        self.assertIn("型號X 訂單ABC 額外說明", loc_value)

        vendor_value = result.metadata.get("廠商名稱", "")
        self.assertTrue(
            vendor_value.startswith("測試供應商") or "台灣大昌華嘉股份有限公司" in vendor_value,
            f"Unexpected 廠商名稱: {vendor_value}"
        )

        self.assertIn("※備註2", result.metadata)
        self.assertEqual(result.metadata["※備註2"], "另一段備註")
        if "※備註1" in result.metadata:
            self.assertEqual(result.metadata["※備註1"], "這是一行備註")

        self.assertGreaterEqual(len(result.items), 1)
        item = result.items[0]
        self.assertEqual(item.get("料號"), "123456")
        self.assertEqual(item.get("產品代碼"), "PROD001")
        self.assertEqual(item.get("交貨日期"), "2025/08/01")
        self.assertEqual(item.get("型號"), "型號X")
        self.assertEqual(item.get("訂購單號"), "訂單ABC")
        self.assertIn("額外說明", item.get("品名／規格／製造廠／型號", ""))

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mocked error 006"))
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertTrue(any("Mocked error" in msg for msg in result.messages))


class TestPdf007Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("O20240620TPB026.pdf")
        self.sample_text_lines = [
            "請購單號：PO007123",
            "供應商：測試供應商",
            "●備註",
            "注意事項1",
            "注意事項2",
            "列印日期：2024/06/20",
        ]
        self.sample_table = [
            ["請購明細單號", "品項", "數量"],
            ["PO007123", "商品A", "10"],
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_parse_file_to_json(self, mock_pdfplumber_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "medium"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "\n".join(self.sample_text_lines)
        mock_page.extract_tables.return_value = [self.sample_table]

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf

        processor = Pdf007Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertEqual(result.po_number, "PO007123")
        self.assertEqual(result.metadata["供應商"], "測試供應商")
        self.assertEqual(result.metadata["●備註"], "注意事項1\n注意事項2")
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0]["請購明細單號"], "PO007123")

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertFalse(any("Mocked error" in msg for msg in result.messages))

    def test_extract_metadata_from_lines(self):
        processor = Pdf007Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.extract_metadata_from_lines(self.sample_text_lines)
        self.assertEqual(result["請購單號"], "PO007123")
        self.assertEqual(result["供應商"], "測試供應商")
        self.assertEqual(result["●備註"], "注意事項1\n注意事項2")

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_extract_tables_exception(self, mock_pdfplumber):
        mock_pdfplumber.side_effect = Exception("PdfPlumber crash")
        processor = Pdf007Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.extract_tables("dummy")
        self.assertEqual(result, [])



class TestPdf008Template(unittest.TestCase):
    def setUp(self):
        self.dummy_path = Path("dummy8.pdf")
        self.sample_text_lines = [
            "廠商:C1921 益品行銷 梧坖",
            "序號: M24081500290",
            "預約退貨時間:2024/08/16",
            "預約退貨時段:",
            "14:",
            "00~14: 59",
            "1 5724081000145 2024/08/11~2024/08/16 11",
        ]

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor")
    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.pdfplumber.open")
    def test_parse_file_to_json_combines_time_and_items(self, mock_pdfplumber_open, mock_file_processor_class):
        mock_processor = MagicMock()
        mock_processor._get_file_capacity.return_value = "34.39 KB"
        mock_processor._get_document_type.return_value = "order"
        mock_processor.source = "local"
        mock_processor.file_path = self.dummy_path
        mock_file_processor_class.return_value = mock_processor

        mock_page = MagicMock()
        mock_page.extract_text.return_value = "\n".join(self.sample_text_lines)
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdfplumber_open.return_value.__enter__.return_value = mock_pdf

        processor = Pdf008Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result: PODataParsed = processor.parse_file_to_json()

        self.assertIsNotNone(result)
        self.assertEqual(result.po_number, "5724081000145")
        self.assertEqual(result.metadata.get("廠商"), "C1921 益品行銷 梧坖")
        self.assertEqual(result.metadata.get("預約退貨時段"), "14:00~14:59")
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0]["退貨單號"], "5724081000145")
        self.assertEqual(result.items[0]["退貨日期"], "2024/08/11~2024/08/16")
        self.assertEqual(result.items[0]["箱數"], "11")
        self.assertEqual(result.items[0]["棧板數"], "1")

    @patch("fastapi_celery.template_processors.file_processors.pdf_processor.ext_extraction.FileExtensionProcessor", side_effect=Exception("Mocked error"))
    def test_parse_file_to_json_exception(self, mock_file_processor_class):
        processor = Pdf001Template(file_path=self.dummy_path, source=SourceType.LOCAL)
        result = processor.parse_file_to_json()

        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.messages, list)
        self.assertTrue(any("Mocked error" in msg for msg in result.messages))


