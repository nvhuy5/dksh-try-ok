import pytest
from pathlib import Path
from unittest.mock import patch, AsyncMock

from fastapi_celery.models.class_models import (
    PODataParsed,
    DocumentType,
    StatusEnum,
    StepOutput,
)
from fastapi_celery.template_processors.workflow_nodes.template_validation import (
    TemplateValidation,
    template_format_validation,
)


# === Fixture: create valid PODataParsed ===
@pytest.fixture
def sample_po_parsed():
    return PODataParsed(
        original_file_path=Path("/path/to/template.xlsx"),
        document_type=DocumentType.ORDER,
        po_number="PO12345",
        items=[
            {"col_1": "123", "col_2": "ABC", "col_3": "2024-01-01"},
            {"col_1": "456", "col_2": "XYZ", "col_3": "2024-01-02"},
        ],
        metadata={"supplier": "ABC"},
        capacity="small",
        step_status=None,
        messages=None,
    )



# === Direct class tests ===

def test_data_validation_success(sample_po_parsed):
    """Tất cả dữ liệu hợp lệ → SUCCESS"""
    schema = [
        {"order": 1, "dataType": "Number", "metadata": '{"required": true}'},
        {"order": 2, "dataType": "String", "metadata": '{"required": true, "maxLength": 10}'},
        {"order": 3, "dataType": "Date", "metadata": '{"required": true}'},
    ]
    validator = TemplateValidation(sample_po_parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.messages is None


def test_data_validation_required_missing(sample_po_parsed):
    """Missing required field → FAILED"""
    schema = [{"order": 1, "dataType": "Number", "metadata": '{"required": true, "allowEmpty": false}'}]
    parsed = sample_po_parsed.model_copy(update={"items": [{"col_1": ""}]})
    validator = TemplateValidation(parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.FAILED
    assert any("required but empty" in msg for msg in result.messages)


def test_data_validation_max_length_error(sample_po_parsed):
    """String exceeds maxLength → FAILED"""
    schema = [{"order": 2, "dataType": "String", "metadata": '{"maxLength": 3}'}]
    parsed = sample_po_parsed.model_copy(update={"items": [{"col_2": "TOO_LONG"}]})
    validator = TemplateValidation(parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.FAILED
    assert any("exceeds maxLength" in msg for msg in result.messages)


def test_data_validation_regex_error(sample_po_parsed):
    """Regex mismatch → FAILED"""
    schema = [{"order": 2, "dataType": "String", "metadata": '{"regex": "^[A-Z]{3}$"}'}]
    parsed = sample_po_parsed.model_copy(update={"items": [{"col_2": "wrong"}]})
    validator = TemplateValidation(parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.FAILED
    assert any("does not match regex" in msg for msg in result.messages)


def test_data_validation_invalid_number(sample_po_parsed):
    """Invalid number → FAILED"""
    schema = [{"order": 1, "dataType": "Number"}]
    parsed = sample_po_parsed.model_copy(update={"items": [{"col_1": "abc"}]})
    validator = TemplateValidation(parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.FAILED
    assert any("is not a valid number" in msg for msg in result.messages)


def test_data_validation_invalid_date(sample_po_parsed):
    """Invalid date → FAILED"""
    schema = [{"order": 3, "dataType": "Date"}]
    parsed = sample_po_parsed.model_copy(update={"items": [{"col_3": "invalid"}]})
    validator = TemplateValidation(parsed)
    result = validator.data_validation(schema)
    assert result.step_status == StatusEnum.FAILED
    assert any("is not a valid date" in msg for msg in result.messages)


# === Orchestration function tests ===

@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.template_validation.BEConnector")
async def test_template_format_validation_success(mock_connector, sample_po_parsed):
    """OOrchestration: API parse + validate succeed → SUCCESS"""
    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    schema = [
        {"order": 1, "dataType": "Number", "metadata": '{"required": true}'},
    ]

    mock_instance = AsyncMock()
    mock_instance.get.side_effect = [
        [{"templateFileParse": {"id": "template123"}}], 
        {"data": {"columns": schema}},                 
        {"data": {"columns": schema}},                 
    ]
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None
    )
    result = await template_format_validation(DummySelf(), step_output)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.output.step_status == StatusEnum.SUCCESS


@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.template_validation.BEConnector")
async def test_template_format_validation_failed_parse_api(mock_connector, sample_po_parsed):
    """Orchestration: parse API fail → FAILED"""
    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    # Only need to fail at the first call, so no long side_effect required
    mock_instance.get.return_value = None
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None
    )
    result = await template_format_validation(DummySelf(), step_output)
    assert result.step_status == StatusEnum.FAILED
    assert "Failed to call template-parse API" in result.step_failure_message[0]


@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.template_validation.BEConnector")
async def test_template_format_validation_schema_missing(mock_connector, sample_po_parsed):
    """Orchestration: schema columns empty → FAILED"""
    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    mock_instance.get.side_effect = [
        [{"templateFileParse": {"id": "template123"}}], 
        {"data": {"columns": []}},                     
        {"data": {"columns": []}},                      
    ]
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None
    )
    result = await template_format_validation(DummySelf(), step_output)
    assert result.step_status == StatusEnum.FAILED
    assert "Schema columns not found" in result.step_failure_message[0]
