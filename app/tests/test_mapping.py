import pytest
from unittest.mock import patch, AsyncMock
from pathlib import Path

from fastapi_celery.models.class_models import (
    PODataParsed,
    DocumentType,
    StatusEnum,
    StepOutput,
)
from app.fastapi_celery.processors.workflow_processors.template_mapping import (
    template_data_mapping,
)


# === Fixture: sample PODataParsed ===
@pytest.fixture
def sample_po_parsed():
    return PODataParsed(
        original_file_path=Path("/path/to/template.xlsx"),
        document_type=DocumentType.ORDER,
        po_number="PO12345",
        items=[{"header1": "123", "header2": "456"}],
        metadata={"supplier": "ABC"},
        capacity="small",
        step_status=None,
        messages=None,
    )


# === Tests for template_data_mapping ===

@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.mapping.BEConnector")
async def test_template_data_mapping_success(mock_connector, sample_po_parsed):
    """Parse API + Mapping API success → SUCCESS"""

    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    mock_instance.get.side_effect = [
        [{"templateFileParse": {"id": "template123"}}],  # parse API
        {
            "templateMappingHeaders": [
                {"header": "header1", "fromHeader": "renamed_col", "order": 1},
                {"header": "header2", "fromHeader": "Unmapping", "order": 2},
            ]
        },  # mapping API
    ]
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    result = await template_data_mapping(DummySelf(), step_output)
    assert result.step_status == StatusEnum.SUCCESS
    assert result.step_failure_message is None
    # Ensure header1 was renamed
    assert "renamed_col" in result.output.items[0]


@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.mapping.BEConnector")
async def test_template_data_mapping_failed_parse_api(mock_connector, sample_po_parsed):
    """Parse API returns None → FAILED"""

    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    mock_instance.get.return_value = None
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    result = await template_data_mapping(DummySelf(), step_output)
    assert result.step_status == StatusEnum.FAILED
    assert "Failed to call template-parse API" in result.step_failure_message[0]


@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.mapping.BEConnector")
async def test_template_data_mapping_failed_mapping_api(mock_connector, sample_po_parsed):
    """Mapping API returns invalid response → FAILED"""

    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    mock_instance.get.side_effect = [
        [{"templateFileParse": {"id": "template123"}}],  # parse API
        None,                                            # mapping API fail
    ]
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    result = await template_data_mapping(DummySelf(), step_output)
    assert result.step_status == StatusEnum.FAILED
    assert "Mapping API did not return a valid response" in result.step_failure_message[0]


@pytest.mark.asyncio
@patch("fastapi_celery.template_processors.workflow_nodes.mapping.BEConnector")
async def test_template_data_mapping_no_headers_matched(mock_connector, sample_po_parsed):
    """Mapping API returns headers but none match the dataframe → FAILED"""

    class DummySelf:
        workflow_step_ids = {"TEMPLATE_FILE_PARSE": "step123"}

    mock_instance = AsyncMock()
    mock_instance.get.side_effect = [
        [{"templateFileParse": {"id": "template123"}}],  # parse API
        {
            "templateMappingHeaders": [
                {"header": "nonexistent", "fromHeader": "renamed_col", "order": 1},
            ]
        },  # mapping API
    ]
    mock_connector.return_value = mock_instance

    step_output = StepOutput(
        output=sample_po_parsed,
        step_status=StatusEnum.SUCCESS,
        step_failure_message=None,
    )

    result = await template_data_mapping(DummySelf(), step_output)
    assert result.step_status == StatusEnum.FAILED
    assert "Mapping failed: expected headers not found" in result.step_failure_message[0]