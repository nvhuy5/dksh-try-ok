import pytest
from types import SimpleNamespace
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi_celery.celery_worker.celery_task import task_execute, handle_task
from fastapi_celery.models.class_models import StartStep, MasterDataParsed, StatusEnum, StepOutput
from typing import Dict, Any
from fastapi_celery.utils.middlewares import request_context
from fastapi_celery.celery_worker.celery_task import update_masterdata_proceed_output
from app.fastapi_celery.processors.helpers import template_helper
# === Test for task_execute ===


@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_success(mock_handle_task: AsyncMock) -> None:
    """Test successful execution of task_execute.

    Verifies that task_execute calls handle_task and returns a success message.
    """
    result = task_execute(file_path="test/path.pdf", project_name="test_project", celery_id="123-abc", source="SFTP")
    assert result == "Task completed successfully"
    mock_handle_task.assert_awaited_once_with(
        "test/path.pdf",
        "123-abc",
        None
    )


@patch("fastapi_celery.celery_worker.celery_task.handle_task", new_callable=AsyncMock)
def test_task_execute_failure(mock_handle_task: AsyncMock) -> None:
    """Test task_execute when handle_task raises an exception.

    Verifies that task_execute returns a failure message when handle_task fails.

    Args:
        mock_handle_task (AsyncMock): Mocked handle_task function.

    Returns:
        None
    """
    mock_handle_task.side_effect = Exception("Boom!")
    with pytest.raises(Exception):
        task_execute.apply(args=("test/path.pdf", "123-abc", "test_project", "source")).get()
    assert mock_handle_task.call_count == 4


# === Test for handle_task ===
class DummyModel:
    def model_copy(self, update=None):
        # simulate Pydantic's model_copy method
        # Return a new instance or dict as needed
        new = DummyModel()
        # You can store updated fields if you want, or just ignore
        return new

    def model_dump(self, exclude_none=False):
        return {"dummy": "data"}

@pytest.mark.asyncio
@patch.dict(
    "fastapi_celery.celery_worker.celery_task.STEP_DEFINITIONS",
    {
        "TEMPLATE_FILE_PARSE": SimpleNamespace(type="mock_step", config={}, store_materialized_data=True),
        "MASTER_DATA_FILE_PARSER": SimpleNamespace(type="mock_step", config={}, store_materialized_data=True),
    },
    clear=True,
)
@patch("fastapi_celery.celery_worker.celery_task.set_context_values")
@patch("fastapi_celery.celery_worker.celery_task.get_context_value")
@patch("fastapi_celery.celery_worker.celery_task.FileProcessor")
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
@patch("fastapi_celery.celery_worker.celery_task.StartStep")
@patch("fastapi_celery.celery_worker.celery_task.execute_step", new_callable=AsyncMock)
@patch("fastapi_celery.celery_worker.celery_task.config_loader.get_config_value")
@patch("fastapi_celery.celery_worker.celery_task.template_utils.parse_data")
async def test_handle_task_success(
    mock_parse_data: MagicMock,
    mock_config: MagicMock,
    mock_execute_step: AsyncMock,
    mock_start_step: MagicMock,
    mock_be_connector: MagicMock,
    mock_file_processor: MagicMock,
    mock_get_context: MagicMock,
    mock_set_context: MagicMock,
) -> None:
    """Test successful execution of handle_task with mocked step definitions."""
    mock_parse_data.return_value = DummyModel()
    mock_set_context.return_value = None
    mock_get_context.return_value = request_context.TrackingModel(request_id="test-request-id")
    mock_config.side_effect = lambda section, key: {
        "converted_files": "test-bucket",
        "materialized_step_data_loc": "materialized/data/path"
    }.get(key, "[]")

    mock_instance = MagicMock()
    mock_instance.file_record = {
        "file_path_parent": "parent/path",
        "file_name": "file.pdf",
        "file_extension": ".pdf"
    }
    mock_file_processor.return_value = mock_instance

    workflow_resp = {
        "id": "workflow-1",
        "name": "Test Workflow",
        "workflowSteps": [
            {"stepName": "TEMPLATE_FILE_PARSE", "workflowStepId": "step-1", "stepOrder": 1, "stepConfiguration": []},
            {"stepName": "MASTER_DATA_FILE_PARSER", "workflowStepId": "step-2", "stepOrder": 2, "stepConfiguration": []}
        ]
    }
    session_resp = {"id": "session-1", "status": "started"}
    step_start_1 = {"workflowHistoryId": "history-1", "status": "started"}
    step_start_2 = {"workflowHistoryId": "history-2", "status": "started"}

    be_mock = AsyncMock()
    be_mock.post.side_effect = [
        workflow_resp, session_resp, step_start_1, {}, step_start_2, {}, {}
    ]
    mock_be_connector.return_value = be_mock

    def side_effect_start_step(**kwargs) -> StartStep:
        return StartStep(**kwargs)

    mock_start_step.side_effect = side_effect_start_step

    def side_effect_execute_step(
        file_processor: Any,
        step: Any,
        context: Dict[str, Any],
        celery_id: str,
        rerun_attempt: int
    ) -> StepOutput:
        context["materialized_step_data_loc"] = "materialized/data/path"
        return StepOutput(
            output={"result": "output"},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    mock_execute_step.side_effect = side_effect_execute_step

    # Call the actual function
    await handle_task("test/path/file.pdf", "celery-123", None)

    # Assertions
    print(f"BEConnector post calls: {be_mock.post.call_count}")
    assert be_mock.post.call_count == 7
    assert mock_execute_step.await_count == 2


@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.FileProcessor")
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
@patch("fastapi_celery.celery_worker.celery_task.config_loader.get_config_value")
async def test_handle_task_no_workflow(
    mock_config: MagicMock,
    mock_be_connector: MagicMock,
    mock_file_processor: MagicMock
) -> None:
    """Test handle_task when no workflow is returned.

    Verifies that the function handles the absence of a workflow gracefully
    and only makes the initial backend call.

    Args:
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_be_connector (MagicMock): Mocked BEConnector class.
        mock_file_processor (MagicMock): Mocked FileProcessor class.

    Returns:
        None
    """
    mock_config.side_effect = lambda section, key: "test-bucket"

    mock_instance = MagicMock()
    mock_instance.file_record = {
        "file_path_parent": "parent/path",
        "file_name": "file.pdf",
        "file_extension": ".pdf"
    }
    mock_file_processor.return_value = mock_instance

    be_mock = AsyncMock()
    be_mock.post.return_value = None
    mock_be_connector.return_value = be_mock

    await handle_task("some/path.pdf", "celery-456", None)

    assert be_mock.post.await_count == 1


@pytest.mark.asyncio
@patch("fastapi_celery.celery_worker.celery_task.set_context_values")
@patch("fastapi_celery.celery_worker.celery_task.get_context_value")
@patch("fastapi_celery.celery_worker.celery_task.FileProcessor")
@patch("fastapi_celery.celery_worker.celery_task.BEConnector")
@patch("fastapi_celery.celery_worker.celery_task.config_loader.get_config_value")
async def test_handle_task_session_fail(
    mock_config: MagicMock,
    mock_be_connector: MagicMock,
    mock_file_processor: MagicMock,
    mock_get_context: MagicMock,
    mock_set_context: MagicMock,
) -> None:
    """Test handle_task when session creation fails.

    Verifies that the function handles session creation failure
    and only makes the expected backend calls.

    Args:
        mock_config (MagicMock): Mocked config_loader.get_config_value function.
        mock_be_connector (MagicMock): Mocked BEConnector class.
        mock_file_processor (MagicMock): Mocked FileProcessor class.

    Returns:
        None
    """
    mock_set_context.return_value = None
    mock_get_context.return_value = request_context.TrackingModel(request_id="test-request-id")
    mock_config.side_effect = lambda section, key: "test-bucket"

    mock_instance = MagicMock()
    mock_instance.file_record = {
        "file_path_parent": "parent/path",
        "file_name": "file.pdf",
        "file_extension": ".pdf"
    }
    mock_file_processor.return_value = mock_instance

    workflow_resp = {
        "id": "workflow-1",
        "name": "Test Workflow",
        "workflowSteps": []
    }

    be_mock = AsyncMock()
    be_mock.post.side_effect = [workflow_resp, None]
    mock_be_connector.return_value = be_mock

    await handle_task("file.pdf", "celery-999", "test_project")
    assert be_mock.post.await_count == 2


@pytest.fixture
def mock_file_processor():
    return MagicMock(
        file_record={
            "file_name": "sample_file.csv",
            "file_extension": ".csv"
        },
        current_time="20250802T120000",
        target_bucket_name="test-bucket",
        document_type="SOME_DOCUMENT_TYPE"
    )


@patch("fastapi_celery.celery_worker.celery_task.read_n_write_s3.read_json_from_s3")
@patch("fastapi_celery.celery_worker.celery_task.read_n_write_s3.write_json_to_s3")
@patch("fastapi_celery.celery_worker.celery_task.template_utils.parse_data")
def test_update_masterdata_proceed_output_success(
    mock_parse_data,
    mock_write_json,
    mock_read_json,
    mock_file_processor
):
    # Arrange
    mock_read_json.return_value = {"some": "raw_data"}

    parsed_data = MasterDataParsed(
        original_file_path="/dummy/path",
        headers=["Col1", "Col2"],
        document_type="master_data",
        items=[{"Col1": "val1", "Col2": "val2"}],
        step_status=StatusEnum.SUCCESS,
        capacity="100",
    )
    mock_parse_data.return_value = parsed_data

    step_names = ["MASTER_DATA_LOAD_DATA"]
    context = {"workflow_detail": {"step": "done"}}

    # Act
    update_masterdata_proceed_output(mock_file_processor, step_names, context)

    # Assert
    expected_key = "process_data/sample_file/sample_file_20250802T120000.json"
    mock_read_json.assert_called_once_with(
        bucket_name="test-bucket",
        object_name=expected_key
    )
    mock_write_json.assert_called_once()
    written_data = mock_write_json.call_args.kwargs["json_data"]
    assert written_data["json_output"] == expected_key
    assert written_data["workflow_detail"] == {"step": "done"}
