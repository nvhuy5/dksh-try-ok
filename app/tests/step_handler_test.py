import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
from typing import Any

from fastapi_celery.celery_worker.celery_task import execute_step
from fastapi_celery.models.class_models import WorkflowStep


class TestExecuteStep(unittest.IsolatedAsyncioTestCase):
    """Test case for the execute_step function.

    Verifies the behavior of execute_step under various scenarios,
    including successful execution, missing methods, and exceptions.
    """

    def setUp(self) -> None:
        """Set up the test environment.

        Initializes a mock FileProcessor, mock WorkflowStep,
        and sample data input for testing.

        Returns:
            None
        """
        self.mock_file_processor = MagicMock()
        self.data_input = {"key": "value"}
        self.mock_step = WorkflowStep(
            workflowStepId="step1",
            stepName="TEMPLATE_FILE_PARSE",
            stepOrder=1,
            stepConfiguration=[]
        )

    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_coroutine_method_and_no_input(
        self,
        mock_get_context_value,
        mock_set_context_values
    ) -> None:
        """Test execute_step with a coroutine method and no input data."""

        mock_get_context_value.side_effect = lambda key: {
            "request_id": "test-request-id",
            "workflow_id": "wf1",
            "document_number": "doc1",
            "file_path": "/tmp/testfile.pdf",
            "document_type": "PO"
        }.get(key)
        mock_set_context_values.return_value = None

        self.mock_step.stepName = "TEMPLATE_FILE_PARSE"

        # ✅ Force the step to execute (prevent skipping)
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)

        # ✅ Simulate async step returning expected data
        self.mock_file_processor.parse_file_to_json = AsyncMock(return_value="parsed_data")

        mock_context = {}
        mock_task_id = "test-task-id"

        result = await execute_step(self.mock_file_processor, self.mock_step, mock_context, mock_task_id)

        self.assertEqual(result, "parsed_data")

    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_coroutine_method_and_input(
        self,
        mock_get_context_value,
        mock_set_context_values
    ) -> None:
        ...
        self.mock_step.stepName = "TEMPLATE_FORMAT_VALIDATION"

        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)

        self.mock_file_processor.template_format_validation  = AsyncMock(return_value="validated_data")

        mock_context = {"input_data": "some_input"}
        mock_task_id = "test-task-id"

        result = await execute_step(self.mock_file_processor, self.mock_step, mock_context, mock_task_id)

        self.assertEqual(result, "validated_data")

    @patch("fastapi_celery.celery_worker.step_handler.set_context_values")
    @patch("fastapi_celery.celery_worker.step_handler.get_context_value")
    async def test_execute_step_with_sync_method_and_input(
        self,
        mock_get_context_value,
        mock_set_context_values
    ) -> None:
        """Test execute_step with a synchronous method and input data."""

        mock_get_context_value.side_effect = lambda key: {
            "request_id": "test-request-id",
            "workflow_id": "wf1",
            "document_number": "doc1",
            "file_path": "/tmp/testfile.pdf",
            "document_type": "PO"
        }.get(key)
        mock_set_context_values.return_value = None

        self.mock_step.stepName = "write_json_to_s3"

        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)

        self.mock_file_processor.write_json_to_s3 = MagicMock(return_value="written")

        mock_task_id = "test-task-id"
        context = {"input_data": {"key": "value"}}

        result = await execute_step(self.mock_file_processor, self.mock_step, context, mock_task_id)

        self.assertEqual(result, "written")

    async def test_execute_step_with_missing_method(self) -> None:
        """Test execute_step with a missing method.

        Verifies that the function returns None when the specified method
        does not exist on the processor.

        Returns:
            None
        """
        self.mock_step.stepName = "non_existent_method"
        setattr(self.mock_file_processor, "non_existent_method", None)

        mock_task_id = "test-task-id"
        with self.assertRaises(ValueError) as context:
            await execute_step(self.mock_file_processor, self.mock_step, self.data_input, mock_task_id)

        self.assertIn("No step configuration found for step", str(context.exception))

    async def test_execute_step_raises_exception_in_method(self) -> None:
        """Test execute_step when the method raises an exception.

        Verifies that the function returns None when the method
        (parse_file_to_json) raises a RuntimeError.

        Returns:
            None
        """
        self.mock_step.stepName = "TEMPLATE_FILE_PARSE"
        # Force execute_step to NOT skip the step
        self.mock_file_processor.check_step_result_exists_in_s3 = MagicMock(return_value=None)

        # Simulate error from the actual method
        self.mock_file_processor.parse_file_to_json = AsyncMock(
            side_effect=RuntimeError("Boom")
        )

        mock_context = {}
        mock_task_id = "test-task-id"

        with self.assertRaises(RuntimeError) as context:
            await execute_step(
                self.mock_file_processor, self.mock_step, mock_context, mock_task_id
            )

        self.assertIn("Boom", str(context.exception))
