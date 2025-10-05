import unittest
from unittest.mock import patch
from fastapi_celery.processors.workflow_processors.write_raw_to_s3 import write_raw_to_s3, get_destination_bucket, get_source_bucket
from fastapi_celery.models.class_models import StepOutput, StatusEnum, DocumentType


class DummySelf:
    document_type = None
    request_id = "req-123"
    project_name = "DKSH_TW"
    sap_masterdata = None
    traceability_context_values = {}


class TestWriteRawToS3(unittest.TestCase):
    def setUp(self):
        self.dummy_self = DummySelf()

    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.StepOutput")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.get_context_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.list_objects_with_prefix")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
    def test_with_matching_keys(self, mock_get_config, mock_list_objects, mock_copy, mock_ctx, mock_step_output):
        mock_step_output.return_value = StepOutput(
            output={"msg": "ok"},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
        mock_ctx.side_effect = lambda key: {
            "request_id": "req-123",
            "project_name": "DKSH_TW",
            "sap_masterdata": None,
        }.get(key, None)
        mock_get_config.side_effect = lambda section, key: f"{key}_bucket"
        mock_copy.return_value = {"msg": "ok"}
        mock_list_objects.return_value = [
            "versioning/testfile/001/",
            "versioning/testfile/002/",
        ]

        result = write_raw_to_s3(self.dummy_self, "testfile.txt")
        self.assertIsInstance(result, StepOutput)
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)

    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.StepOutput")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.get_context_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.list_objects_with_prefix")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
    def test_with_no_matching_keys(self, mock_get_config, mock_list_objects, mock_copy, mock_ctx, mock_step_output):
        mock_step_output.return_value = StepOutput(
            output={"msg": "ok"},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
        mock_ctx.side_effect = lambda key: {
            "request_id": "req-123",
            "project_name": "DKSH_TW",
            "sap_masterdata": None,
        }.get(key, None)
        mock_get_config.side_effect = lambda section, key: f"{key}_bucket"
        mock_copy.return_value = {"msg": "ok"}
        mock_list_objects.return_value = ["some/other/path/"]

        result = write_raw_to_s3(self.dummy_self, "testfile.txt")
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)

    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.StepOutput")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.get_context_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.list_objects_with_prefix")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
    def test_with_empty_existing_keys(self, mock_get_config, mock_list_objects, mock_copy, mock_ctx, mock_step_output):
        mock_step_output.return_value = StepOutput(
            output={"msg": "ok"},
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )
        mock_ctx.side_effect = lambda key: {
            "request_id": "req-123",
            "project_name": "DKSH_TW",
            "sap_masterdata": None,
        }.get(key, None)
        mock_get_config.side_effect = lambda section, key: f"{key}_bucket"
        mock_copy.return_value = {"msg": "ok"}
        mock_list_objects.return_value = []

        result = write_raw_to_s3(self.dummy_self, "testfile.txt")
        self.assertEqual(result.step_status, StatusEnum.SUCCESS)

    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.StepOutput")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.get_context_value")
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.read_n_write_s3.copy_object_between_buckets", side_effect=Exception("Mocked S3 error"))
    @patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
    def test_exception_handling(self, mock_get_config, mock_copy, mock_ctx, mock_step_output):
        mock_step_output.return_value = StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=["Exception in write_raw_to_s3: Mocked S3 error"],
        )
        mock_ctx.side_effect = lambda key: {
            "request_id": "req-123",
            "project_name": "DKSH_TW",
            "sap_masterdata": None,
        }.get(key, None)
        mock_get_config.side_effect = lambda section, key: f"{key}_bucket"

        result = write_raw_to_s3(self.dummy_self, "testfile.txt")
        self.assertEqual(result.step_status, StatusEnum.FAILED)
        self.assertIsInstance(result.step_failure_message, list)
        self.assertIn("Mocked S3 error", result.step_failure_message[0])

# ==== Tests for get_source_bucket ==== #

@patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
def test_get_source_bucket_tw(mock_get_config):
    mock_get_config.return_value = "bucket_tw"
    result = get_source_bucket(DocumentType.MASTER_DATA, "DKSH_TW")
    assert result == "bucket_tw"
    mock_get_config.assert_called_once_with("s3_buckets", "datahub_s3_raw_data_tw")


@patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
def test_get_source_bucket_vn(mock_get_config):
    mock_get_config.return_value = "bucket_vn"
    result = get_source_bucket(DocumentType.MASTER_DATA, "DKSH_VN")
    assert result == "bucket_vn"
    mock_get_config.assert_called_once_with("s3_buckets", "datahub_s3_raw_data_vn")


def test_get_source_bucket_invalid_project(caplog):
    result = get_source_bucket(DocumentType.MASTER_DATA, "UNKNOWN")
    assert result is None


def test_get_source_bucket_invalid_document_type(caplog):
    result = get_source_bucket("NOT_MASTER", "DKSH_TW")
    assert result is None


# ==== Tests for get_destination_bucket ==== #

@patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
def test_get_destination_bucket_sap_masterdata(mock_get_config):
    mock_get_config.return_value = "sap_bucket"
    result = get_destination_bucket("DKSH_TW", True)
    assert result == "sap_bucket"
    mock_get_config.assert_called_once_with("s3_buckets", "sap_masterdata_bucket")


@patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
def test_get_destination_bucket_tw(mock_get_config):
    mock_get_config.return_value = "bucket_tw"
    result = get_destination_bucket("DKSH_TW", False)
    assert result == "bucket_tw"
    mock_get_config.assert_called_once_with("s3_buckets", "tw_masterdata_bucket")


@patch("fastapi_celery.template_processors.workflow_nodes.write_raw_to_s3.config_loader.get_config_value")
def test_get_destination_bucket_vn(mock_get_config):
    mock_get_config.return_value = "bucket_vn"
    result = get_destination_bucket("DKSH_VN", False)
    assert result == "bucket_vn"
    mock_get_config.assert_called_once_with("s3_buckets", "vn_masterdata_bucket")


def test_get_destination_bucket_invalid(caplog):
    result = get_destination_bucket("UNKNOWN", False)
    assert result is None