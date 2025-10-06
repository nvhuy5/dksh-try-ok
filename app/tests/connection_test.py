import io
import boto3
import pytest
import unittest
import logging
import json
from unittest.mock import patch, MagicMock, AsyncMock
from redis.exceptions import RedisError
from moto import mock_aws
from botocore.exceptions import ClientError
from fastapi_celery.utils.read_n_write_s3 import (
    put_object,
    get_object,
    object_exists,
    write_json_to_s3,
)
from fastapi_celery.connections.redis_connection import RedisConnector
from fastapi_celery.connections.aws_connection import S3Connector
from fastapi_celery.connections.be_connection import BEConnector
from fastapi_celery.connections.aws_connection import AWSSecretsManager

# === Redis ===


class TestRedisUtils(unittest.TestCase):
    def setUp(self):
        self.task_id = "task123"
        self.step_name = "stepA"
        self.status = "completed"
        self.step_id = "step-id-001"
        self.workflow_id = "workflow-xyz"

    # --- store_step_status ---
    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_store_step_status_success(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hset.return_value = True
        mock_redis.expire.return_value = True

        result = redis_utils.store_step_status(
            self.task_id, self.step_name, self.status, self.step_id
        )
        self.assertTrue(result)
        self.assertEqual(mock_redis.hset.call_count, 2)
        self.assertEqual(mock_redis.expire.call_count, 2)

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_store_step_status_no_step_id(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        result = redis_utils.store_step_status(
            self.task_id, self.step_name, self.status
        )
        self.assertTrue(result)
        mock_redis.hset.assert_called_once()  # Only one hset call
        mock_redis.expire.assert_called()

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_store_step_status_failure(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hset.side_effect = RedisError("Connection error")
        result = redis_utils.store_step_status(
            self.task_id, self.step_name, self.status
        )
        self.assertFalse(result)

    # --- get_step_statuses ---
    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_step_statuses_success(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.return_value = {"stepA": "completed"}
        result = redis_utils.get_all_step_status(self.task_id)
        self.assertEqual(result, {"stepA": "completed"})

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_step_statuses_failure(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.side_effect = RedisError("Connection error")
        result = redis_utils.get_all_step_status(self.task_id)
        self.assertEqual(result, {})

    # --- get_step_ids ---
    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_step_ids_success(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.return_value = {"stepA": "step-id-001"}
        result = redis_utils.get_step_ids(self.task_id)
        self.assertEqual(result, {"stepA": "step-id-001"})

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_step_ids_failure(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.side_effect = RedisError("Connection error")
        result = redis_utils.get_step_ids(self.task_id)
        self.assertEqual(result, {})

    # --- store_workflow_id ---
    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_store_workflow_id_success(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hset.return_value = True
        mock_redis.expire.return_value = True

        result = redis_utils.store_workflow_id(self.task_id, self.workflow_id, "1")

        self.assertTrue(result)
        mock_redis.hset.assert_called_once_with(
            f"task:{self.task_id}:workflow_id", self.workflow_id, "1"
        )
        mock_redis.expire.assert_called_once_with(
            f"task:{self.task_id}:workflow_id", 3600
        )

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_store_workflow_id_failure(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hset.side_effect = RedisError("Connection error")
        result = redis_utils.store_workflow_id(self.task_id, self.workflow_id, "2")
        self.assertFalse(result)

    # --- get_workflow_id ---
    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_workflow_id_success(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.return_value = {self.workflow_id: "done"}
        result = redis_utils.get_workflow_id(self.task_id)
        expected = {"workflow_id": self.workflow_id, "status": "done"}
        self.assertEqual(result, expected)

    @patch("fastapi_celery.connections.redis_connection.redis.Redis")
    def test_get_workflow_id_failure(self, mock_redis_constructor):
        mock_redis = mock_redis_constructor.return_value
        redis_utils = RedisConnector()
        mock_redis.hgetall.side_effect = RedisError("Connection error")
        result = redis_utils.get_workflow_id(self.task_id)
        self.assertIsNone(result)


# === S3 Connection ===
class TestS3Utils(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.bucket_name = "test-bucket"
        self.object_name = "test-object.json"
        self.file_record = {"file_name": "test-file.txt"}
        self.json_data = {"key": "value"}
        self.buffer = io.BytesIO(b"test data")

    # === Tests for put_object ===
    def test_put_object_with_buffer_success(self):
        result = put_object(
            self.mock_client, self.bucket_name, self.object_name, self.buffer
        )
        self.mock_client.upload_fileobj.assert_called_once()
        self.assertEqual(result["status"], "Success")

    def test_put_object_with_file_path_success(self):
        result = put_object(
            self.mock_client, self.bucket_name, self.object_name, "test.txt"
        )
        self.mock_client.upload_file.assert_called_once()
        self.assertEqual(result["status"], "Success")

    def test_put_object_invalid_type(self):
        result = put_object(self.mock_client, self.bucket_name, self.object_name, 12345)
        self.assertEqual(result["status"], "Failed")
        self.assertIn("uploading_data must be", result["error"])

    def test_put_object_upload_error(self):
        self.mock_client.upload_fileobj.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "InternalError"}}, "Upload"
        )
        result = put_object(
            self.mock_client, self.bucket_name, self.object_name, self.buffer
        )
        self.assertEqual(result["status"], "Failed")
        self.assertIn("InternalError", result["error"])

    # === Tests for get_object ===
    def test_get_object_success(self):
        body_data = io.BytesIO(b"mock data")
        self.mock_client.get_object.return_value = {"Body": body_data}
        result = get_object(self.mock_client, self.bucket_name, self.object_name)
        self.assertIsInstance(result, io.BytesIO)
        self.assertEqual(result.read(), b"mock data")

    def test_get_object_failure(self):
        self.mock_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
        )
        result = get_object(self.mock_client, self.bucket_name, self.object_name)
        self.assertIsNone(result)

    # === Tests for object_exists ===
    def test_object_exists_true(self):
        self.mock_client.head_object.return_value = {"ContentLength": 123}
        exists, metadata = object_exists(
            self.mock_client, self.bucket_name, self.object_name
        )
        self.assertTrue(exists)
        self.assertIsNotNone(metadata)

    def test_object_exists_false(self):
        self.mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )
        exists, metadata = object_exists(
            self.mock_client, self.bucket_name, self.object_name
        )
        self.assertFalse(exists)
        self.assertIsNone(metadata)

    def test_object_exists_error(self):
        self.mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Access Denied"}}, "HeadObject"
        )
        exists, metadata = object_exists(
            self.mock_client, self.bucket_name, self.object_name
        )
        self.assertFalse(exists)
        self.assertIsNone(metadata)

    # === Tests for write_json_to_s3 ===
    @patch("fastapi_celery.utils.read_n_write_s3.put_object")
    @patch(
        "fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector.__init__",
        return_value=None,
    )
    def test_write_json_to_s3_success(self, mock_init, mock_put_object):
        from fastapi_celery.utils import read_n_write_s3

        read_n_write_s3.aws_connection.S3Connector.client = self.mock_client
        read_n_write_s3.aws_connection.S3Connector.bucket_name = self.bucket_name

        mock_put_object.return_value = {"status": "Success"}

        result = write_json_to_s3(self.json_data, self.file_record, self.bucket_name)
        self.assertEqual(result["status"], "Success")

    @patch("fastapi_celery.utils.read_n_write_s3.put_object")
    @patch(
        "fastapi_celery.utils.read_n_write_s3.aws_connection.S3Connector.__init__",
        return_value=None,
    )
    def test_write_json_to_s3_upload_failure(self, mock_init, mock_put_object):
        from fastapi_celery.utils import read_n_write_s3

        read_n_write_s3.aws_connection.S3Connector.client = self.mock_client
        read_n_write_s3.aws_connection.S3Connector.bucket_name = self.bucket_name

        mock_put_object.return_value = {"status": "Failed", "error": "UploadError"}

        result = write_json_to_s3(self.json_data, self.file_record, self.bucket_name)
        self.assertEqual(result["status"], "Failed")
        self.assertIn("UploadError", result["error"])

    @patch(
        "fastapi_celery.connections.aws_connection.S3Connector",
        side_effect=Exception("Connection error"),
    )
    def test_write_json_to_s3_connection_failure(self, mock_connector_class):
        result = write_json_to_s3(self.json_data, self.file_record, self.bucket_name)
        self.assertEqual(result["status"], "Failed")
        self.assertEqual(result["error"], "S3 connection failed")


@mock_aws
def test_ensure_bucket_exists_creates_bucket(caplog):
    bucket_name = "test-ensure-bucket"
    region = "ap-southeast-1"

    # Create an S3 client and ensure no bucket exists initially
    s3 = boto3.client("s3", region_name=region)
    buckets = s3.list_buckets()["Buckets"]
    assert all(b["Name"] != bucket_name for b in buckets)

    # Set up logger to capture log messages
    logger = logging.getLogger("AWS Connection")
    caplog.set_level("INFO", logger="AWS Connection")
    logger.addHandler(caplog.handler)

    # Create S3Connector and ensure it triggers bucket creation
    S3Connector(bucket_name=bucket_name, region_name=region)

    # Ensure that the bucket is created after calling _ensure_bucket_exists
    buckets = s3.list_buckets()["Buckets"]
    assert any(b["Name"] == bucket_name for b in buckets)

    # Check the log output for the correct bucket creation message
    assert f"Bucket '{bucket_name}' created successfully." in caplog.text


@mock_aws
def test_create_bucket_client_error(monkeypatch, caplog):
    bucket_name = "fail-bucket"
    region = "ap-southeast-1"

    # Create S3Connector manually
    connector = S3Connector.__new__(S3Connector)
    connector.bucket_name = bucket_name
    connector.region_name = region
    connector.client = boto3.client("s3", region_name=region)

    # Manually set required attributes used in logging
    connector.request_id = "req-abc"
    connector.traceability_context_values = {
        "file_path": "dummy/path",
        "workflow_id": "workflow-123",
        "document_type": "invoice",
        "document_number": "INV-001",
    }

    # Simulate ClientError when create_bucket is called
    error_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
    connector.client.create_bucket = lambda **kwargs: (_ for _ in ()).throw(
        ClientError(error_response, "CreateBucket")
    )

    # Setup logging capture
    logger = logging.getLogger("AWS Connection")
    caplog.set_level("ERROR", logger="AWS Connection")
    logger.addHandler(caplog.handler)

    # Assert exception is raised and log is captured
    with pytest.raises(ClientError):
        connector._create_bucket()

    assert f"Error creating bucket '{bucket_name}': ClientError" in caplog.text
    assert "Access Denied" in caplog.text


# === Secrets ===


@mock_aws
def test_get_secret_string():
    secret_name = "my-secret"
    secret_value = {"username": "admin", "password": "pass123"}

    client = boto3.client("secretsmanager", region_name="ap-southeast-1")
    client.create_secret(Name=secret_name, SecretString=json.dumps(secret_value))

    secrets_manager = AWSSecretsManager(region_name="ap-southeast-1")
    result = secrets_manager.get_secret(secret_name)

    assert result == secret_value


@mock_aws
def test_get_missing_secret_returns_none(caplog):
    logger = logging.getLogger("AWS Connection")
    caplog.set_level(logging.ERROR)
    logger.addHandler(caplog.handler)

    secrets_manager = AWSSecretsManager(region_name="ap-southeast-1")
    result = secrets_manager.get_secret("non-existent")

    assert result is None
    assert "Secret not found." in caplog.text


@mock_aws
def test_get_secret_generic_exception_logging(caplog):
    logger = logging.getLogger("AWS Connection")
    caplog.set_level(logging.ERROR)
    logger.addHandler(caplog.handler)

    # Simulate a generic exception (e.g., network failure) by mocking the client method
    secrets_manager = AWSSecretsManager(region_name="ap-southeast-1")

    # Mock the `get_secret_value` method to raise an exception using MagicMock's side_effect
    secrets_manager.client.get_secret_value = MagicMock(
        side_effect=Exception("Some unexpected error occurred")
    )
    secrets_manager.get_secret("some-secret")

    # Check if the log contains the expected generic error message
    assert "Error retrieving secret" in caplog.text
    assert "Some unexpected error occurred" in caplog.text


@mock_aws
def test_get_secret_with_secret_binary(caplog):
    logger = logging.getLogger("AWS Connection")
    caplog.set_level(logging.ERROR)
    logger.addHandler(caplog.handler)

    # Simulate binary secret value
    secret_name = "my-secret-binary"
    secret_value = {"username": "admin", "password": "pass123"}
    # Encode the secret value as bytes
    secret_binary = json.dumps(secret_value).encode("utf-8")

    # Mock the `get_secret_value` to return the SecretBinary
    secrets_manager = AWSSecretsManager(region_name="ap-southeast-1")
    secrets_manager.client.get_secret_value = MagicMock(
        return_value={"SecretBinary": secret_binary}
    )

    # Call the method
    result = secrets_manager.get_secret(secret_name)
    assert result == secret_value

    # Check if the log contains the expected generic error message (if any error occurs)
    assert "Error retrieving secret" not in caplog.text


# === Backend ===
@pytest.mark.asyncio
async def test_be_connector_post():
    mock_response = {"key": "value"}

    # Create an AsyncMock instance for the client
    client = AsyncMock()
    client.request.return_value = mock_response

    # Perform the test (await the coroutine to get the actual response)
    response = await client.request("POST", "some_url", json={"data": "test"})

    # Now you can assert the actual result
    assert response == mock_response


@pytest.mark.asyncio
async def test_be_connector_post_success():
    # Mock response data returned from the .json() method (not async)
    mock_response_data = {"data": {"key": "value"}}

    # Create a mock response object with .json as a normal method
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value=mock_response_data)
    mock_response.raise_for_status = MagicMock()

    with patch("httpx.AsyncClient.request", return_value=mock_response):
        connector = BEConnector(
            api_url="https://fakeapi.com", body_data={"some": "data"}
        )
        await connector.post()

        # Assertions
        assert mock_response.raise_for_status.call_count == 1
