# Standard Library Imports
import io
import json
import logging
import traceback

# Third-Party Imports
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel

# Local Application Imports
from utils import log_helpers
from connections import aws_connection
from models.traceability_models import ServiceLog, LogType
from utils.middlewares.request_context import get_context_value
from typing import Optional

# ===
# === Try to retrieve all traceability attributes when an object created
request_id = get_context_value("request_id")
traceability_context_values = {
    key: val
    for key in [
        "file_path",
        "workflow_name",
        "workflow_id",
        "document_number",
        "document_type",
    ]
    if (val := get_context_value(key)) is not None
}

# === Set up logging ===
logger_name = "Read and Write to S3"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})

_s3_connectors = {}


# === Upload to S3 ===
def put_object(client, bucket_name: str, object_name: str, uploading_data) -> dict:
    """
    Upload data to an S3 bucket.

    Supports uploading from a buffer (BytesIO/StringIO) or a file path.

    Args:
        client: AWS S3 client.
        bucket_name (str): Target S3 bucket name.
        object_name (str): S3 object key.
        uploading_data: Data to upload (buffer or file path).

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed") and optional "error".

    Raises:
        ClientError: If S3 upload fails.
        BotoCoreError: If AWS client encounters an error.
        TypeError: If uploading_data is invalid.
    """
    try:
        if isinstance(uploading_data, (io.BytesIO, io.StringIO)):
            uploading_data.seek(0)
            client.upload_fileobj(uploading_data, Bucket=bucket_name, Key=object_name)
            logger.info(
                f"Buffer uploaded successfully to {bucket_name}/{object_name}.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.TASK,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return {"status": "Success"}
        elif isinstance(uploading_data, str):
            client.upload_file(
                Filename=uploading_data, Bucket=bucket_name, Key=object_name
            )
            logger.info(
                f"File '{uploading_data}' uploaded successfully to {bucket_name}/{object_name}.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.TASK,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return {"status": "Success"}
        else:
            msg = "uploading_data must be either a buffer-like object or a file path"
            logger.error(
                msg,
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ERROR,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return {"status": "Failed", "error": msg}

    except (ClientError, BotoCoreError, TypeError) as e:
        logger.error(
            f"Error uploading to S3: {type(e).__name__} - {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return {"status": "Failed", "error": str(e)}


# === Download from S3 ===
def get_object(client, bucket_name: str, object_name: str) -> io.BytesIO | None:
    """
    Download an object from an S3 bucket.

    Args:
        client: AWS S3 client.
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        io.BytesIO | None: Buffer with object data, or None if download fails.

    Raises:
        ClientError: If S3 object retrieval fails.
        BotoCoreError: If AWS client encounters an error.
    """
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response["Body"].read()
        buffer = io.BytesIO(data)
        logger.info(
            f"Object '{object_name}' read successfully from bucket '{bucket_name}'.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return buffer
    except (ClientError, BotoCoreError) as e:
        logger.error(
            f"Error reading object '{bucket_name}/{object_name}': {type(e).__name__} - {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return None


def copy_object_between_buckets(
    source_bucket: str, source_key: str, dest_bucket: str, dest_key: str
) -> dict:
    """
    Copy an object between S3 buckets.

    Args:
        source_bucket (str): Source S3 bucket name.
        source_key (str): Source S3 object key.
        dest_bucket (str): Destination S3 bucket name.
        dest_key (str): Destination S3 object key.

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed"), source, destination, and optional "error".

    Raises:
        ClientError: If S3 copy operation fails.
        BotoCoreError: If AWS client encounters an error.
    """
    try:
        logger.info(
            f"Start copying from {source_bucket}/{source_key} to {dest_bucket}/{dest_key}"
        )
        s3_connector = aws_connection.S3Connector(bucket_name=source_bucket)
        client = s3_connector.client
        copy_source = {"Bucket": source_bucket, "Key": source_key}

        client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        logger.info(
            f"Object copied successfully to {dest_bucket}/{dest_key}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )

        return {
            "status": "Success",
            "source": {"bucket": source_bucket, "key": source_key},
            "destination": {"bucket": dest_bucket, "key": dest_key},
        }

    except (ClientError, BotoCoreError) as e:
        logger.error(
            f"Error copying object: {type(e).__name__} - {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return {
            "status": "Failed",
            "error": str(e),
            "source": {"bucket": source_bucket, "key": source_key},
            "destination": {"bucket": dest_bucket, "key": dest_key},
        }


# === Check if object exists or get metadata ===
def object_exists(
    client, bucket_name: str, object_name: str
) -> tuple[bool, dict | None]:
    """
    Check if an object exists in an S3 bucket and retrieve its metadata.

    Args:
        client: AWS S3 client.
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        tuple[bool, dict | None]: (True, metadata) if object exists, (False, None) if not or on error.

    Raises:
        ClientError: If S3 metadata retrieval fails (except 404).
    """
    try:
        response = client.head_object(Bucket=bucket_name, Key=object_name)
        logger.info(
            f"Metadata for object '{object_name}' retrieved successfully from bucket '{bucket_name}'.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return True, response
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.warning(
                f"Object '{object_name}' not found in bucket '{bucket_name}'.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ERROR,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return False, None
        logger.error(
            f"Error accessing object metadata: {type(e).__name__} - {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return False, None


def any_json_in_s3_prefix(bucket_name: str, prefix: str) -> bool:
    """
    Check if any .json file exists under the given prefix in S3.

    Args:
        bucket_name (str): The S3 bucket name.
        prefix (str): The S3 prefix path (like a folder).

    Returns:
        bool: True if any .json file exists, False otherwise.
    """
    if bucket_name not in _s3_connectors:
        _s3_connectors[bucket_name] = aws_connection.S3Connector(
            bucket_name=bucket_name
        )
        logger.info(f"Created new S3Connector for bucket: {bucket_name}")
    else:
        logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

    client = _s3_connectors[bucket_name].client
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                return True
    return False


def write_json_to_s3(
    json_data,
    file_record: dict,
    bucket_name: str,
    s3_key_prefix: str = "",
    rerun_attempt: Optional[int] = None,
) -> dict:
    """
    Write JSON data to an S3 bucket.

    Args:
        json_data: JSON-serializable data to upload.
        file_record (dict): File metadata with "file_name" or "object_name".
        bucket_name (str): Target S3 bucket name.
        s3_key_prefix (str, optional): Prefix for S3 object key. Defaults to "".

    Returns:
        dict: Status dictionary with "status" ("Success" or "Failed"), json_data, file_info,
        and optional convert_file_info or error.

    Raises:
        Exception: If S3 connection or JSON upload fails.
    """
    try:
        logger.info(
            f"Start uploading JSON to S3. File record: {file_record}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )

        # Determine extension
        ext = ".json"
        base_name = file_record.get("file_name", "unnamed").rsplit(".", 1)[0]

        # Append rerun_attempt if present
        if rerun_attempt is not None:
            object_name = f"{base_name}_rerun_{rerun_attempt}{ext}"
        else:
            object_name = f"{base_name}{ext}"

        # Apply prefix if given
        if file_record.get("s3_key_prefix", None):
            object_name = f"{file_record['s3_key_prefix'].rstrip('/')}/{base_name}_{file_record['current_time']}{ext}"
            del file_record["s3_key_prefix"]  # Remove to avoid confusion
        elif s3_key_prefix:
            object_name = f"{s3_key_prefix.rstrip('/')}/{object_name}"

        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Create new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket : {bucket_name}")

        s3_connector = _s3_connectors[bucket_name]
        client = s3_connector.client
        bucket = s3_connector.bucket_name
    except Exception as e:
        logger.error(
            f"Error when creating S3 connection: {str(e)}\n{traceback.format_exc()}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return {
            "status": "Failed",
            "error": "S3 connection failed",
            "file_info": file_record,
        }

    try:
        buffer = io.BytesIO()
        payload = (
            json_data.output.model_dump()
            if isinstance(json_data, BaseModel)
            else json_data
        )
        buffer.write(
            json.dumps(
                payload,
                ensure_ascii=False,
                default=str,
            ).encode("utf-8")
        )
        buffer.seek(0)

        upload_result = put_object(client, bucket, object_name, buffer)
        if upload_result.get("status") == "Failed":
            error_message = upload_result.get("error", "Unknown error")
            logger.error(
                f"Upload failed: {error_message}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.ERROR,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return {
                "status": "Failed",
                "error": error_message,
                "file_info": file_record,
            }

        logger.info(
            f"JSON uploaded successfully to {bucket}/{object_name}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return {
            "json_data": json_data,
            "file_info": file_record,
            "convert_file_info": {
                "dest_bucket": bucket,
                "dest_object_name": object_name,
            },
            "status": "Success",
        }

    except Exception as e:
        logger.error(
            f"Upload failed: {str(e)}\n{traceback.format_exc()}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return {"status": "Failed", "error": "Upload failed", "file_info": file_record}


def read_json_from_s3(bucket_name: str, object_name: str) -> dict | None:
    """
    Read a JSON object from S3 and parse it.

    Args:
        bucket_name (str): S3 bucket name.
        object_name (str): S3 object key.

    Returns:
        dict | None: Parsed JSON content, or None if not found or error.
    """
    try:
        logger.info(
            f"Start reading JSON from S3. Bucket: {bucket_name}, Key: {object_name}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )

        # Get or create S3Connector
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Created new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

        s3_connector = _s3_connectors[bucket_name]
        client = s3_connector.client
        bucket = s3_connector.bucket_name

    except Exception as e:
        logger.error(
            f"Error when creating S3 connection: {str(e)}\n{traceback.format_exc()}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return None

    try:
        # Get the object
        buffer = get_object(client, bucket_name=bucket, object_name=object_name)
        if buffer is None:
            logger.warning(
                f"Object not found in S3: {bucket}/{object_name}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.WARNING,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return None

        # Parse JSON
        content = buffer.read().decode("utf-8")
        data = json.loads(content)

        logger.info(
            f"Successfully read and parsed JSON from S3: {bucket}/{object_name}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return data

    except Exception as e:
        logger.error(
            f"Error reading/parsing JSON from S3: {str(e)}\n{traceback.format_exc()}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return None


def list_objects_with_prefix(bucket_name: str, prefix: str) -> list:
    """
    List all object keys in bucket with given prefix.

    Args:
        bucket_name (str): S3 bucket_name
        prefix (str): S3_prefix_key
    Returns:
        list: List of keys (strings) if found, empty if none or error.
    """
    try:
        if bucket_name not in _s3_connectors:
            _s3_connectors[bucket_name] = aws_connection.S3Connector(
                bucket_name=bucket_name
            )
            logger.info(f"Created new S3Connector for bucket: {bucket_name}")
        else:
            logger.info(f"Reusing existing S3Connector for bucket: {bucket_name}")

        client = _s3_connectors[bucket_name].client
        paginator = client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        keys = []
        for page in page_iterator:
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    except Exception as e:
        logger.error(
            f"Error listing objects with prefix '{prefix}' in bucket '{bucket_name}': {e}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **traceability_context_values,
                "traceability": request_id,
            },
        )
        return []
