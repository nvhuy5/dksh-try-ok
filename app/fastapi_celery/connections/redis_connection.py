# Standard Library Imports
import logging
import traceback
from typing import Optional, Dict

# Third-Party Imports
import redis
from redis.exceptions import RedisError

import config_loader
from utils import log_helpers
from models.tracking_models import ServiceLog, LogType
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = "Redis Connection Config"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===

# === REDIS database === #
# Connect to Redis
logger.debug("Starting Redis Connection...")


# === Store per-task workflow step statuses in Redis === #
class RedisConnector:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=config_loader.get_env_variable("REDIS_HOST", "localhost"),
            port=config_loader.get_env_variable("REDIS_PORT", 6379),
            password=None,
            db=0,
            decode_responses=True,
        )

    def store_step_status(
        self,
        task_id: str,
        step_name: str,
        status: str,
        step_id: Optional[str] = None,
        ttl: int = 3600,
    ) -> bool:
        """Store the status and optional ID of a workflow step in Redis.

        Stores the step status and ID (if provided) in Redis hashes, sets a TTL for the keys,
        and logs errors if the operation fails.

        Args:
            task_id (str): The ID of the task.
            step_name (str): The name of the workflow step.
            status (str): The status of the step.
            step_id (Optional[str], optional): The ID of the step. Defaults to None.
            ttl (int, optional): Time-to-live for the Redis keys in seconds. Defaults to 3600.

        Returns:
            bool: True if the operation succeeds, False otherwise.
        """
        logger.info(f"Finish tracking StepID: {step_id}...")
        try:
            step_status_key = f"task:{task_id}:step_statuses"
            step_ids_key = f"task:{task_id}:step_ids"

            self.redis_client.hset(step_status_key, step_name, status)
            if step_id:
                self.redis_client.hset(step_ids_key, step_name, step_id)

            self.redis_client.expire(step_status_key, ttl)
            self.redis_client.expire(step_ids_key, ttl)

            return True
        except RedisError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"Redis error while storing step status for {task_id}: {e}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                },
            )
            return False

    def get_all_step_status(self, task_id: str) -> Dict[str, str]:
        """
        Retrieve the status of all steps for a task from Redis
        Fetches all step status stored in a Redis hash for the given task ID.
        Logs errors if the operation fails.

        Args:
            task_id (str): The ID of the task.

        Returns:
            Dict[str, str]: A dictionary mapping step names to their status, or empty dict if the operation fails.
        """
        try:
            step_status_key = f"task:{task_id}:step_status"
            return self.redis_client.hgetall(step_status_key)
        except RedisError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"Redis error fetching step status for {task_id}: {e}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                },
            )
            return {}

    def get_step_ids(self, task_id: str) -> Dict[str, str]:
        """Retrieve the IDs of all steps for a task from Redis.

        Fetches all step IDs stored in a Redis hash for the given task ID.
        Logs errors if the operation fails.

        Args:
            task_id (str): The ID of the task.

        Returns:
            Dict[str, str]: A dictionary mapping step names to their IDs, or empty dict if the operation fails.
        """
        try:
            step_ids_key = f"task:{task_id}:step_ids"
            return self.redis_client.hgetall(step_ids_key)
        except RedisError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"Redis error fetching step IDs for {task_id}: {e}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                },
            )
            return {}

    # === Store and retrieve workflow ID associated with a task === #
    def store_workflow_id(
        self, task_id: str, workflow_id: str, status: str, ttl: int = 3600
    ) -> bool:
        """Store the workflow ID associated with a task in Redis.

        Stores the workflow ID in Redis with a TTL and logs errors if the operation fails.

        Args:
            task_id (str): The ID of the task.
            workflow_id (str): The ID of the workflow.
            ttl (int, optional): Time-to-live for the Redis key in seconds. Defaults to 3600.

        Returns:
            bool: True if the operation succeeds, False otherwise.
        """
        logger.info(
            f"Update WorkflowID: {workflow_id} for TaskID: {task_id} to Redis",
            extra={
                "service": ServiceLog.DATABASE,
                "log_type": LogType.ACCESS,
                "task_id": task_id,
            },
        )
        try:
            key = f"task:{task_id}:workflow_id"
            self.redis_client.hset(key, workflow_id, status)
            self.redis_client.expire(key, ttl)
            logger.info(
                f"Updated workflow status: {workflow_id} => {status} in Redis key {key}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                },
            )
            return True
        except RedisError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"Redis error while storing workflow ID for {task_id}: {e}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                },
            )
            return False

    def get_workflow_id(self, task_id: str) -> Optional[Dict[str, str]]:
        """Retrieve the workflow ID associated with a task from Redis.

        Fetches the workflow ID stored in Redis for the given task ID.
        Logs errors if the operation fails.

        Args:
            task_id (str): The ID of the task.

        Returns:
            Optional[Dict[str, str]]: The workflow ID and its status,
            or None if the operation fails or the key does not exist.
        """
        try:
            key = f"task:{task_id}:workflow_id"
            logger.info(
                f"Find the workflow_id using task_id: {task_id}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ACCESS,
                    "task_id": task_id,
                },
            )
            hash_data = self.redis_client.hgetall(key)
            if not hash_data:
                return None

            # Since only one item expected, get it:
            workflow_id, status = next(iter(hash_data.items()))
            return {"workflow_id": workflow_id, "status": status}
        except RedisError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"Redis error while fetching workflow ID for {task_id}: {e}!\n{short_tb}",
                extra={
                    "service": ServiceLog.DATABASE,
                    "log_type": LogType.ERROR,
                    "task_id": task_id,
                },
            )
            return None

    def store_jwt_token(self, token: str, ttl: int) -> bool:
        """
        Store a JWT token in Redis with a specified time-to-live (TTL).

        Args:
            token: The JWT token to store.
            ttl: Time-to-live in seconds for the token in Redis.

        Returns:
            bool: True if the token was stored successfully, False otherwise.
        """
        try:
            key = "jwt_token"
            self.redis_client.set(key, token, ex=ttl)
            logger.info("Updated JWT token to Redis")
            return True
        except RedisError as e:
            logger.error(f"Redis error while updating JWT token: {e}")
            return False

    def get_jwt_token(self) -> Optional[str]:
        """
        Retrieve a JWT token from Redis.

        Returns:
            Optional[str]: The JWT token if found, None otherwise.
        """
        try:
            key = "jwt_token"
            token = self.redis_client.get(key)
            if token:
                logger.info("Found the JWT token from Redis")
                return token
            return None
        except RedisError as e:
            logger.error(f"Redis error while retrieving JWT token: {e}")
            return None
