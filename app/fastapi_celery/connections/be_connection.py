import httpx
from typing import Optional, Dict, Any
import traceback
import logging

from models.tracking_models import ServiceLog, LogType
from connections.redis_connection import RedisConnector
from utils import log_helpers
from utils.middlewares.request_context import get_context_value
import config_loader

# ===
# Set up logging
logger_name = "Backend API Connection"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===

API_KEY = config_loader.get_env_variable("JWT_SECRET_KEY")
JWT_TOKEN_KEY = "jwt_token"
jwt_request = {"type": "AUTHENTICATE_DATA_WORKFLOW_CODE"}


class BEConnector:
    """Backend API Connector for making HTTP requests.

    Initializes an HTTP client with a URL and optional body data, and provides
    methods for sending POST, GET, and PUT requests. Logs errors during requests.
    """

    def __init__(
        self,
        api_url: str,
        body_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the connector with an API URL and optional body data.

        Args:
            api_url (str): The URL of the API endpoint.
            body_data (Optional[Dict[str, Any]], optional): Data to send in the request body. Defaults to None.

        """
        # === Try to retrieve all traceability attributes when an object created
        # self.redis_utils = RedisConnector()
        # self.request_id = get_context_value("request_id")
        # self.traceability_context_values = {
        #     key: val
        #     for key in [
        #         "file_path",
        #         "workflow_name",
        #         "workflow_id",
        #         "document_number",
        #         "document_type",
        #     ]
        #     if (val := get_context_value(key)) is not None
        # }
        # logger.debug(
        #     f"Function: {__name__}\n"
        #     f"RequestID: {self.request_id}\n"
        #     f"TraceabilityContext: {self.traceability_context_values}"
        # )

        self.api_url = api_url
        self.body_data = body_data or {}
        self.params = params or {}
        self.metadata = {}

    def post(self) -> Optional[Dict[str, Any]]:
        """Send a POST request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return self._request("POST")

    def get(self) -> Optional[Dict[str, Any]]:
        """Send a GET request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return self._request("GET")

    def put(self) -> Optional[Dict[str, Any]]:
        """Send a PUT request to the API endpoint.

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        return self._request("PUT")

    # async def _request(self, method: str) -> Optional[Dict[str, Any]]:
    #     """Send an HTTP request to the API endpoint using the specified method.

    #     Args:
    #         method (str): HTTP method to use ('POST', 'GET', or 'PUT').

    #     Returns:
    #         Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
    #     """
    #     async with httpx.AsyncClient() as client:
    #         try:
    #             headers = {"X-Token": API_KEY}
    #             response = await client.request(
    #                 method,
    #                 self.api_url,
    #                 headers=headers,
    #                 json=self.body_data,
    #                 params=self.params,
    #             )
    #             response.raise_for_status()
    #             response_data = response.json()
    #             return response_data.get("data", {})
    #         except httpx.HTTPStatusError as e:
    #             short_tb = "".join(
    #                 traceback.format_exception(type(e), e, e.__traceback__, limit=3)
    #             )
    #             logger.error(
    #                 f"{method} error {self.api_url}: {e.response.status_code} - {e.response.text}!\n{short_tb}",
    #                 extra={
    #                     "service": ServiceLog.DATABASE,
    #                     "log_type": LogType.ERROR,
    #                     **self.traceability_context_values,
    #                     "traceability": self.request_id,
    #                 },
    #             )
    #         except Exception as e:
    #             short_tb = "".join(
    #                 traceback.format_exception(type(e), e, e.__traceback__, limit=3)
    #             )
    #             logger.exception(
    #                 f"Unexpected error during {method} request: {str(e)}!\n{short_tb}",
    #                 extra={
    #                     "service": ServiceLog.DATABASE,
    #                     "log_type": LogType.ERROR,
    #                     **self.traceability_context_values,
    #                     "traceability": self.request_id,
    #                 },
    #             )
    #     return None

    def _request(self, method: str) -> Optional[Dict[str, Any]]:
        """Send an HTTP request to the API endpoint using the specified method.

        Args:
            method (str): HTTP method to use ('POST', 'GET', or 'PUT').

        Returns:
            Optional[Dict[str, Any]]: Response data under the 'data' key, or None if request fails.
        """
        try:
            with httpx.Client(verify=False) as client:
                headers = {"X-Token": API_KEY}
                response = client.request(
                    method,
                    self.api_url,
                    headers=headers,
                    json=self.body_data,
                    params=self.params,
                )
                response.raise_for_status()
                response_data = response.json()
                return response_data.get("data", {})
        except httpx.HTTPStatusError as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.error(
                f"{method} error {self.api_url}: {e.response.status_code} - {e.response.text}!\n{short_tb}",
                # extra={
                #     "service": ServiceLog.DATABASE,
                #     "log_type": LogType.ERROR,
                #     **self.traceability_context_values,
                #     "traceability": self.request_id,
                # },
            )
        except Exception as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            logger.exception(
                f"Unexpected error during {method} request: {str(e)}!\n{short_tb}",
                # extra={
                #     "service": ServiceLog.DATABASE,
                #     "log_type": LogType.ERROR,
                #     **self.traceability_context_values,
                #     "traceability": self.request_id,
                # },
            )
        return None


    def get_field(self, key: str) -> Optional[Any]:
        """
        Get a specific field from the metadata dictionary.

        Args:
            key (str): The key of the metadata field to retrieve.

        Returns:
            Optional[Any]: The value associated with the key if present, else None.
        """
        return self.metadata.get(key)

    def __repr__(self) -> str:
        """Return a string representation of the connector.

        Returns:
            str: String representation with metadata keys.
        """
        return f"<POTemplateMetadata keys={list(self.metadata.keys())}>"
