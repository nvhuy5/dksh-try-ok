import logging.config
import config_loader
from models.tracking_models import LogType, ServiceLog

LOG_COLORS = {
    "DEBUG": "cyan",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}
EXCLUDED_FIELDS = [
    "log.original",
    "process",
    "log.origin",
]

# Using a Bash script to create environment-specific log files.
ENV = config_loader.get_config_value("environment", "env")
LOG_LEVEL = "DEBUG" if ENV == "dev" else "INFO"


def logging_config(logger: str) -> None:
    """
    Configure a logger with console and file handlers.

    Sets up a logger with colored console output and file logging in a temporary directory.

    Args:
        logger (str): Name of the logger to configure.

    Raises:
        PermissionError: If log directory or file cannot be created due to permissions.
        OSError: If there are issues accessing the temporary directory.
    """
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "ecs": {
                    "()": "ecs_logging.StdlibFormatter",
                    "exclude_fields": EXCLUDED_FIELDS,
                },
            },
            "handlers": {
                "console": {
                    "level": "INFO",
                    "class": "logging.StreamHandler",
                    "formatter": "ecs",
                }
            },
            "loggers": {
                f"{logger}": {
                    "level": LOG_LEVEL,
                    "handlers": ["console"],
                    "propagate": False,
                },
            },
            "root": {
                "level": LOG_LEVEL,
                "handlers": ["console"],
            },
        }
    )


class ValidatingLoggerAdapter(logging.LoggerAdapter):
    """
    Validates and normalizes 'service' and 'log_type' fields in the log's extra dictionary.
    Ensures they are members of ServiceLog and LogType enums.

    Args:
        extra (dict): The extra fields passed to the logger.

    Returns:
        dict: The validated and normalized extra fields.
    """

    def validate_log_fields(self, extra: dict) -> dict:
        if "service" in extra:
            service = extra["service"]
            if not isinstance(service, ServiceLog):
                if service in ServiceLog._value2member_map_:
                    service = ServiceLog(service)
                else:
                    raise ValueError(f"Invalid service log value: {service}")
            extra["service"] = str(service)

        if "log_type" in extra:
            log_type = extra["log_type"]
            if not isinstance(log_type, LogType):
                if log_type in LogType._value2member_map_:
                    log_type = LogType(log_type)
                else:
                    raise ValueError(f"Invalid log type value: {log_type}")
            extra["log_type"] = str(log_type)

        return extra

    def process(self, msg, kwargs):
        """
        Start the log validation for extra attribute
        """
        extra = kwargs.get("extra") or {}
        if not isinstance(extra, dict):
            extra = {}
        try:
            kwargs["extra"] = self.validate_log_fields(extra)
        except Exception:
            # silently ignore validation errors to avoid breaking logging
            pass
        return msg, kwargs
