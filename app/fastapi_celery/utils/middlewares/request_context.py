import contextvars
from typing import Optional
from app.fastapi_celery.models.tracking_models import TrackingModel

# Context variable to store TraceabilityContextModel
traceability_context: contextvars.ContextVar[Optional[TrackingModel]] = (
    contextvars.ContextVar("traceability_context", default=None)
)


def set_context_values(**kwargs) -> None:
    """
    Dynamically set or update fields in the traceability context.

    You must provide 'request_id' if no context is currently set.

    Accepts any valid field defined in TraceabilityContextModel.
    """
    current_context = traceability_context.get()

    # Filter only valid fields based on model schema
    valid_fields = TrackingModel.model_fields.keys()
    filtered_kwargs = {
        tracing_key: value
        for tracing_key, value in kwargs.items()
        if tracing_key in valid_fields and value is not None
    }
    if current_context:
        updated_context = current_context.model_copy(update=filtered_kwargs)
    else:
        if "request_id" not in filtered_kwargs:
            raise ValueError("request_id must be provided if no context is set yet.")
        updated_context = TrackingModel(**filtered_kwargs)

    traceability_context.set(updated_context)


def get_context() -> Optional[TrackingModel]:
    """
    Get the current traceability context.

    Returns:
        Optional[TraceabilityContextModel]: The current context or None.
    """
    return traceability_context.get()


def get_context_value(key: str) -> Optional[str]:
    """
    Retrieve a specific field from the current context.

    Args:
        key (str): Attribute name (e.g., "request_id", "document_number").

    Returns:
        Optional[str]: The value if exists, else None.
    """
    ctx = get_context()
    return getattr(ctx, key, None) if ctx else None
