import traceback
import logging
from models.class_models import PODataParsed, StatusEnum

logger = logging.getLogger("PDF Helper")


def build_success_response(file_path, document_type, po_number, items, metadata, capacity):
    return PODataParsed(
        original_file_path=file_path,
        document_type=document_type,
        po_number=po_number,
        items=items,
        metadata=metadata,
        step_status=StatusEnum.SUCCESS,
        messages=None,
        capacity=capacity,
    )


def build_failed_response(file_path, document_type=None, capacity=None, exc: Exception = None):
    logger.error(f"Error while parse file JSON from PDF: {exc}")
    short_tb = "".join(
        traceback.format_exception(type(exc), exc, exc.__traceback__, limit=3)
    )
    return PODataParsed(
        original_file_path=file_path,
        document_type=document_type if document_type in ("master_data", "order") else "order",
        po_number=None,
        items=[],
        metadata={},
        step_status=StatusEnum.FAILED,
        messages=[short_tb],
        capacity=capacity or "",
    )
