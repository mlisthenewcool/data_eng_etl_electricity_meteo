"""Validation error formatting utilities for Pydantic models.

This module provides helpers to translate complex Pydantic validation
tracebacks into flattened, human-readable formats suitable for logging.

Base Pydantic model with strict schema enforcement.
"""

from pydantic import BaseModel, ConfigDict, ValidationError


class StrictModel(BaseModel):
    """Base model that forbids extra fields to prevent typos and configuration drift."""

    model_config = ConfigDict(extra="forbid")


def format_pydantic_errors(pydantic_errors: ValidationError) -> dict[str, str]:
    """Convert Pydantic validation errors to a structured dictionary.

    This utility function transforms Pydantic's error format into a simpler
    dict for logging and error messages.

    Args:
        pydantic_errors: The exception raised by a Pydantic model during failed validation.

    Returns:
        Dictionary mapping error location (dot-separated path) to error message.
    """
    return {
        ".".join(str(item) for item in err["loc"]): err["msg"] for err in pydantic_errors.errors()
    }
