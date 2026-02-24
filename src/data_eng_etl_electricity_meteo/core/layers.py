"""Medallion architecture layer type and pipeline stage type."""

from enum import StrEnum


class MedallionLayer(StrEnum):
    """Medallion architecture layers, ordered from raw to analytical."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class PipelineStage(StrEnum):
    """Pipeline operation stages (superset of medallion layers)."""

    INGEST = "ingest"
    EXTRACT = "extract"
    BRONZE = "bronze"
    SILVER = "silver"
    LOAD_POSTGRES = "load_postgres"
