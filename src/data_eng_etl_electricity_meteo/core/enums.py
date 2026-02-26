"""Medallion architecture layers and pipeline execution stages."""

from enum import StrEnum


class MedallionLayer(StrEnum):
    """Medallion architecture layers, ordered from raw to analytical."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class PipelineStage(StrEnum):
    """Pipeline execution stages covering ingestion, transformation, and loading."""

    INGEST = "ingest"
    EXTRACT = "extract"
    BRONZE = "bronze"
    SILVER = "silver"
    LOAD_POSTGRES = "load_postgres"
    GOLD = "gold"
