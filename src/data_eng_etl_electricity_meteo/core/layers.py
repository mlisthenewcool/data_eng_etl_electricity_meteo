"""Medallion architecture layer type."""

from enum import StrEnum


class MedallionLayer(StrEnum):
    """Medallion architecture layers, ordered from raw to analytical."""

    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
