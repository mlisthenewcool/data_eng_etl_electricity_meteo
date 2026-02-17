"""Medallion architecture layer type."""

from typing import Literal

__all__: list[str] = ["MedallionLayer"]

type MedallionLayer = Literal["landing", "bronze", "silver", "gold"]
