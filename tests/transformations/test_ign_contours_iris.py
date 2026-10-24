"""Unit tests for IGN contours IRIS transforms."""

import struct

import polars as pl
import pytest

from data_eng_etl_electricity_meteo.transformations.datasets.ign_contours_iris import (
    transform_silver,
)

# --------------------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------------------


def _square_wkb(*, center_x: float, center_y: float, half_side: float) -> bytes:
    """Encode an axis-aligned square polygon as little-endian WKB.

    Built by hand so the test input does not depend on the spatial extension under test.
    """
    ring = [
        (center_x - half_side, center_y - half_side),
        (center_x + half_side, center_y - half_side),
        (center_x + half_side, center_y + half_side),
        (center_x - half_side, center_y + half_side),
        (center_x - half_side, center_y - half_side),
    ]
    # byte order 1 = little-endian, geometry type 3 = Polygon, 1 ring
    header = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(ring))
    return header + b"".join(struct.pack("<dd", x, y) for x, y in ring)


# --------------------------------------------------------------------------------------
# transform_silver
# --------------------------------------------------------------------------------------


class TestTransformSilver:
    def test_centroid_axis_order(self) -> None:
        # Lambert 93 (EPSG:2154) square centered on central Paris.
        # A lat/lon swap would put the centroid at (2.34, 48.86) instead.
        lf = pl.LazyFrame(
            {
                "code_iris": ["751010101"],
                "nom_iris": ["Saint-Germain-l'Auxerrois 1"],
                "code_insee": ["75101"],
                "nom_commune": ["Paris 1er Arrondissement"],
                "type_iris": ["H"],
                "geometrie": [_square_wkb(center_x=651_500, center_y=6_862_500, half_side=500)],
            }
        )

        result = transform_silver(lf).collect()

        assert result["centroid_lat"].item() == pytest.approx(48.8607, abs=1e-3)
        assert result["centroid_lon"].item() == pytest.approx(2.3389, abs=1e-3)
