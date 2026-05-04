"""Validate column alignment between climatologie download and bronze transform.

The climatologie download projects columns at write time (~9 GB → ~760 MB) and the
bronze transform reads them back with explicit dtypes. Both modules declare the column
list independently — this test enforces that they stay aligned.
"""

from data_eng_etl_electricity_meteo.custom_downloads.meteo_climatologie import (
    _LANDING_COLUMNS,
)
from data_eng_etl_electricity_meteo.transformations.datasets.meteo_france_climatologie import (
    _BRONZE_COLUMNS,
)


def test_landing_columns_match_bronze_columns() -> None:
    """``_LANDING_COLUMNS`` (download projection) must match ``_BRONZE_COLUMNS`` keys.

    The download writes only the columns listed in ``_LANDING_COLUMNS``; the bronze
    transform expects exactly those columns.
    A drift between the two breaks the bronze step with a schema mismatch on next run.
    """
    landing = list(_LANDING_COLUMNS)
    bronze = list(_BRONZE_COLUMNS.keys())
    assert landing == bronze, (
        f"_LANDING_COLUMNS and _BRONZE_COLUMNS keys must be identical (same order).\n"
        f"  Only in landing: {set(landing) - set(bronze)}\n"
        f"  Only in bronze : {set(bronze) - set(landing)}"
    )
