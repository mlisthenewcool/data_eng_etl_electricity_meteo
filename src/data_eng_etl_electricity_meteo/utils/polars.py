"""Polars utility functions."""

import polars as pl


def collect_narrow(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a LazyFrame, narrowing the return type to ``DataFrame``.

    Polars type stubs declare ``LazyFrame.collect()`` as returning ``InProcessQuery |
    DataFrame`` (GPU support). This wrapper narrows the type to ``DataFrame`` so callers
    avoid ``ty: ignore`` pragmas.

    Parameters
    ----------
    lf
        LazyFrame to collect.

    Returns
    -------
    pl.DataFrame
        Collected DataFrame.
    """
    df = lf.collect()
    assert isinstance(df, pl.DataFrame)
    return df
