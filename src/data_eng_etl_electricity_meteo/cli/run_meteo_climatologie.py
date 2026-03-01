r"""CLI entrypoint for Météo France climatologie pipeline.

Exposes ``--year-start`` / ``--year-end`` arguments that are specific to the
climatologie download (95 departmental files from data.gouv.fr).
For datasets without extra arguments, use ``cli/run_pipeline.py``.

Usage::

    uv run --env-file=.env.local src/.../cli/run_meteo_climatologie.py
    uv run --env-file=.env.local src/.../cli/run_meteo_climatologie.py \
        --skip-postgres
    uv run --env-file=.env.local src/.../cli/run_meteo_climatologie.py \
        --year-start 2024 --year-end 2025
"""

from functools import partial

import typer

from data_eng_etl_electricity_meteo.cli.runner import run_pipeline
from data_eng_etl_electricity_meteo.utils.meteo_download import download_climatologie

DATASET_NAME = "meteo_france_climatologie"

app = typer.Typer()


@app.command()
def main(
    year_start: int | None = typer.Option(None, help="Start year (default: current - 1)."),
    year_end: int | None = typer.Option(None, help="End year (default: current)."),
    skip_postgres: bool = typer.Option(False, help="Skip Postgres loading."),
) -> None:
    """Run the Météo France climatologie pipeline."""
    run_pipeline(
        dataset_name=DATASET_NAME,
        custom_download=partial(
            download_climatologie,
            year_start=year_start,
            year_end=year_end,
        ),
        skip_postgres=skip_postgres,
    )


if __name__ == "__main__":
    app()
