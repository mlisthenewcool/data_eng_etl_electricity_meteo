"""Generic CLI entrypoint for any remote dataset.

Auto-detects custom download strategies from the ``CUSTOM_DOWNLOADS`` registry.

Usage::

    uv run --env-file=.env.local src/.../cli/run_pipeline.py odre_installations
    uv run --env-file=.env.local src/.../cli/run_pipeline.py meteo_france_climatologie
    uv run --env-file=.env.local src/.../cli/run_pipeline.py \
        odre_eco2mix_tr --skip-postgres
"""

import typer

from data_eng_etl_electricity_meteo.cli.runner import run_pipeline
from data_eng_etl_electricity_meteo.pipeline.custom_downloads import CUSTOM_DOWNLOADS

app = typer.Typer()


@app.command()
def main(
    dataset_name: str = typer.Argument(help="Catalog identifier (e.g. odre_installations)."),
    skip_postgres: bool = typer.Option(False, help="Skip Postgres loading."),
) -> None:
    """Run the remote dataset pipeline for a single dataset."""
    run_pipeline(
        dataset_name=dataset_name,
        custom_download=CUSTOM_DOWNLOADS.get(dataset_name),
        skip_postgres=skip_postgres,
    )


if __name__ == "__main__":
    app()
