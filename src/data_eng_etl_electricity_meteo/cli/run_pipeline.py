"""Generic CLI entrypoint for any remote dataset.

Auto-detects custom download strategies from the ``CUSTOM_DOWNLOADS`` registry.

Examples
--------
- uv run pipeline odre_installations
- uv run pipeline meteo_france_climatologie
- uv run pipeline odre_eco2mix_tr --skip-postgres
- uv run pipeline odre_eco2mix_tr --only-postgres
"""

import typer

from data_eng_etl_electricity_meteo.cli.pipeline_runner import run_pipeline
from data_eng_etl_electricity_meteo.custom_downloads.registry import CUSTOM_DOWNLOADS
from data_eng_etl_electricity_meteo.custom_metadata.registry import CUSTOM_METADATA

app = typer.Typer(no_args_is_help=True)


@app.command()
def main(
    dataset_name: str = typer.Argument(
        help="Catalog identifier (e.g. odre_installations, odre_eco2mix_tr).",
        show_default=False,
    ),
    skip_postgres: bool = typer.Option(
        False,
        help="Skip the final Postgres load step.",
        show_default=True,
    ),
    only_postgres: bool = typer.Option(
        False,
        help="Skip ingestion, only load existing silver Parquet into Postgres.",
        show_default=True,
    ),
) -> None:
    """Run the remote dataset pipeline for a single dataset."""
    if skip_postgres and only_postgres:
        raise typer.BadParameter("--skip-postgres and --only-postgres are mutually exclusive")

    run_pipeline(
        dataset_name,
        custom_download=CUSTOM_DOWNLOADS.get(dataset_name),
        custom_metadata=CUSTOM_METADATA.get(dataset_name),
        skip_postgres=skip_postgres,
        only_postgres=only_postgres,
    )


if __name__ == "__main__":
    app()
