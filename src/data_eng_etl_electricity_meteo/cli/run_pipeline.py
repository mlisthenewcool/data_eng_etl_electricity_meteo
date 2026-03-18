"""Generic CLI entrypoint for any remote dataset.

Download strategy is resolved automatically by ``pipeline_runner.run_pipeline``.

Examples
--------
- uv run pipeline odre_installations
- uv run pipeline meteo_france_climatologie
- uv run pipeline odre_eco2mix_tr --skip-postgres
- uv run pipeline odre_eco2mix_tr --only-postgres
"""

import typer

from data_eng_etl_electricity_meteo.cli.pipeline_runner import run_pipeline

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
        skip_postgres=skip_postgres,
        only_postgres=only_postgres,
    )


if __name__ == "__main__":
    app()
