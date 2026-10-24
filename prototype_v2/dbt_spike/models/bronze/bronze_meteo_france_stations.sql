SELECT *, file_row_number AS _source_row FROM read_parquet('{{ var("bronze_path") }}/meteo_france_stations.parquet', file_row_number = true)
