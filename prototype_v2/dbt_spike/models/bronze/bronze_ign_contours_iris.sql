SELECT *, file_row_number AS _source_row FROM read_parquet('{{ var("bronze_path") }}/ign_contours_iris.parquet', file_row_number = true)
