SELECT *, file_row_number AS _source_row FROM read_parquet('{{ var("bronze_path") }}/odre_eco2mix_tr.parquet', file_row_number = true)
