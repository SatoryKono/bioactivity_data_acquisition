# API Reference

## CLI

- `library.cli:app` — Typer-приложение (команды: `pipeline`, `get-document-data`, `version`)
- `library.cli:get_document_data` — обогащение документов
- `library.cli:pipeline` — основной ETL пайплайн

## Конфигурация

- `library.config.Config.load(path, overrides=None, env_prefix="BIOACTIVITY__")`
- Модели: `HTTPSettings`, `SourceSettings`, `IOSettings`, `OutputSettings`, `ValidationSettings`, `DeterminismSettings`, `PostprocessSettings`

## ETL

- `library.etl.run:run_pipeline(config, logger)` — e2e пайплайн
- `library.etl.load:write_deterministic_csv(df, destination, ...)`
- `library.etl.load:write_qc_artifacts(df, qc_path, corr_path, ...)`

## Документы

- `library.documents.pipeline:run_document_etl(config, frame)` — ETL для документов
- `library.documents.pipeline:write_document_outputs(result, output_dir, date_tag, config=None)`
