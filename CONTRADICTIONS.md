# Противоречия в документации

| Тип | Раздел/файл | Формулировка №1 | Формулировка №2/факт | Почему конфликт | [ref_doc_1] | [ref_doc_2] | Критичность |
|-----|-------------|-----------------|---------------------|-----------------|-------------|-------------|-------------|
| F | Архитектурные инварианты: стадии пайплайна | extract → transform → validate → write → run | N/A | Расхождение в формулировке стадий пайплайна (с run или без) | [docs/etl_contract/01-pipeline-contract.md](docs/etl_contract/01-pipeline-contract.md) | [docs/etl_contract/00-etl-overview.md](docs/etl_contract/00-etl-overview.md) | HIGH |
