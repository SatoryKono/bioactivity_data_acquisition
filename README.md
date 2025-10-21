# Bioactivity Data Acquisition

Модульный ETL-пайплайн для загрузки биоактивностных данных из внешних API (ChEMBL, UniProt, IUPHAR, Crossref, PubMed, OpenAlex, Semantic Scholar), нормализации, валидации (Pandera) и детерминированного экспорта CSV.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-latest-green.svg)](https://satorykono.github.io/bioactivity_data_acquisition/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Quickstart

```bash
# Установка
pip install .[dev]

# Быстрый запуск
make quick-start
```

## Статус пайплайнов

| Пайплайн | Статус | Источники | Конфигурация |
|----------|--------|-----------|--------------|
| **Documents** | ✅ Стабильно | Crossref, OpenAlex, PubMed, Semantic Scholar | `configs/config_documents_full.yaml` |
| **Targets** | ✅ Стабильно | ChEMBL, UniProt, IUPHAR | `configs/config_target_full.yaml` |
| **Assays** | ✅ Стабильно | ChEMBL | `configs/config_assay_full.yaml` |
| **Activities** | ✅ Стабильно | ChEMBL | `configs/config_activity_full.yaml` |
| **Testitems** | ✅ Стабильно | ChEMBL, PubChem | `configs/config_testitem_full.yaml` |

## Документация

📚 **[Полная документация](https://satorykono.github.io/bioactivity_data_acquisition/)** — руководства, API референс, архитектура

## Основные возможности

- **Множественные API источники**: ChEMBL, UniProt, IUPHAR, Crossref, OpenAlex, PubMed, Semantic Scholar, PubChem
- **Валидация данных**: Pandera схемы для сырых и нормализованных данных
- **Детерминированный экспорт**: Воспроизводимые CSV с контролем качества
- **Автоматические отчёты**: QC-метрики и корреляционные матрицы
- **Мониторинг**: OpenTelemetry и структурированное логирование
- **CLI интерфейс**: Typer-based командная строка

## Лицензия

MIT License — см. [LICENSE](LICENSE) для деталей.
