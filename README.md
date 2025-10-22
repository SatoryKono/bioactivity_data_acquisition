# Bioactivity Data Acquisition

Модульный ETL-пайплайн для загрузки биоактивностных данных из внешних API (ChEMBL, UniProt, IUPHAR, Crossref, PubMed, OpenAlex, Semantic Scholar), нормализации, валидации (Pandera) и детерминированного экспорта CSV.

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://img.shields.io/badge/docs-latest-green.svg)](https://satorykono.github.io/bioactivity_data_acquisition/)
[![Build Status](https://github.com/SatoryKono/bioactivity_data_acquisition/workflows/Documentation/badge.svg)](https://github.com/SatoryKono/bioactivity_data_acquisition/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Quickstart

```bash
# Установка
pip install .[dev]

# Быстрый запуск
make quick-start

# Новый унифицированный интерфейс (v2 конфигурации)
make run ENTITY=documents CONFIG=configs/config_documents_v2.yaml
make run ENTITY=targets CONFIG=configs/config_targets_v2.yaml
make run ENTITY=assays CONFIG=configs/config_assays_v2.yaml
make run ENTITY=activities CONFIG=configs/config_activities_v2.yaml
make run ENTITY=testitems CONFIG=configs/config_testitems_v2.yaml
```

## Статус пайплайнов

| Пайплайн | Статус | Источники | Конфигурация v2 | Конфигурация v1 (deprecated) |
|----------|--------|-----------|-----------------|------------------------------|
| **Documents** | ✅ Стабильно | Crossref, OpenAlex, PubMed, Semantic Scholar | `configs/config_documents_v2.yaml` | `configs/config_documents_full.yaml` |
| **Targets** | ✅ Стабильно | ChEMBL, UniProt, IUPHAR | `configs/config_targets_v2.yaml` | `configs/config_target_full.yaml` |
| **Assays** | ✅ Стабильно | ChEMBL | `configs/config_assays_v2.yaml` | `configs/config_assay_full.yaml` |
| **Activities** | ✅ Стабильно | ChEMBL | `configs/config_activities_v2.yaml` | `configs/config_activity_full.yaml` |
| **Testitems** | ✅ Стабильно | ChEMBL, PubChem | `configs/config_testitems_v2.yaml` | `configs/config_testitem_full.yaml` |

### Миграция на v2 конфигурации

**Рекомендуется использовать новые v2 конфигурации** для лучшей стандартизации и функциональности:

- **Унифицированная структура**: Все конфигурации следуют единому шаблону
- **Улучшенная валидация**: Расширенные проверки качества данных
- **Стандартизированные артефакты**: Единообразные имена выходных файлов
- **Унифицированные CLI флаги**: Согласованные опции для всех команд

Старые конфигурации v1 помечены как deprecated, но продолжают работать для обратной совместимости.

## Документация

📚 **[Полная документация](https://satorykono.github.io/bioactivity_data_acquisition/)** — руководства, API референс, архитектура, туториалы

## Основные возможности

- **Множественные API источники**: ChEMBL, UniProt, IUPHAR, Crossref, OpenAlex, PubMed, Semantic Scholar, PubChem
- **Валидация данных**: Pandera схемы для сырых и нормализованных данных
- **Детерминированный экспорт**: Воспроизводимые CSV с контролем качества
- **Автоматические отчёты**: QC-метрики и корреляционные матрицы
- **Мониторинг**: OpenTelemetry и структурированное логирование
- **CLI интерфейс**: Typer-based командная строка

## Лицензия

MIT License — см. [LICENSE](LICENSE) для деталей.
