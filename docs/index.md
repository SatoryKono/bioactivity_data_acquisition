# Bioactivity Data Acquisition

Модульный ETL-пайплайн для загрузки биоактивностных данных из внешних API (ChEMBL и др.), нормализации, валидации (Pandera) и детерминированного экспорта CSV, включая QC-отчёты и корреляционные матрицы.

## Навигация по документации

### 🎓 [Туториалы](tutorials/index.md)

Изучите систему с нуля через пошаговые руководства.

- [Быстрый старт](tutorials/quickstart.md) — основы за 15 минут
- [Полный пайплайн (E2E)](tutorials/e2e-pipeline.md) — полный цикл обработки
- [Обогащение документов (E2E)](tutorials/e2e-documents.md) — работа с API

### 🔧 [How-to руководства](how-to/index.md)

Решите конкретные задачи с помощью пошаговых инструкций.

- [Установка](how-to/installation.md) — настройка среды
- [Запуск ETL локально](how-to/run-etl-locally.md) — локальная работа
- [Настройка API клиентов](how-to/configure-api-clients.md) — подключение источников
- [Отладка пайплайна](how-to/debug-pipeline.md) — решение проблем
- [Проверки качества](how-to/run-quality-checks.md) — QC и валидация
- [Контрибьюшн](how-to/contribute.md) — участие в разработке
- [Эксплуатация](how-to/operations.md) — продакшен

### 📚 [Справочник](reference/index.md)

Исчерпывающая техническая документация.

- [CLI](reference/cli/index.md) — команды и параметры
- [API](reference/api/index.md) — автогенерированная документация
- [Конфигурация](reference/configuration/index.md) — параметры и схемы
- [Схемы данных](reference/data-schemas/index.md) — структуры и валидация
- [Выходные данные](reference/outputs/index.md) — форматы и артефакты

### 💡 [Объяснения](explanations/index.md)

Понимание архитектурных решений и принципов.

- [Архитектура](explanations/architecture.md) — общий обзор системы
- [Решения дизайна](explanations/design-decisions.md) — обоснования выбора
- [Чувствительность к регистру](explanations/case-sensitivity.md) — сохранение регистра

## Быстрый старт

```bash
# Установка
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install .[dev]

# Базовый запуск (v2 конфигурации)
make run ENTITY=documents CONFIG=configs/config_documents_v2.yaml
make run ENTITY=targets CONFIG=configs/config_targets_v2.yaml
make run ENTITY=assays CONFIG=configs/config_assays_v2.yaml
make run ENTITY=activities CONFIG=configs/config_activities_v2.yaml
make run ENTITY=testitems CONFIG=configs/config_testitems_v2.yaml
```

## Основные возможности

- **Множественные API источники**: ChEMBL, UniProt, IUPHAR, Crossref, PubMed, OpenAlex, Semantic Scholar, PubChem
- **Валидация данных**: Pandera схемы для сырых и нормализованных данных
- **Детерминированный экспорт**: Воспроизводимые CSV с контролем качества
- **Автоматические отчёты**: QC-метрики и корреляционные матрицы
- **Мониторинг**: OpenTelemetry и структурированное логирование
- **CLI интерфейс**: Typer-based командная строка

## Схемы и валидация

- [Pandera-схемы](reference/data-schemas/index.md) для сырья и нормализованных данных
- [Схемы данных](reference/data-schemas/index.md) и инварианты
- [Валидация](reference/data-schemas/validation.md) и обработка ошибок

## Выходные артефакты

- Детерминированные CSV/Parquet
- [Выходные данные](reference/outputs/index.md) (базовые/расширенные)

## Качество кода

- **Тестирование**: pytest с покрытием ≥90%
- **Типизация**: mypy в strict режиме
- **Линтинг**: ruff и black
- **Pre-commit**: автоматические проверки

## CI/CD

- **GitHub Actions**: тесты, линтеры, типизация
- **Документация**: автоматический деплой на GitHub Pages
- **Безопасность**: bandit и safety проверки

## Лицензия и вклад

- [Правила контрибьюшенов](how-to/contribute.md)
