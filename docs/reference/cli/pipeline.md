# Команда pipeline

Запуск основного ETL-пайплайна для обработки биоактивностных данных.

## Синтаксис

```bash
bioactivity-data-acquisition pipeline [OPTIONS]
```

## Опции

| Опция | Тип | Описание | По умолчанию |
|-------|-----|----------|--------------|
| `--config`, `-c` | PATH | Путь к YAML-конфигурации | Обязательно |
| `--set` | KEY=VALUE | Переопределение параметров | - |
| `--help` | - | Показать справку | - |

## Примеры

### Базовый запуск

```bash
bioactivity-data-acquisition pipeline --config configs/config.yaml
```

### С переопределениями

```bash
bioactivity-data-acquisition pipeline \
  --config configs/config.yaml \
  --set runtime.workers=8 \
  --set http.global.timeout_sec=60
```

### Dry-run режим

```bash
bioactivity-data-acquisition pipeline \
  --config configs/config.yaml \
  --set runtime.dry_run=true
```

## Переопределения конфигурации

Вы можете переопределить любые параметры конфигурации с помощью `--set`:

```bash
# Изменение количества воркеров
--set runtime.workers=8

# Изменение таймаута HTTP
--set http.global.timeout_sec=60

# Изменение уровня логирования
--set logging.level=DEBUG

# Ограничение количества страниц
--set sources.chembl.pagination.max_pages=5
```

## Выходные файлы

Команда создаёт следующие файлы:

- `data/output/bioactivities.csv` — основной датасет
- `data/output/bioactivities_qc_report.csv` — QC отчёт
- `data/output/bioactivities_correlation.csv` — корреляционная матрица

## Коды возврата

- `0` — успешное выполнение
- `1` — ошибка конфигурации
- `2` — ошибка валидации данных
- `3` — ошибка записи файлов
