# Metadata Directory

Эта папка содержит метаданные проекта, организованные по типам и пайплайнам.

## Структура

```
metadata/
├── manifests/          # Манифесты пайплайнов и процессов
│   ├── documents_pipeline.json      # Основной манифест пайплайна документов
│   ├── documents_postprocess.json   # Манифест постобработки документов
│   ├── cleanup_manifest.json        # Манифест процесса очистки
│   └── quality_manifest.json        # Манифест контроля качества
└── reports/            # Отчёты и аудиты
    └── config_audit.csv             # Аудит конфигурации
```

## Манифесты (manifests/)

Манифесты содержат метаданные о различных процессах и пайплайнах:

- **documents_pipeline.json** - общая информация о пайплайне обработки документов
- **documents_postprocess.json** - детали постобработки документов
- **cleanup_manifest.json** - информация о процессе очистки данных
- **quality_manifest.json** - метаданные контроля качества

## Отчёты (reports/)

Отчёты содержат результаты аудитов и анализа:

- **config_audit.csv** - аудит конфигурационных параметров

## Схема именования

- Манифесты: `<pipeline>_<process>.json`
- Отчёты: `<type>_<pipeline>/<filename>`

Примеры:
- `documents_pipeline.json`
- `documents_postprocess.json`
- `config_audit.csv`
