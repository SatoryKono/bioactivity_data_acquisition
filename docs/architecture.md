# Pipeline Architecture

```mermaid
flowchart LR
  A[Config (YAML + ENV + --set)] --> B[CLI Typer команда]
  B --> C[extract]
  C --> D[transform]
  D --> E[validate]
  E --> F[output + QC]
```

Пайплайн принимает конфигурацию как объединение YAML-файлов, переменных окружения и флагов `--set`, после чего Typer-команда из `src/bioetl/cli/` инициирует запуск. Каждый этап реализован отдельными компонентами из `src/bioetl/pipelines/` и `src/bioetl/core/`, соблюдающими контракт `extract → transform → validate → output`.

## Связанные разделы

- [Обзор CLI](cli/00-cli-overview.md)
- [Каталог пайплайнов](pipelines/10-pipelines-catalog.md)
- [Политика детерминизма](determinism/01-determinism-policy.md)
