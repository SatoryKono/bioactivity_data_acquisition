# API загрузчика конфигурации

Документ фиксирует публичные контракты для модулей `src/bioetl/config/loader.py`
и `src/bioetl/config/environment.py`. При расширении API обновляйте этот файл и
раздел индекса `docs/api/INDEX.md`.

> **Импорт моделей.** Все примеры ниже используют
> `bioetl.config.models.models.PipelineConfig`. Путь
> `bioetl.config.models`/`bioetl/config/models.py` допускается только для внешней
> совместимости и помечен DeprecationWarning.

## `load_config`

**Сигнатура**:
`load_config(config_path: str | Path, *, profiles: Sequence[str | Path] | None = None, cli_overrides: Mapping[str, Any] | None = None, env: Mapping[str, str] | None = None, env_prefixes: Sequence[str] = ("BIOETL__", "BIOACTIVITY__"), include_default_profiles: bool = False) -> PipelineConfig`

**Источник**: `src/bioetl/config/loader.py`

Загружает, объединяет и валидирует YAML-конфигурацию пайплайна:

- рекурсивно обрабатывает `extends` и `!include`;
- последовательно применяет профили (`profiles`, `include_default_profiles`);
- накладывает CLI-оверрайды (`cli_overrides`) и значения из окружения (`env`,
  `env_prefixes`);
- валидирует результат через `PipelineConfig.model_validate`.

**Возвращает**: экземпляр `PipelineConfig` с полностью развёрнутой
конфигурацией.

**Исключения**:

- `FileNotFoundError` — основной файл, профили, include-ресурсы или строгие
  каталоговые слои среды не найдены;
- `yaml.YAMLError` — синтаксическая ошибка YAML;
- `TypeError` — файл не содержит mapping либо ключи не строки, источники
  merge-ноды некорректны;
- `ValueError` — обнаружен цикл `extends` либо переменная окружения `BIOETL_ENV`
  вне допустимого множества;
- `pydantic.ValidationError` — итоговая конфигурация не соответствует
  `PipelineConfig`.

### Пример

```yaml
# configs/profiles/base.yaml
pipeline:
  id: example
  retries: 3

# configs/fragments/http-headers.yaml
headers:
  User-Agent: BioETL/1.0

# configs/pipeline.yaml
extends:
  - profiles/base.yaml

pipeline:
  retries: 5

http:
  client: !include fragments/http-headers.yaml
```

```python
from pathlib import Path

from bioetl.config.loader import load_config

config = load_config(Path("configs/pipeline.yaml"))
assert config.pipeline.retries == 5
assert config.http.client.headers["User-Agent"] == "BioETL/1.0"
```

## `load_environment_settings`

**Сигнатура**:
`load_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings`

Оборачивает чтение `.env` и переменных процесса в типизированную модель
`EnvironmentSettings`. Поддерживает префикс `BIOETL__...`, алиасы `PUBMED_*`,
`CROSSREF_MAILTO`, `SEMANTIC_SCHOLAR_API_KEY`, `IUPHAR_API_KEY`, `VOCAB_STORE`,
`BIOETL_OFFLINE_CHEMBL_CLIENT`.

**Параметры**:

- `env_file` — путь к дополнительному `.env`; если не указан, используется
  дефолтное поведение `pydantic.BaseSettings`.

**Возвращает**: экземпляр `EnvironmentSettings` со строгой валидацией и
приведением типов.

**Исключения**:

- `FileNotFoundError` — переданный `env_file` отсутствует;
- `ValueError` — неверное значение `BIOETL_ENV`, некорректный e-mail или булево
  поле;
- `pydantic.ValidationError` — непрошедшие проверки модели.

### Интеграция с окружением

```python
from bioetl.config.environment import (
    load_environment_settings,
    build_env_override_mapping,
)

settings = load_environment_settings()
runtime_overrides = build_env_override_mapping(settings)
```

Полученный `runtime_overrides` можно детерминированно слить с результатом
`load_config`. Для обратной совместимости оставлен `apply_runtime_overrides`,
который синхронизирует `BIOETL__...` переменные в `os.environ`.
