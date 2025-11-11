# API загрузчика конфигурации

Документ фиксирует публичные контракты для модулей `src/bioetl/config/loader.py` и `src/bioetl/config/environment.py`. При расширении API обновляйте этот файл и раздел индекса `docs/api/INDEX.md`.

## `load_config`

**Сигнатура**: `load_config(config_path: str | Path, *, profiles: Sequence[str | Path] | None = None, cli_overrides: Mapping[str, Any] | None = None, env: Mapping[str, str] | None = None, env_prefixes: Sequence[str] = ("BIOETL__", "BIOACTIVITY__"), include_default_profiles: bool = False) -> PipelineConfig`

Загружает, объединяет и валидирует YAML-конфигурацию пайплайна:

- рекурсивно обрабатывает `extends` и `!include`;
- последовательно применяет профили (`profiles`, `include_default_profiles`);
- накладывает CLI-оверрайды (`cli_overrides`) и значения из окружения (`env`, `env_prefixes`);
- валидирует результат через `PipelineConfig.model_validate`.

**Возвращает**: экземпляр `PipelineConfig` с полностью развёрнутой конфигурацией.

**Исключения**:

- `FileNotFoundError` — основной файл, профили или include-ресурсы не найдены;
- `yaml.YAMLError` — синтаксическая ошибка YAML;
- `TypeError` — файл не содержит mapping или ключи не строки;
- `ValueError` — обнаружен цикл `extends` либо недопустимое значение `BIOETL_ENV`;
- `pydantic.ValidationError` — итоговая конфигурация не соответствует `PipelineConfig`.

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

**Сигнатура**: `load_environment_settings(*, env_file: Path | None = None) -> EnvironmentSettings`

Оборачивает чтение `.env` и переменных процесса в типизированную модель `EnvironmentSettings`. Поддерживает префикс `BIOETL__...`, алиасы `PUBMED_*`, `CROSSREF_MAILTO`, `SEMANTIC_SCHOLAR_API_KEY`, `IUPHAR_API_KEY`, `VOCAB_STORE`, `BIOETL_OFFLINE_CHEMBL_CLIENT`.

**Параметры**:

- `env_file` — путь к дополнительному `.env`; если не указан, используется дефолтное поведение `pydantic.BaseSettings`.

**Возвращает**: экземпляр `EnvironmentSettings` со строгой валидацией и приведением типов.

**Исключения**:

- `FileNotFoundError` — переданный `env_file` отсутствует;
- `ValueError` — неверное значение `BIOETL_ENV`, некорректный e-mail или булево поле;
- `pydantic.ValidationError` — непрошедшие проверки модели.

### Интеграция с окружением

```python
from bioetl.config.environment import load_environment_settings, apply_runtime_overrides

settings = load_environment_settings()
overrides = apply_runtime_overrides(settings)
```

`apply_runtime_overrides` (см. модуль `environment.py`) дополнит `os.environ` недостающими ключами `BIOETL__...`, используя загруженные значения.

