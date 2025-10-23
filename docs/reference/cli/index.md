# CLI Reference

## Обзор

CLI (Command Line Interface) предоставляет удобный способ запуска пайплайнов и управления системой через командную строку. Основан на библиотеке Typer и поддерживает автодополнение.

## Основные команды

### Главная команда

```bash
python -m library.cli [OPTIONS] COMMAND [ARGS]...
```

**Опции:**

- `--install-completion` — установить автодополнение для текущей оболочки
- `--show-completion` — показать автодополнение для копирования или настройки
- `--help` — показать справку

### Команды

| Команда | Описание |
|---------|----------|
| `pipeline` | Выполнить ETL пайплайн с использованием конфигурационного файла |
| `get-document-data` | Собрать и обогатить метаданные документов из настроенных источников |
| `get-target-data` | Извлечь и обогатить данные мишеней из ChEMBL/UniProt/IUPHAR |
| `get-activity-data` | Извлечь и обогатить данные активностей из ChEMBL |
| `testitem-run` | Запустить ETL пайплайн для молекул |
| `testitem-validate-config` | Валидировать конфигурационный файл для молекул |
| `testitem-info` | Показать информацию о ETL пайплайне для молекул |
| `analyze-iuphar-mapping` | Анализировать маппинг IUPHAR в данных мишеней |
| `list-manifests` | Показать все доступные манифесты |
| `show-manifest` | Показать содержимое манифеста |
| `list-reports` | Показать все доступные отчёты |
| `health` | Проверить состояние всех настроенных API клиентов |
| `version` | Показать версию пакета |
| `install-completion` | Установить автодополнение оболочки для CLI |

## Детальная документация команд

### pipeline

Выполнить ETL пайплайн с использованием конфигурационного файла.

```bash
python -m library.cli pipeline [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `-s, --set TEXT` — переопределить значения конфигурации через точечную нотацию (KEY=VALUE)
- `--log-level TEXT` — уровень логирования [по умолчанию: INFO]
- `--log-file PATH` — путь к файлу логов
- `--log-format TEXT` — формат консоли (text/json) [по умолчанию: text]
- `--no-file-log` — отключить файловое логирование
- `--help` — показать справку

**Примеры:**

```bash
# Базовый запуск
python -m library.cli pipeline -c configs/config_document.yaml

# С переопределением параметров
python -m library.cli pipeline -c configs/config_target.yaml --set sources.chembl.rate_limit=3

# С подробным логированием
python -m library.cli pipeline -c configs/config_assay.yaml --log-level DEBUG

# С JSON логированием
python -m library.cli pipeline -c configs/config_activity.yaml --log-format json
```

### get-document-data

Собрать и обогатить метаданные документов из настроенных источников.

```bash
python -m library.cli get-document-data [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `-s, --set TEXT` — переопределить значения конфигурации через точечную нотацию (KEY=VALUE)
- `--log-level TEXT` — уровень логирования [по умолчанию: INFO]
- `--log-file PATH` — путь к файлу логов
- `--log-format TEXT` — формат консоли (text/json) [по умолчанию: text]
- `--no-file-log` — отключить файловое логирование
- `--help` — показать справку

**Примеры:**

```bash
# Обогащение документов
python -m library.cli get-document-data -c configs/config_document.yaml

# С ограничением источников
python -m library.cli get-document-data -c configs/config_document.yaml --set sources.pubmed.enabled=false
```

### get-target-data

Извлечь и обогатить данные мишеней из ChEMBL/UniProt/IUPHAR.

```bash
python -m library.cli get-target-data [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `-s, --set TEXT` — переопределить значения конфигурации через точечную нотацию (KEY=VALUE)
- `--log-level TEXT` — уровень логирования [по умолчанию: INFO]
- `--log-file PATH` — путь к файлу логов
- `--log-format TEXT` — формат консоли (text/json) [по умолчанию: text]
- `--no-file-log` — отключить файловое логирование
- `--help` — показать справку

### get-activity-data

Извлечь и обогатить данные активностей из ChEMBL.

```bash
python -m library.cli get-activity-data [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `-s, --set TEXT` — переопределить значения конфигурации через точечную нотацию (KEY=VALUE)
- `--log-level TEXT` — уровень логирования [по умолчанию: INFO]
- `--log-file PATH` — путь к файлу логов
- `--log-format TEXT` — формат консоли (text/json) [по умолчанию: text]
- `--no-file-log` — отключить файловое логирование
- `--help` — показать справку

### testitem-run

Запустить ETL пайплайн для молекул.

```bash
python -m library.cli testitem-run [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `-s, --set TEXT` — переопределить значения конфигурации через точечную нотацию (KEY=VALUE)
- `--log-level TEXT` — уровень логирования [по умолчанию: INFO]
- `--log-file PATH` — путь к файлу логов
- `--log-format TEXT` — формат консоли (text/json) [по умолчанию: text]
- `--no-file-log` — отключить файловое логирование
- `--help` — показать справку

### testitem-validate-config

Валидировать конфигурационный файл для молекул.

```bash
python -m library.cli testitem-validate-config [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `--help` — показать справку

### testitem-info

Показать информацию о ETL пайплайне для молекул.

```bash
python -m library.cli testitem-info [OPTIONS]
```

**Опции:**

- `--help` — показать справку

### analyze-iuphar-mapping

Анализировать маппинг IUPHAR в данных мишеней.

```bash
python -m library.cli analyze-iuphar-mapping [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `--help` — показать справку

### list-manifests

Показать все доступные манифесты.

```bash
python -m library.cli list-manifests [OPTIONS]
```

**Опции:**

- `--help` — показать справку

### show-manifest

Показать содержимое манифеста.

```bash
python -m library.cli show-manifest [OPTIONS]
```

**Опции:**

- `--help` — показать справку

### list-reports

Показать все доступные отчёты.

```bash
python -m library.cli list-reports [OPTIONS]
```

**Опции:**

- `--help` — показать справку

### health

Проверить состояние всех настроенных API клиентов.

```bash
python -m library.cli health [OPTIONS]
```

**Опции:**

- `-c, --config FILE` — путь к конфигурационному файлу (обязательно)
- `--help` — показать справку

**Примеры:**

```bash
# Проверка состояния API
python -m library.cli health -c configs/config_document.yaml
```

### version

Показать версию пакета.

```bash
python -m library.cli version [OPTIONS]
```

**Опции:**

- `--help` — показать справку

### install-completion

Установить автодополнение оболочки для CLI.

```bash
python -m library.cli install-completion [OPTIONS]
```

**Опции:**

- `--help` — показать справку

## Переопределение конфигурации

### Синтаксис --set

Используйте точечную нотацию для переопределения значений конфигурации:

```bash
# Переопределение лимитов API
--set sources.chembl.rate_limit=3
--set sources.pubmed.budget=25000

# Переопределение путей
--set io.input_dir=/custom/input
--set io.output_dir=/custom/output

# Переопределение логирования
--set logging.level=DEBUG
--set logging.file=/custom/logs/app.log
```

### Примеры переопределения

```bash
# Ограничение источников
python -m library.cli pipeline -c configs/config_document.yaml \
  --set sources.pubmed.enabled=false \
  --set sources.crossref.enabled=false

# Изменение лимитов
python -m library.cli pipeline -c configs/config_target.yaml \
  --set sources.chembl.rate_limit=2 \
  --set sources.uniprot.budget=15000

# Настройка логирования
python -m library.cli pipeline -c configs/config_assay.yaml \
  --set logging.level=DEBUG \
  --set logging.format=json
```

## Автодополнение

### Установка автодополнения

```bash
# Установить для текущей оболочки
python -m library.cli --install-completion

# Показать код для ручной установки
python -m library.cli --show-completion
```

### Поддерживаемые оболочки

- Bash
- Zsh
- Fish
- PowerShell

## Логирование

### Уровни логирования

- `DEBUG` — подробная отладочная информация
- `INFO` — общая информация о работе (по умолчанию)
- `WARNING` — предупреждения
- `ERROR` — ошибки
- `CRITICAL` — критические ошибки

### Форматы логирования

- `text` — человекочитаемый формат (по умолчанию)
- `json` — структурированный JSON формат

### Примеры логирования

```bash
# Подробное логирование в файл
python -m library.cli pipeline -c configs/config_document.yaml \
  --log-level DEBUG \
  --log-file logs/pipeline.log

# JSON логирование
python -m library.cli pipeline -c configs/config_target.yaml \
  --log-format json

# Отключение файлового логирования
python -m library.cli pipeline -c configs/config_assay.yaml \
  --no-file-log
```

## Обработка ошибок

### Коды возврата

- `0` — успешное выполнение
- `1` — общая ошибка
- `2` — ошибка конфигурации
- `3` — ошибка валидации данных
- `4` — ошибка API
- `5` — ошибка файловой системы

### Отладка

```bash
# Включить отладочное логирование
python -m library.cli pipeline -c configs/config_document.yaml --log-level DEBUG

# Проверить конфигурацию
python -m library.cli testitem-validate-config -c configs/config_testitem.yaml

# Проверить состояние API
python -m library.cli health -c configs/config_document.yaml
```

## Связанные документы

- [Конфигурация](../configuration/index.md)
- [Пайплайны](../../pipelines/documents.md)
- [FAQ](../../faq.md)
- [How-To руководства](../../how-to/index.md)
 
 
