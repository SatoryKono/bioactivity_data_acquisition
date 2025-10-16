# Конфигурационные файлы для обработки документов

## Обзор

Данная папка содержит конфигурационные файлы для различных сценариев обработки
документов с использованием ETL pipeline.

## Доступные конфигурации

### 1. `config.yaml`(Основная конфигурация)

**Назначение**: Основная конфигурация для обработки документов
**Особенности**:

- Включает ChEMBL, Crossref, OpenAlex, PubMed

- Semantic Scholar отключен по умолчанию

- Стандартные настройки таймаутов и retry

- Нормализация журналов и формирование ссылок включены

**Использование**:

```

bioactivity-data-acquisition get-document-data --config configs/config.yaml

```

### 2.`config*documents.yaml`(Специализированная для документов)

**Назначение**: Оптимизированная конфигурация специально для документов
**Особенности**:

- Все источники включены (включая Semantic Scholar с флагом enabled: false)

- Увеличенные таймауты для ChEMBL

- Настроенная нормализация журналов

- Формирование литературных ссылок

**Использование**:

```

bioactivity-data-acquisition get-document-data --config
configs/config*documents.yaml

```

### 3.`config*documents*test.yaml`(Быстрое тестирование)

**Назначение**: Быстрое тестирование функциональности
**Особенности**:

- Только ChEMBL источник

- Ограничение до 10 документов

- Меньшие таймауты и размеры страниц

- Отключена корреляция для скорости

- DEBUG уровень логирования

**Использование**:

```

bioactivity-data-acquisition get-document-data --config
configs/config*documents*test.yaml

```

### 4.`config*documents*full.yaml`(Полная обработка)

**Назначение**: Максимальная детализация и все источники
**Особенности**:

- Все источники включены

- Увеличенные таймауты и retry

- Больше воркеров (8)

- Строгие требования к качеству данных

- Максимальное количество страниц

**Использование**:

```

bioactivity-data-acquisition get-document-data --config
configs/config*documents*full.yaml

```

## Ключевые параметры

### Источники данных

- **ChEMBL**: Основной источник документов

- **Crossref**: Дополнительные метаданные по DOI

- **OpenAlex**: Альтернативные метаданные

- **PubMed**: Медицинские публикации

- **Semantic Scholar**: Академические метаданные (опционально)

### Нормализация журналов

```

postprocess:
  journal*normalization:
    enabled: true
    columns: ["journal", "pubmed*journal", "chembl*journal", "crossref*journal"]

```

### Формирование ссылок

```

postprocess:
  citation*formatting:
    enabled: true
    columns:
      journal: "journal"
      volume: "volume"
      issue: "issue"
      first*page: "first*page"
      last*page: "last*page"

```

### Runtime параметры

-`workers`: Количество параллельных воркеров

- `limit`: Ограничение количества документов (null = без ограничений)

- `dry*run`: Режим тестирования без сохранения

- `date*tag`: Тег даты в имени файла

## Переменные окружения

Для работы с API ключами установите следующие переменные:

```

export CHEMBL*API*TOKEN="your*chembl*token"
export CROSSREF*API*KEY="your*crossref*key"
export PUBMED*API*KEY="your*pubmed*key"
export SEMANTIC*SCHOLAR*API*KEY="your*semantic*scholar*key"

```

## Рекомендации по использованию

1. **Для разработки**: Используйте`config*documents*test.yaml`2. **Для продакшена**:
Используйте`config*documents.yaml`или`config*documents*full.yaml`3. **Для отладки**: Включите DEBUG
уровень логирования
4. **Для больших объемов**: Увеличьте количество воркеров и таймауты

## Выходные файлы

После обработки создаются следующие файлы:

-`documents*<date*tag>.csv`- Основные данные с нормализованными журналами и ссылками

-`documents*<date*tag>*qc.csv`- Отчет о качестве данных

## Мониторинг

Для мониторинга API лимитов используйте:

```

python -m library.tools.check*api*limits
python -m library.tools.monitor*api

```
