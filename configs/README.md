# Конфигурационные файлы для обработки документов

## Обзор

Данная папка содержит конфигурационные файлы для различных сценариев обработки документов с использованием ETL pipeline.

## Доступные конфигурации

### 1. `config.yaml` (Основная конфигурация)
**Назначение**: Основная конфигурация для обработки документов
**Особенности**:
- Включает ChEMBL, Crossref, OpenAlex, PubMed
- Semantic Scholar отключен по умолчанию
- Стандартные настройки таймаутов и retry
- Нормализация журналов и формирование ссылок включены

**Использование**:
```bash
bioactivity-data-acquisition get-document-data --config configs/config.yaml
```

### 2. `config_documents.yaml` (Специализированная для документов)
**Назначение**: Оптимизированная конфигурация специально для документов
**Особенности**:
- Все источники включены (включая Semantic Scholar с флагом enabled: false)
- Увеличенные таймауты для ChEMBL
- Настроенная нормализация журналов
- Формирование литературных ссылок

**Использование**:
```bash
bioactivity-data-acquisition get-document-data --config configs/config_documents.yaml
```

### 3. `config_documents_test.yaml` (Быстрое тестирование)
**Назначение**: Быстрое тестирование функциональности
**Особенности**:
- Только ChEMBL источник
- Ограничение до 10 документов
- Меньшие таймауты и размеры страниц
- Отключена корреляция для скорости
- DEBUG уровень логирования

**Использование**:
```bash
bioactivity-data-acquisition get-document-data --config configs/config_documents_test.yaml
```

### 4. `config_documents_full.yaml` (Полная обработка)
**Назначение**: Максимальная детализация и все источники
**Особенности**:
- Все источники включены
- Увеличенные таймауты и retry
- Больше воркеров (8)
- Строгие требования к качеству данных
- Максимальное количество страниц

**Использование**:
```bash
bioactivity-data-acquisition get-document-data --config configs/config_documents_full.yaml
```

## Ключевые параметры

### Источники данных
- **ChEMBL**: Основной источник документов
- **Crossref**: Дополнительные метаданные по DOI
- **OpenAlex**: Альтернативные метаданные
- **PubMed**: Медицинские публикации
- **Semantic Scholar**: Академические метаданные (опционально)

### Нормализация журналов
```yaml
postprocess:
  journal_normalization:
    enabled: true
    columns: ["journal", "pubmed_journal", "chembl_journal", "crossref_journal"]
```

### Формирование ссылок
```yaml
postprocess:
  citation_formatting:
    enabled: true
    columns:
      journal: "journal"
      volume: "volume"
      issue: "issue"
      first_page: "first_page"
      last_page: "last_page"
```

### Runtime параметры
- `workers`: Количество параллельных воркеров
- `limit`: Ограничение количества документов (null = без ограничений)
- `dry_run`: Режим тестирования без сохранения
- `date_tag`: Тег даты в имени файла

## Переменные окружения

Для работы с API ключами установите следующие переменные:

```bash
export CHEMBL_API_TOKEN="your_chembl_token"
export CROSSREF_API_KEY="your_crossref_key"
export PUBMED_API_KEY="your_pubmed_key"
export SEMANTIC_SCHOLAR_API_KEY="your_semantic_scholar_key"
```

## Рекомендации по использованию

1. **Для разработки**: Используйте `config_documents_test.yaml`
2. **Для продакшена**: Используйте `config_documents.yaml` или `config_documents_full.yaml`
3. **Для отладки**: Включите DEBUG уровень логирования
4. **Для больших объемов**: Увеличьте количество воркеров и таймауты

## Выходные файлы

После обработки создаются следующие файлы:
- `documents_<date_tag>.csv` - Основные данные с нормализованными журналами и ссылками
- `documents_<date_tag>_qc.csv` - Отчет о качестве данных

## Мониторинг

Для мониторинга API лимитов используйте:
```bash
python -m library.tools.check_api_limits
python -m library.tools.monitor_api
```
