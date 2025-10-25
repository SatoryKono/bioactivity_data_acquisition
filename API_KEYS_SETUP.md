# Настройка API ключей

## Полученные API ключи

### Semantic Scholar API
- **API Key**: `o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4`
- **Rate Limit**: 1 запрос в секунду
- **Header**: `x-api-key`

## Настройка

### 1. Создайте файл `.env.local`

Скопируйте файл `api_keys.env` в `.env.local`:

```bash
cp api_keys.env .env.local
```

### 2. Заполните API ключи в `.env.local`

```bash
# API Keys for external services

# ChEMBL API (optional - for higher rate limits)
CHEMBL_API_TOKEN=your_chembl_token_here

# PubMed API (optional - for higher rate limits)  
PUBMED_API_KEY=your_pubmed_key_here

# Semantic Scholar API (required for higher rate limits)
SEMANTIC_SCHOLAR_API_KEY=o2N1y1RHYU3aqEj556Oyv4oBzZrHthM2bWda2lf4

# Crossref API (optional - for higher rate limits)
CROSSREF_API_KEY=your_crossref_key_here
```

### 3. Обновленная конфигурация

В `configs/config_document.yaml` уже настроен Semantic Scholar с:
- API ключом в заголовке `x-api-key`
- Rate limit: 0.8 запросов в секунду (для безопасности)
- Правильными настройками retry и timeout

## Проверка настройки

После настройки API ключей, пайплайн documents будет использовать:
- **Semantic Scholar**: с API ключом для увеличенных лимитов
- **Другие источники**: с базовыми лимитами (если не указаны ключи)

## Безопасность

⚠️ **Важно**: 
- Файл `.env.local` добавлен в `.gitignore` и не будет закоммичен
- Не передавайте API ключи в публичных репозиториях
- Регулярно ротируйте ключи для безопасности
