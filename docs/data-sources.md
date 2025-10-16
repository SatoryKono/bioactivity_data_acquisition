# Data sources

Минимальная информация по источникам и типовым ограничениям.

## ChEMBL
- Базовый URL: https://www.ebi.ac.uk/chembl/api/data
- Типовые ограничения: страничная пагинация, возможные 5xx/429

## Crossref
- Базовый URL: https://api.crossref.org/works
- Токен: заголовок `Crossref-Plus-API-Token`

## OpenAlex
- Базовый URL: https://api.openalex.org/works

## PubMed
- Базовый URL: https://eutils.ncbi.nlm.nih.gov/entrez/eutils/
- Ключ `api_key` повышает лимиты RPS

## Semantic Scholar
- Базовый URL: https://api.semanticscholar.org/graph/v1/paper
- Строгие лимиты без API‑ключа; рекомендуется ключ `x-api-key`

См. также: api-limits.md и docs/operations.md для скриптов мониторинга и интерпретации отчётов.
