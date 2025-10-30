# De-duplication Plan

Этот документ описывает план детерминированной дедупликации для всего пайплайна.

## Guiding Principles

- Опора на канонические идентификаторы (ChEMBL ID, UniProt, PubChem)
- Чёткие бизнес-ключи и хеши BLAKE2 для производных
- Идемпотентность и трассируемость

## Strategy Overview

- Вводим слой нормализации идентификаторов
- Детерминированная сортировка строк перед операциями слияния
- Явные политики конфликтов

## Client Initialization

- Shared клиент ChEMBL и адаптеры с TTL‑кэшем и троттлингом
- Callers must persist the returned client alongside the resolved batch and
  limit metadata to honour the shared runtime contract, and tests should
  monkeypatch `_init_chembl_client` to intercept client creation in a single
  location.
