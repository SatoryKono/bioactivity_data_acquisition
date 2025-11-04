# Патчи для исправления критичных проблем

## 1. Создание отсутствующих файлов из .lychee.toml

### docs/architecture/00-architecture-overview.md

```diff
+ # Architecture Overview
+
+ This document provides an overview of the bioetl framework architecture.
+
+ [This file should be created or removed from .lychee.toml]
```


## 2. Исправление противоречий в именовании

### Унификация chunk_size vs batch_size

```diff
--- docs/pipelines/09-document-chembl-extraction.md
+++ docs/pipelines/09-document-chembl-extraction.md
@@ -XXX,XX +XXX,XX @@
- | Sources / ChEMBL | `sources.chembl.chunk_size` | `10` |
+ | Sources / ChEMBL | `sources.chembl.batch_size` | `10` |
```

**Обоснование:** Для согласованности с остальными пайплайнами используем batch_size.

