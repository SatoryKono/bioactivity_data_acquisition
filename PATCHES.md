```diff
--- a/docs/pipelines/05-assay-extraction.md
+++ b/docs/pipelines/05-assay-extraction.md
@@
-**Унифицированный интерфейс**: Все пайплайны используют единую команду `bioetl pipeline run`. См. стандарт в [10-configuration.md](10-configuration.md#53-cli-interface-specification-aud-4).
-```bash
-# Golden compare для детерминизма
-bioetl pipeline run --config configs/pipelines/assay.yaml \
-  --golden golden_assay.csv
+**Унифицированный интерфейс**: Все пайплайны запускаются через `python -m bioetl.cli.main <pipeline>`, что совпадает с README и каталогом пайплайнов.
+```bash
+# Golden compare для детерминизма
+python -m bioetl.cli.main assay \
+  --config configs/pipelines/chembl/assay.yaml \
+  --golden golden_assay.csv
@@
-# Sample с ограничением
-bioetl pipeline run --config configs/pipelines/assay.yaml \
-  --sample 100
+# Sample с ограничением
+python -m bioetl.cli.main assay \
+  --config configs/pipelines/chembl/assay.yaml \
+  --sample 100
@@
-# Контроль API параметров
-bioetl pipeline run --config configs/pipelines/assay.yaml \
-  --set sources.chembl.max_url_length=2000 \
-  --set sources.chembl.batch_size=25
+# Контроль API параметров
+python -m bioetl.cli.main assay \
+  --config configs/pipelines/chembl/assay.yaml \
+  --set sources.chembl.max_url_length=2000 \
+  --set sources.chembl.batch_size=25
@@
-# Strict validation
-bioetl pipeline run --config configs/pipelines/assay.yaml \
-  --fail-on-schema-drift \
-  --set qc.severity_threshold=error
+# Strict validation
+python -m bioetl.cli.main assay \
+  --config configs/pipelines/chembl/assay.yaml \
+  --fail-on-schema-drift \
+  --set qc.severity_threshold=error
```

```diff
--- a/docs/pipelines/06-activity-data-extraction.md
+++ b/docs/pipelines/06-activity-data-extraction.md
@@
-- Следует стандарту `docs/configs/00-typed-configs-and-profiles.md`.
-- Профильный файл: `configs/pipelines/activity.yaml` (`extends: "../base.yaml"`).
+- Следует стандарту `docs/configs/00-typed-configs-and-profiles.md`.
+- Профильный файл: `configs/pipelines/chembl/activity.yaml` (`extends: "../base.yaml"`).
```

```diff
--- a/docs/pipelines/09-document-chembl-extraction.md
+++ b/docs/pipelines/09-document-chembl-extraction.md
@@
-- Профильный файл: `configs/pipelines/document.yaml` (`extends: "../base.yaml"`).
+- Профильный файл: `configs/pipelines/chembl/document.yaml` (`extends: "../base.yaml"`).
```

```diff
--- a/.lychee.toml
+++ b/.lychee.toml
@@
-retry_wait_time = "2s"
+retry_wait_time = 2
```
