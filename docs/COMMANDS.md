# Cursor Commands — Bioactivity ETL

Paste these into `.cursor/rules` or keep as a playbook. Each command is deterministic and traceable.

---

## /run-activity

Goal: fetch ChEMBL activity data with defaults and QC hooks.
Defaults:

- INPUT: data/input/activity.csv
- CONFIG: configs/pipelines/activity.yaml
- OUTPUT: data/output/activity
- SAMPLE: 10

Bash:

```bash

python src/scripts/run_activity.py --input-file "data/input/activity.csv" --config "configs/pipelines/activity.yaml" --output-dir "data/output/activity" --sample 10

```

PowerShell:

```powershell

python src\scripts\run_activity.py --input-file data\input\activity.csv --config configs\pipelines\activity.yaml --output-dir data\output\activity --sample 10

```

---

## /run-assay

Goal: fetch assay data with defaults and a brief artifact summary.
Defaults:

- INPUT: data/input/assay.csv
- CONFIG: configs/pipelines/assay.yaml
- OUTPUT: data/output/assay
- SAMPLE: 10

Bash:

```bash

python src/scripts/run_assay.py --input-file "data/input/assay.csv" --config "configs/pipelines/assay.yaml" --output-dir "data/output/assay" --sample 10

```

PowerShell:

```powershell

python src\scripts\run_assay.py --input-file data\input\assay.csv --config configs\pipelines\assay.yaml --output-dir data\output\assay --sample 10

```

---

## /run-document

Goal: fetch publication metadata with defaults and QC hooks.
Defaults:

- INPUT: data/input/document.csv
- CONFIG: configs/pipelines/document.yaml
- OUTPUT: data/output/document
- SAMPLE: 10

Bash:

```bash

python src/scripts/run_document.py --input-file "data/input/document.csv" --config "configs/pipelines/document.yaml" --output-dir "data/output/document" --sample 10

```

PowerShell:

```powershell

python src\scripts\run_document.py --input-file data\input\document.csv --config configs\pipelines\document.yaml --output-dir data\output\document --sample 10

```

---

## /run-target

Goal: fetch target data with defaults and QC hooks.
Defaults:

- INPUT: data/input/target.csv
- CONFIG: configs/pipelines/target.yaml
- OUTPUT: data/output/target
- SAMPLE: 10

Bash:

```bash

python src/scripts/run_target.py --input-file "data/input/target.csv" --config "configs/pipelines/target.yaml" --output-dir "data/output/target" --sample 10

```

PowerShell:

```powershell

python src\scripts\run_target.py --input-file data\input\target.csv --config configs\pipelines\target.yaml --output-dir data\output\target --sample 10

```

---

## /run-testitem

Goal: fetch testitem data with defaults and QC recap.
Defaults:

- INPUT: data/input/testitem.csv
- CONFIG: configs/pipelines/testitem.yaml
- OUTPUT: data/output/testitem
- SAMPLE: 10

Bash:

```bash

python src/scripts/run_testitem.py --input-file "data/input/testitem.csv" --config "configs/pipelines/testitem.yaml" --output-dir "data/output/testitem" --sample 10

```

PowerShell:

```powershell

python src\scripts\run_testitem.py --input-file data\input\testitem.csv --config configs\pipelines\testitem.yaml --output-dir data\output\testitem --sample 10

```

---

## /run-inventory

Goal: regenerate the pipeline inventory snapshot and cluster report artifacts.

Defaults:

- CONFIG: configs/inventory.yaml
- OUTPUT: docs/pipelines/PIPELINES.inventory.csv
- CLUSTERS: docs/pipelines/PIPELINES.inventory.clusters.md

Bash:

```bash

python src/scripts/run_inventory.py --config "configs/inventory.yaml"

```

PowerShell:

```powershell

python src\scripts\run_inventory.py --config configs\inventory.yaml

```

To verify the committed snapshot without rewriting files, append `--check` to the command.

---

## /validate-columns

Goal: validate output columns against requirements for specific pipeline.

Usage:

```bash

python src/scripts/validate_columns.py --entity activity --schema-version latest

```

PowerShell:

```powershell

python src\scripts\validate_columns.py --entity activity --schema-version latest

```

---

## /validate-all-columns

Goal: validate output columns for all pipelines.

Usage:

```bash

python src/scripts/validate_columns.py --entity all --schema-version latest

```

PowerShell:

```powershell

python src\scripts\validate_columns.py --entity all --schema-version latest

```

---

## /qc-summary

Goal: print paths, sizes, and row counts where available for last run outputs.

Bash:

```bash

ls -l data/output || true
python - << 'PY'
import glob, csv, os
for p in glob.glob('data/output/**/*.csv', recursive=True):
    try:
        with open(p, newline='', encoding='utf-8') as f:
            n = sum(1 for _ in csv.reader(f))
        print(f"{p} rows={n}")
    except Exception as e:
        print(f"{p} rows=?  {e}")
PY

```

PowerShell:

```powershell

Get-ChildItem -Recurse data\output | Format-List
python - << 'PY'
import glob, csv
for p in glob.glob('data/output/**/*.csv', recursive=True):
    try:
        with open(p, newline='', encoding='utf-8') as f:
            n = sum(1 for _ in csv.reader(f))
        print(f"{p}\trows={n}")
    except Exception as e:
        print(f"{p}\trows=?\t{e}")
PY

```
