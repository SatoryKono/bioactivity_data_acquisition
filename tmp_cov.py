import json
from pathlib import Path
with open('coverage-clients-cli-schemas.json', 'r', encoding='utf-8') as fh:
    data = json.load(fh)
files = data['files']
threshold = 0.95
for fname, info in sorted(files.items()):
    norm = fname.replace('\\', '/')
    if not (norm.startswith('src/bioetl/clients/') or norm.startswith('src/bioetl/cli/') or norm.startswith('src/bioetl/schemas/')):
        continue
    summary = info['summary']
    total = summary['num_statements']
    cov = summary['covered_lines']/total if total else 1.0
    if cov >= threshold:
        continue
    missing = [line for line in info['missing_lines'] if line not in (info.get('excluded_lines') or [])]
    print(f"{norm}: {cov*100:.2f}% ({len(missing)} lines)")
    print("    missing:", missing[:20])
