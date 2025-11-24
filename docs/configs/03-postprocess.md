# Postprocess configuration

> **Note**: Implementation status: **implemented**. The classes live in
> `src/bioetl/config/models/postprocess.py` and are wired into
> `PipelineDomainConfig`.

`postprocess` captures the deterministic artefacts emitted after the write
stage: correlation studies, auxiliary QC reports, and per-pipeline enrichments.
Every pipeline inherits defaults from `configs/defaults/postprocess.yaml`, which
currently focuses on correlation metrics that can be toggled independently from
`--extended`.

## Schema

| Key | Type | Default | Description |
| --- | ---- | ------- | ----------- |
| `postprocess.correlation.enabled` | `bool` | `false` | Enables correlation report generation even when the CLI does not request `--extended`. |

The top-level `postprocess` section uses `extra="forbid"`, so any new toggles
must be declared explicitly. For pipelines that require additional post-run
steps, extend this document and the corresponding Pydantic model.

## Defaults

```yaml
# configs/defaults/postprocess.yaml
postprocess:
  correlation:
    enabled: false
```

## Usage

1. Include the default profile in your pipeline YAML:
   `<<: !include ../../defaults/postprocess.yaml`.
2. Override fields when needed:

   ```yaml
   postprocess:
     correlation:
       enabled: true
   ```
3. Verify the merged output with `bioetl config inspect --set postprocess.correlation.enabled=true`.

When combined with `bioetl config inspect`, these knobs become part of the
standard configuration workflow alongside `fallbacks` and `validation`
sections.
