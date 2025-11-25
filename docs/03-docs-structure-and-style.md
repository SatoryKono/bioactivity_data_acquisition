# Docs structure and style

This document defines how the `docs/` directory is structured and how
new documentation should be written.

It complements the IDE rules under `.windsurf/rules`, especially:

- `01-naming-conventions.md`
- `docs-filenames-lowercase-hyphen.md`
- `docs-landing-pages-index-readme.md`

## Scope

These rules apply to all markdown files under `docs/` and its
subdirectories.

## Directory layout

- Top-level sections under `docs/` use numbered, hyphenated filenames.
  Examples:
  - `00-project-overview.md`
  - `01-architecture-overview.md`
  - `02-pipelines-overview.md`
- Each major section directory under `docs/` has an `INDEX.md` entry
  point. Examples:
  - `docs/INDEX.md`
  - `docs/pipelines/INDEX.md`
- Subdirectories for concrete entities (for example, individual
  pipelines) use `README.md` as their entry point.
  Examples:
  - `docs/pipelines/activity-chembl/README.md`
  - `docs/pipelines/document-chembl/README.md`

## Filenames and ordering

- Filenames are in English, lowercase, with words separated by hyphens.
  No spaces, CamelCase, or underscores.
- Ordered documents in a section use a two-digit numeric prefix
  `NN-` to express ordering, followed by a descriptive name.
  Examples:
  - `00-project-overview.md`
  - `03-docs-structure-and-style.md`
- Section landing pages keep the simple name `INDEX.md` and do not use
  numeric prefixes.

## Cross-linking

- Use relative links between docs. Examples:
  - `../datatypes/activity.md`
  - `./02-pipelines-overview.md`
  - `pipelines/INDEX.md`
- When linking into a section, prefer its `INDEX.md` or `README.md`
  instead of deep links to specific headings, unless a deep link is
  required for clarity.
- Keep link text descriptive rather than repeating the raw filename.

## CLI and config examples

- Use fenced code blocks with an explicit language, typically `bash` for
  shell commands and `yaml` for configuration snippets.

  ```bash
  bioetl run activity_chembl \
    --config configs/pipelines/activity/example.yaml \
    --profile determinism
  ```

  ```yaml
  pipeline:
    code: activity_chembl
    run_id: example-run-001
  ```

- Show complete, copy-pasteable examples with explicit flags and
  profiles, avoiding ellipses (`...`) in command lines.
- When documenting configuration precedence, keep the order explicit:
  `env > CLI --set > pipeline-config > profiles`.

## Language and formatting

- Documentation is written in English.
- Use clear, concise paragraphs and prefer lists over long blocks of
  text when enumerating rules or steps.
- Headings should be consistent within a document; `Title Case` is
  recommended for top-level and section headings.
- Keep line length reasonably short for readability (around
  80 characters where practical), but do not break links or code blocks
  solely for this reason.

## Adding new docs

When adding new documentation under `docs/`:

1. Decide where it belongs in the directory tree and whether it needs a
   numeric prefix.
2. Ensure the filename is lowercase with hyphens and, if applicable, a
   two-digit `NN-` prefix.
3. Add or update links from the relevant `INDEX.md` or `README.md`
   files so the new document is discoverable.
4. Follow the CLI and config example conventions when documenting
   commands or configuration.
