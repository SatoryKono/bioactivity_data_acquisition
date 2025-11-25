# Schemas registry overview

BioETL uses Pandera schemas as a central mechanism to validate all
datasets before they are written.

All public schemas live in the `bioetl.schemas` package and are exposed
through a central registry function (for example `get_schema`).

## Core principles

- Validate-before-write:
  every dataset must be validated against a Pandera schema before it is
  written to disk.
- Strict and ordered schemas:
  schemas are configured with `strict=True`, `ordered=True`, and
  `coerce=True` to enforce column presence, order, and types.
- Deterministic column order:
  column order in output files follows the order defined in the schema.

## Using the registry

Typical usage pattern:

1. Look up the schema by name or entity code using the central
   registry function.
2. Apply any lightweight normalization required to match the schema
   (for example, ensuring all required columns exist).
3. Call `schema.validate(df)` before writing the dataframe to storage.

This pattern ensures that all written datasets respect the same
contracts and can be validated consistently in CI.

## Relationship to datatype docs

The `datatypes/` directory contains human-readable descriptions of
fields for each entity (for example `activity.md`, `assay.md`,
`target.md`).

Pandera schemas provide the executable specification of these tables,
including:

- column names and order,
- data types and nullability rules,
- constraints and allowed values where applicable.

Both views are complementary: datatype documentation explains the
business meaning, while schemas enforce the technical contract.
