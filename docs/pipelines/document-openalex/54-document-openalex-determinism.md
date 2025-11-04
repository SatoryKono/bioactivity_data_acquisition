# 54 Document OpenAlex Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the Document (OpenAlex) pipeline.

## Sort Keys

`['openalex_id']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [53-document-openalex-io.md](53-document-openalex-io.md)
- [00-document-openalex-overview.md](00-document-openalex-overview.md)
