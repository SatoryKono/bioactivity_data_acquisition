# 62 Document Crossref Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the Document (Crossref) pipeline.

## Sort Keys

`['doi']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [61-document-crossref-io.md](61-document-crossref-io.md)
- [00-document-crossref-overview.md](00-document-crossref-overview.md)
