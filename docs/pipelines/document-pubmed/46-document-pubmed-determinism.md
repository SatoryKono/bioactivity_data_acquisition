# 46 Document PubMed Determinism

**Version:** 1.0.0  
**Date:** 2025-01-29  
**Author:** Data Acquisition Team

## Purpose

This document describes determinism requirements for the Document (PubMed) pipeline.

## Sort Keys

`['pmid']`

## Hash Generation

SHA256-based row and business key hashing per global determinism policy.

## Related Documentation

- [Determinism Policy](../determinism/00-determinism-policy.md)
- [45-document-pubmed-io.md](45-document-pubmed-io.md)
- [00-document-pubmed-overview.md](00-document-pubmed-overview.md)
