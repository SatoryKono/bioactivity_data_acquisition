# Activity ChEMBL: Data Extraction

This document describes the `extract` stage of the ChEMBL Activity pipeline. It details how the pipeline connects to the ChEMBL API, retrieves activity records, and prepares them for the subsequent `transform` stage.

## 1. Overview

The primary goal of the extraction stage is to fetch biological activity data from the ChEMBL REST API. This process is implemented in the `ChemblActivityPipeline.extract()` method.

The key responsibilities of this stage are:
- Establishing a connection to the ChEMBL API using the configured `UnifiedAPIClient`.
- Capturing the ChEMBL database release version for lineage tracking.
- Systematically paginating through the `/activity.json` endpoint to retrieve all relevant records.
- Respecting the optional `--limit` CLI flag to perform partial extractions for testing or debugging.
- Loading the collected records into a pandas DataFrame.

## 2. Extraction Process

The extraction follows a well-defined sequence of operations:

1.  **Client Initialization**: An instance of `UnifiedAPIClient` is created using the configuration defined under the `sources.chembl` key in the pipeline's YAML config. This client handles all HTTP requests, retries, and rate limiting.

2.  **Release Handshake**: Before fetching activity data, the pipeline makes a request to the `/status.json` endpoint. The `chembl_release` value from the response is captured and stored. This version is later embedded in the output metadata to ensure traceability.

3.  **Paginated Retrieval**: The pipeline starts fetching data from the `/activity.json` endpoint. The ChEMBL API uses a cursor-based pagination system. Each response contains a `page_meta` object with a `next` field, which provides the endpoint for the next page of results.

4.  **Iteration**: The pipeline continuously fetches pages by following the `next` links until the `next` field is `null`, indicating that all pages have been retrieved.

5.  **Record Limiting**: If the `--limit` flag is used, the extraction process will stop once the specified number of records has been collected.

6.  **DataFrame Creation**: All collected JSON records are consolidated and loaded into a single `pandas.DataFrame`, which is the output of this stage.

## 3. API Interaction Details

### Endpoint

- **Primary Endpoint**: `/activity.json`
- **Status Endpoint**: `/status.json` (for release version)

### Pagination

The pipeline does not use traditional offset-based pagination. Instead, it relies entirely on the `next` URL provided in the `page_meta` object of each API response.

A typical `page_meta` object looks like this:

```json
{
  "page_meta": {
    "limit": 20,
    "next": "/chembl/api/data/activity?limit=20&offset=20",
    "offset": 0,
    "previous": null,
    "total_count": 50000
  }
}
```

The `ChemblActivityPipeline` extracts the path and query string from the `next` URL (e.g., `/activity?limit=20&offset=20`) and uses it for the subsequent request. This makes the pagination mechanism robust and independent of the underlying offset/limit implementation.

### Configuration

The extraction process is configured through the `sources.chembl` section of the pipeline's configuration file. Key parameters include:

- **`base_url`**: The base URL for the ChEMBL API (e.g., `https://www.ebi.ac.uk/chembl/api/data`).
- **`batch_size`**: This parameter controls the `limit` query parameter for each page request. It is capped at a maximum of 25 by the pipeline to avoid generating URLs that are too long for the ChEMBL server to handle.

## 4. Output

The `extract` stage returns a single `pandas.DataFrame`. The columns of the DataFrame correspond directly to the keys in the JSON objects returned by the ChEMBL `/activity` endpoint. This DataFrame is then passed as the input payload to the `transform` stage.
