# Azure-Project — Dynamic Multi-CSV ETL Pipeline

An end-to-end Azure data engineering project that dynamically ingests multiple CSV files, transforms them using PySpark on Databricks through a Medallion Architecture, and serves the processed data through Synapse Analytics to Power BI.

## Architecture

**Flow:** Source CSVs → Azure Data Factory (dynamic ingestion) → ADLS Gen2 (Bronze) → Azure Databricks / PySpark (Bronze → Silver → Gold transformation) → ADLS Gen2 (Gold) → Synapse Serverless SQL (external tables) → Power BI (dashboard)

| Stage | Service | Purpose |
|---|---|---|
| Ingestion | Azure Data Factory | Dynamically copies multiple CSV files using a parameterized Lookup → ForEach → Copy pipeline — no hardcoded per-file activities |
| Raw Storage | Azure Data Lake Storage Gen2 (`adwrsdatalake`) | Bronze layer — raw, untouched files |
| Transformation | Azure Databricks (PySpark) | Cleans, validates, and aggregates data through Bronze → Silver → Gold layers |
| Serving | Azure Synapse Analytics (Serverless SQL) | External tables (Parquet-backed) over the Gold layer, queryable via SQL without moving data |
| Visualization | Power BI | Live dashboard on order volume, customer counts, and trends |

## Why this split between Databricks and Synapse?
Databricks handles the actual data transformation logic (PySpark notebooks) since it's purpose-built for iterative Spark development. Synapse Serverless SQL is used purely as a serving layer — creating external tables directly over the Gold Parquet files so Power BI (and any other BI tool) can query them with plain SQL, without duplicating data into a separate warehouse.

## Why a dynamic ingestion pipeline?
Instead of one Copy activity per source file, the ADF pipeline uses a Lookup activity to fetch the file list, then a ForEach activity loops through it, driving a single parameterized Copy activity (`tgt_folder`, `tgt_file` resolved via `@item()`). Adding a new source file means updating the file list — not rebuilding the pipeline.

## Resources Used
- **Resource Group:** `azr_adwrs_project`
- **Storage Account (raw/bronze):** `adwrsdatalake` (ADLS Gen2)
- **Azure Data Factory:** `adwrsdf`
- **Azure Databricks Service:** `adwrs_project`
- **Synapse Workspace:** `snpsadwrsproject`
- **Synapse Storage Account:** `snpsstorage`

## Repo Structure

```
Azure-Project/
├── dataset/            # ADF dataset definitions (parameterized source & sink)
├── factory/            # ADF factory-level configuration
├── linkedService/       # Connections to source, ADLS Gen2, Databricks
├── pipeline/            # ADF pipeline JSON (Lookup + ForEach + Copy logic)
├── notebooks/           # Databricks PySpark notebooks (Bronze/Silver/Gold transforms)
├── sql/                 # Synapse SQL scripts (external tables, schema creation, views)
├── publish_config.json  # ADF publish configuration
└── README.md
```

## Data Quality Handled in Transformation
- Type conversion issues
- Null value handling
- Invalid categorical values
- Schema drift across source files
- Malformed date formats
- Duplicate records

## Tech Stack
Azure Data Factory · Azure Data Lake Storage Gen2 · Azure Databricks · PySpark · Azure Synapse Analytics (Serverless SQL) · Power BI · GitHub (CI/CD-connected via native Git integration with ADF and Synapse)

## Author
Bhargav Teja Kunigiri — Data Engineer transitioning from Talend/TAC to the Azure-native data ecosystem.
