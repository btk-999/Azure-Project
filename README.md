# Azure-Project — Dynamic Multi-CSV ETL Pipeline

An end-to-end Azure data engineering project that dynamically ingests multiple CSV files from a GitHub source, lands them in Azure Data Lake Storage Gen2, and transforms them using Synapse Analytics (PySpark).

## Architecture

**Flow:** GitHub (CSV source) → Azure Data Factory (dynamic ingestion) → ADLS Gen2 (raw storage) → Synapse Analytics / PySpark (transformation) → Silver layer (cleaned data)

| Stage | Service | Purpose |
|---|---|---|
| Ingestion | Azure Data Factory | Dynamically copies multiple CSVs from a GitHub source using a parameterized ForEach + Copy pipeline — no hardcoded per-file activities |
| Storage | Azure Data Lake Storage Gen2 (`adwrsdatalake`) | Stores raw and processed files |
| Transformation | Azure Synapse Analytics (PySpark) | Cleans and transforms raw CSVs into a structured Silver layer |

## Why a dynamic pipeline?
Instead of building one Copy activity per CSV file, the pipeline uses a single parameterized dataset and a `ForEach` activity driven by a file-name array. Adding a new source file means updating one parameter — not rebuilding the pipeline.

## Resources Used
- **Resource Group:** `azr_adwrs_project`
- **Storage Account:** `adwrsdatalake` (ADLS Gen2)
- **Azure Data Factory:** `adwrsdf`
- **Synapse Workspace:** `snpsadwrsproject`

## Repo Structure

```
Azure-Project/
├── dataset/           # ADF dataset definitions (parameterized source & sink)
├── factory/           # ADF factory-level configuration
├── linkedService/      # Connections to GitHub (HTTP) and ADLS Gen2
├── pipeline/           # ADF pipeline JSON (dynamic ForEach + Copy logic)
├── silver_layer        # Synapse PySpark notebook — cleans and transforms raw CSVs
├── publish_config.json # ADF publish configuration
└── README.md
```

## Data Quality Handled in Transformation
- Type conversion issues
- Null value handling
- Invalid categorical values
- Schema drift across CSVs
- Malformed date formats
- Duplicate records

## Tech Stack
Azure Data Factory · Azure Data Lake Storage Gen2 · Azure Synapse Analytics · PySpark · GitHub (CI/CD-connected via native Git integration)

## Author
Bhargav Teja Kunigiri — Data Engineer transitioning from Talend/TAC to the Azure-native data ecosystem.
