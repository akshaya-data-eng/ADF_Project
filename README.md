# ADF_Project
# ✈️ ADF Aviation Data Pipeline Project

A full end-to-end Azure Data Factory (ADF) pipeline project for ingesting, transforming, and serving aviation data across Bronze, Silver, and Gold layers using the **Medallion Architecture**.

---

## 📁 Project Structure

```
ADF Project
├── Pipelines
│   ├── onprem_ingestion       → On-premises file ingestion
│   ├── API_Ingestion          → REST API data ingestion
│   ├── SQLToDataLake          → Incremental SQL ingestion (Watermark)
│   ├── SilverLayer            → Silver layer transformation
│   ├── GoldLayer              → Gold layer data serving
│   └── Parent pipeline        → Orchestrates all child pipelines
│
├── Data Flows
│   ├── DataTransformation     → Silver layer transformations
│   └── DataServing            → Gold layer aggregations & rankings
│
├── Datasets
│   ├── ds_onprem_src_param    → Parameterized on-prem source
│   ├── ds_src_api             → REST API source
│   ├── ds_sqlsource           → SQL Server source
│   └── (sink datasets)        → ADLS Gen2 targets
│
└── Linked Services
    ├── Self-Hosted IR          → On-prem connectivity (aksh-SelfHosted)
    └── Azure IR                → Cloud service connectivity
```

---

## 🔄 Pipeline Overview

### 1. `onprem_ingestion` — On-Premises File Ingestion

Ingests multiple files from an on-premises file system to ADLS Gen2 using a **ForEach** loop.

**Flow:**
```
ForEach (iterate files array)
    └── MigrateOnPremFile (Copy Data)
```

**Key Design:**
- Uses a `files` parameter (Array) containing file metadata like `file_name`
- Source dataset: `ds_onprem_src_param` with `p_file_name` = `@item().file_name`
- Mapping parameters: `p_mapping_flight`, `p_mapping_passenger`, `p_mapping_airline` (Object type)
- Connected via **Self-Hosted Integration Runtime** (`aksh-SelfHosted`)

---

### 2. `API_Ingestion` — REST API Ingestion

Fetches data from an external REST API and loads it into ADLS Gen2.

**Flow:**
```
WebAPICall (Web Activity)
    └── CopyAPIData (Copy Data)
```

**Key Design:**
- Web activity calls the API endpoint first
- Copy activity uses `ds_src_api` dataset with `GET` request method
- Sink writes to ADLS Gen2

---

### 3. `SQLToDataLake` — Incremental SQL Ingestion (Watermark Pattern)

Implements a **watermark-based incremental load** from SQL Server to ADLS Gen2.

**Flow:**
```
LastLoad (Lookup)  ──┐
                     ├──► CopySQLData (Copy Data) ──► watermark (Copy Data)
LatestLoad (Lookup) ─┘
```

**Key Design:**
- `LastLoad` lookup fetches the previous watermark timestamp
- `LatestLoad` lookup fetches the max timestamp from source
- `CopySQLData` queries: `SELECT * FROM dbo.FactBookings WHERE ...` (filtered between watermarks)
- After successful copy, `watermark` activity updates the watermark table

---

### 4. `SilverLayer` — Silver Layer Pipeline

Triggers the Silver layer **Data Flow** transformation.

**Flow:**
```
SilverDataflow (Data Flow Activity)
```

- Runs `DataTransformation` data flow
- Cleans, enriches, and structures raw Bronze data into Silver

---

### 5. `GoldLayer` — Gold Layer Pipeline

Triggers the Gold layer **Data Flow** for serving aggregated analytics.

**Flow:**
```
GoldDataflow (Data Flow Activity) → DataServing data flow
```

- Runs on **AutoResolveIntegrationRuntime**
- Compute size: **Small**
- Logging level: **None**

---

### 6. `Parent Pipeline` — Orchestrator

Sequentially executes all ingestion and transformation pipelines.

**Flow:**
```
ExecuteOnPrem ──► ExecuteAPI ──► ExecuteIncremental
(onprem_ingestion)  (API_Ingestion)  (SQLToDataLake)
```

**Parameters passed:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `files` | Array | List of files with metadata for on-prem ingestion |
| `p_mapping_flight` | String | Column mapping for Flight table |
| `p_mapping_passenger` | String | Column mapping for Passenger table |
| `p_mapping_airline` | String | Column mapping for Airline table |

---

## 🔀 Data Flow: `DataTransformation` (Silver Layer)

Transforms raw Bronze data into clean Silver layer tables.

| Stream | Source | Transformations | Sink |
|--------|--------|-----------------|------|
| **DimAirline** | `ds_silver_src` | Derived columns (airline_id, airline_name, country) → AlterRow | sink1 |
| **DimPassenger** | `ds_dimpass_src` | Select → Derived (GenderFlag) → Derived (GenderFe) → Filter (age > 25) → Derived (Name) → AlterRow | sink2 |
| **FactPassenger** | `ds_fact_src` | Cast columns (castCost) → AlterRow | sink3 |
| **DimFlight** | `ds_dimflight_src` | Select columns (flight_id, ...) → AlterRow | sink4 |
| **Airport** | `ds_airport_src` | Derived (airport_id, airport_name, city, ...) → AlterRow | sink5 |

**Notable Transformations:**
- `filterGreater25` — Filters passengers where `age > 25`
- `derivedGenderFlag` — Creates gender flag column
- `castCost` — Casts cost columns to correct data types

---

## 🏆 Data Flow: `DataServing` (Gold Layer)

Aggregates and ranks data for analytics and reporting.

### Stream 1 — Flight Total Cost Analysis
```
DimFlight + FactBookings
    └── FlightFactBookings (Inner Join)
        └── selectCol (rename columns)
            └── aggregateCost (GROUP BY flight_number → SUM Total_cost)
                └── sortingCost (Sort Total_cost DESC)
                    └── filterGreater5000 (Total_cost > 5000)
                        └── alterRow2
                            └── sinkGoldFlight
```

### Stream 2 — Flight Check-in Status Count
```
FlightFactBookings
    └── selectCol
        └── aggregateCheckStatus (GROUP BY checkin_status → COUNT)
            └── alterRow3
                └── sinkFlightCheckIn
```

### Stream 3 — Top 5 Airlines by Total Sales
```
FactBookings + DimAirline
    └── AirlineJoin (Left Outer Join)
        └── selectCols
            └── aggregateAirline (GROUP BY airline_name → SUM Total_Sales)
                └── Ranking (Window function → Top ranking)
                    └── filterTop5 (Filter Top <= 5)
                        └── alterRow1
                            └── sinkGoldAirline
```

---

## ⚙️ Infrastructure

### Integration Runtimes

| Runtime | Type | Usage |
|---------|------|-------|
| `aksh-SelfHosted` | Self-Hosted IR | On-premises file system access |
| `AutoResolveIntegrationRuntime` | Azure IR | Cloud-to-cloud data flows |

### Linked Services
- **File System** — Connected via Self-Hosted IR to `C:\adf_projectfiles`
- **ADLS Gen2** — Azure Data Lake Storage Gen2 (sink)
- **Azure SQL / SQL Server** — Source for incremental ingestion
- **REST API** — External HTTP API source

---

## 🗂️ Data Model

```
Bronze Layer (Raw)          Silver Layer (Cleaned)        Gold Layer (Aggregated)
──────────────────          ──────────────────────        ───────────────────────
DimPassenger (CSV)    ───► DimPassenger (Filtered)  ───► Top Airlines by Sales
DimFlight (CSV)       ───► DimFlight (Selected)     ───► Flight Cost Summary
DimAirline (CSV)      ───► DimAirline (Enriched)    ───► Check-in Status Count
Airport (CSV)         ───► Airport (Derived)
FactBookings (SQL)    ───► FactPassenger (Cast)
API Data (REST)
```

---

## 🚀 How to Run

1. **Trigger Parent Pipeline** manually or via schedule trigger
2. Execution order (sequential):
   - `onprem_ingestion` → migrates on-prem CSV files to Bronze
   - `API_Ingestion` → fetches API data to Bronze
   - `SQLToDataLake` → incremental SQL load to Bronze
3. **SilverLayer pipeline** — runs `DataTransformation` data flow
4. **GoldLayer pipeline** — runs `DataServing` data flow

> ⚠️ Ensure the Self-Hosted Integration Runtime (`aksh-SelfHosted`) is running on the on-premises machine before triggering the pipeline.

---

## 📋 Prerequisites

- Azure Data Factory instance
- ADLS Gen2 storage account with Bronze/Silver/Gold containers
- Self-Hosted Integration Runtime installed on the on-prem machine
- Java Runtime Environment (JRE 8, 64-bit) installed on the SHIR machine (required for Parquet file support)
- SQL Server accessible from ADF
- `JAVA_HOME` environment variable set on the SHIR machine

---

## 🔧 Parameters Reference

### `files` Array Example
```json
[
  { "file_name": "DimPassenger.csv" },
  { "file_name": "DimFlight.csv" },
  { "file_name": "DimAirline.csv" }
]
```

### Mapping Parameter Example (`p_mapping_flight`)
```json
{
  "type": "TabularTranslator",
  "mappings": [
    { "source": { "name": "flight_id" }, "sink": { "name": "flight_id" } },
    { "source": { "name": "flight_number" }, "sink": { "name": "flight_number" } }
  ]
}
```

---

## 👤 Author

Developed as part of an end-to-end Azure Data Engineering project covering on-prem ingestion, API ingestion, incremental loading, and Medallion Architecture transformation using Azure Data Factory.
