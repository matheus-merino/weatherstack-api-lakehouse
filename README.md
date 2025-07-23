# End-to-End Weather Data Lakehouse on Azure Databricks

## 1. Introduction

This project demonstrates a complete, end-to-end data engineering pipeline built on the Azure cloud and Databricks platform. The primary objective is to ingest daily weather data from an external API for three distinct cities, process this data through a medallion architecture, and serve it in a clean format for business intelligence and analytics.

The core technologies and concepts showcased in this project include:
- **Cloud Infrastructure:** Microsoft Azure (Storage Account, Key Vault, Service Principal).
- **Data Platform:** Azure Databricks.
- **Data Lakehouse:** Using Delta Lake as the storage format.
- **Architecture:** Medallion Architecture (Bronze, Silver, Gold).
- **Data Modeling:** Dimensional modeling principles (Fact and Dimension tables) in the Silver layer.
- **ETL/ELT:** Spark for data ingestion and transformation.
- **Data Serving:** Power BI for final dashboarding and visualization.

The analytical goal is to perform a comparative analysis of weather patterns across three selected cities: **Gramado (Brazil)**, **Punta del Este (Uruguay)**, and **Punta Arenas (Chile)**.

---

## 2. Architecture

This project is built upon the **Medallion Architecture**, a standard design pattern that ensures data quality, reliability, and scalability by logically organizing data into three distinct layers.

### Medallion Architecture

-   **Bronze Layer:** This is the first stop for our raw data. Data is ingested from the source and stored in its original, unaltered format (a raw JSON string per record). This layer serves as a historical archive and a secure source for re-processing, ensuring we never lose the original data.
-   **Silver Layer:** Data from the Bronze layer is cleaned, validated, and conformed into a structured, queryable format. Here, we apply some data modeling best practices. This layer serves as the enterprise "single source of truth".
-   **Gold Layer:** This is the final presentation layer, optimized for specific business use cases and analytics. Data from the Silver layer is aggregated, joined, and shaped into denormalized tables that directly feed into BI dashboards and reports.

### Data Lake Structure

The data is physically organized in Azure Data Lake Storage Gen2 with the following folder structure, reflecting the Medallion layers:

```
/datalake/
├── landing/
│   └── weather/
│       └── YYYY-MM-DD/
│           └── gramado_brazil_timestamp.json
│           └── punta_arenas_chile_timestamp.json
│           └── punta_del_este_uruguay_timestamp.json
├── bronze/
│   └── weather/
│       └── ... (Delta Lake files)
├── silver/
│   ├── weather_air_quality_cleaned/
│   ├── weather_dim_location/
│   └── weather_fct_cleaned/
└── gold/
│   ├── weather_7days_moving_avgs/
│   ├── weather_aggregates_by_city/
│   ├── weather_current_snapshot/
│   └── weather_events_summary/
```

### Databricks Catalog

The physical data in the lake is cataloged in the Hive Metastore for easy querying. A logical schema `weather` is used to group all project tables:

-   **Bronze Table:**
    -   `bronze_weather_raw`: Raw json data provided by the API, along with metadata columns.
-   **Silver Tables:**
    -   `silver_air_quality_cleaned`: A fact table containing time-series air-quality measurements.
    -   `silver_dim_location`: A dimension table holding descriptive, static attributes of each city.
    -   `silver_weather_cleaned`: A fact table containing time-series weather measurements.
-   **Gold Tables:**
    -   `gold_7days_moving_avgs`: A 7-days moving average table informing daily measure averages (temperature, humidity, wind speed, precipitation) by each city.
    -   `gold_current_weather_snapshot`: A non-aggregated table showing the latest weather conditions for each city, powering the "live" section of the dashboard.
    -   `gold_aggregates_by_city`: A table with aggregated metrics (avg, min, max) for overall city comparisons.
    -   `gold_weather_events_summary`: A table that counts significant weather events (e.g., hot days, rainy days) for comparative analysis.

---

## 3. Azure Infrastructure & Setup

To run this project, the following Azure resources must be provisioned:

### Required Resources
1.  **Azure Subscription**
2.  **Resource Group**
3.  **Storage Account** (with ADLS Gen2 enabled)
4.  **Azure Key Vault** (to securely store secrets)
5.  **Azure Databricks Workspace**
6.  **Service Principal** (for secure access between Databricks and the Storage Account)

### Setup Steps
1.  Provision all the resources listed above within the same Resource Group.
2.  Create a Service Principal in Azure Active Directory.
3.  [Grant the Service Principal the **"Storage Blob Data Contributor"**](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/aad-storage-service-principal) role on the Storage Account.
4.  Create a secret in Azure Key Vault to store the Service Principal's client secret. Also, store the Weatherstack API key here.
5.  In Databricks, [create a **Secret Scope** backed by the Azure Key Vault](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/#secrets) to allow notebooks to securely access the secrets.
6.  [Mount the ADLS Gen2 container to the Databricks File System (DBFS)](https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts) at `/mnt/data` using the Service Principal for authentication. This is done once in the initial setup notebook (`00-Setup / mount-data-lake`).

---

## 4. About Weatherstack API

The primary data source for this project is the [Weatherstack API](https://weatherstack.com/). The design of this data pipeline was heavily influenced by the capabilities and constraints of the API plan used for development.

Initially, the strategy was to build a rich, time-series dataset by using the `/historical` API endpoint. This would allow a single, efficient batch job to run once per day to fetch the complete, 24-hour dataset for the previous day.

However, this project is developed under the constraints of the **Weatherstack Free Tier**, which imposes two limitations:

1.  **Endpoint Access:** Access is restricted to the `/current` endpoint only. The `/historical` and time-series endpoints are not available.
2.  **Rate Limiting:** A strict limit of **100 API calls per month** is enforced.

These constraints made the ideal strategy unfeasible. A frequent-calling strategy (e.g., hourly) would quickly exhaust the monthly call limit. Furthermore, running the Databricks job multiple times a day would significantly increase cloud infrastructure costs.

Given these limitations, the project's data collection goal was pivoted. Instead of building a high-frequency time-series, the pipeline is designed to create a **Daily Comparative Snapshot**.

-   **Execution Frequency:** The entire pipeline runs **once per day**.
-   **Cost & API Efficiency:** This approach consumes approximately 90 API calls per month (`3 cities * 30 days`), staying within the free tier limit, and minimizes Databricks costs.
-   **Data Consistency:** The job is scheduled for a fixed, strategic time each day (e.g., 14:00 local time). This ensures that each data point represents a consistent "mid-afternoon" snapshot, making daily comparisons meaningful.

This engineering decision allows the project to deliver valuable comparative analytics while operating entirely within the technical and financial constraints of the available free-tier resources.

---

## 5. Data Serving & Analytics

The Gold layer tables are the final, curated datasets designed for direct consumption by analytics tools. They are aggregated to answer specific business questions and to provide optimal performance for reporting queries.

This project culminates in an interactive Power BI dashboard that connects directly to the Gold tables in Databricks. The dashboard provides three distinct views:
1.  A "live" snapshot of current conditions.
2.  A comparative summary of significant weather events.
3.  An analisys of historical weather measures by city with moving averages.
4.  A comparative analysis of overall weather measures.

**[Access the Live Power BI Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNGU1YTUxYzUtYjZlMS00YWFjLTkxYjktYmNkNTA4NGM4YWM5IiwidCI6IjgwZGExZmNhLTBlNTktNDFmYS04ZTQ3LWY0ZmFhM2UxMzI0ZCIsImMiOjR9)**