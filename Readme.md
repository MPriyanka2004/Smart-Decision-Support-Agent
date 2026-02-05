# Smart CPG Decision Support Agent 

## Overview

This project implements a **Smart Decision Support Agent** for a **Consumer Packaged Goods (CPG)** business using **Azure Databricks**.
It ingests multi-store, multi-SKU sales data, performs **Exploratory Data Analysis (EDA)**, builds **Bronze–Silver–Gold data layers**, simulates business scenarios, and provides **natural-language insights via a CLI-based agent**.

## Objectives

* Ingest historical CPG sales data
* Perform EDA to understand data quality and business patterns
* Build Silver and Gold analytical layers
* Simulate “what-if” business scenarios (price hikes, promotions)
* Enable NLP-based decision support using an agent loop
* Validate the pipeline using notebook-based tests

## Architecture

```
CSV Data
  ↓
Bronze Table (cpg_sales_table)
  ↓
EDA (read-only, exploratory)
  ↓
Silver Table (cpg_sales_silver)
  ↓
Gold Tables (aggregated insights)
  ↓
Scenario Simulation
  ↓
Agent + CLI (NLP responses)
```

## Project Structure (Databricks Workspace)

```
Capstone - 1/
│
├── 01_Data_Loading_and_Silver_Gold
├── 02_Trend_Anomaly_Detection
├── 03_Scenario_Simulation
├── 04_Agent_Loop_Prototype
├── 05_CLI
│
├── utils_agents
├── utils_simulation
│
├── tests/
│   ├── test_data_loader
│   ├── test_tools
│   └── test_agent
│
└── cpg_sales_sample_5000.csv
```

> Utility logic and tests are implemented as **Databricks notebooks** and reused using `%run`, which is a standard Databricks practice.

## Notebook 

### 01_Data_Loading_and_Silver_Gold

**Purpose:** Ingestion + Exploratory Data Analysis + Silver layer

* Loads CSV data and creates the **Bronze table**
* Performs **EDA on Bronze data**:

  * Data volume and granularity
  * Date range and coverage
  * Missing value checks
  * Revenue and units sold distributions
  * Region, category, store-size analysis
  * Promotion and holiday impact
* Documents EDA insights using markdown
* Creates the **Silver table** with cleaned and enriched fields

> EDA results are exploratory and **not persisted as tables**.

### 02_Trend_Anomaly_Detection

**Purpose:** Analytics and Gold layer creation

* Builds Gold tables from Silver:

  * Monthly sales trends
  * Promotion impact analysis
  * Region and category performance
* Validates seasonality and trend behavior

### 03_Scenario_Simulation

**Purpose:** What-if business simulations

* Simulates scenarios such as:

  * Price hike with demand drop
* Uses Silver data
* Focuses only on business impact modeling

### 04_Agent_Loop_Prototype

**Purpose:** Agentic AI logic

* Implements intent classification
* Converts analytical results into **natural-language insights**
* Acts as the core decision-support engine

### 05_CLI

**Purpose:** User interaction

* Provides a CLI-style interface inside Databricks
* Accepts business questions
* Displays NLP responses (not raw tables)

## Utility Notebooks

### utils_agents

* Intent classification
* NLP response generation
* Agent orchestration logic

### utils_simulation

* Business scenario simulations
* Price hike and demand impact logic

These notebooks are reused via:

```python
%run ./utils_agents
%run ./utils_simulation
```

## Data Layers

### Bronze

* Raw ingested data
* Table: `cpg_sales_table`

### Silver

* Cleaned and enriched data
* Derived fields:

  * `year`, `month`
  * `is_promo`
  * `revenue_per_unit`
* Table: `cpg_sales_silver`

### Gold

* Business-ready aggregated insights:

  * `cpg_sales_gold_monthly`
  * `cpg_sales_gold_promo`
  * `cpg_sales_gold_region_category`

## Testing

Testing is implemented using **Databricks notebooks** under the `tests` folder.

### Test Coverage

* Data pipeline validation (Bronze, Silver, Gold tables)
* Scenario simulation correctness
* Agent NLP response validation

Tests use `assert` statements and fail only if expectations are not met.
