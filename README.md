## Real-Time Sales Data Pipeline

## Overview:

*This project implements an end-to-end real-time data pipeline using Kafka, PySpark Structured Streaming, and Delta Lake on Databricks.*

<It processes streaming data through a Medallion Architecture (Bronze → Silver → Gold) while handling:>

## Real-time ingestion

*CDC (Change Data Capture) events*
*SCD Type 2 historical tracking*
*Window-based aggregations for analytics*

<Kafka → Bronze → Silver → Gold>
              <↓>
        <SCD Type 2 Dimension>

## Pipeline Breakdown

**1. Ingestion (Kafka → Bronze)**

--> Data is produced to Kafka topics
--> Streaming jobs ingest data into Bronze Delta tables
--> Data is stored in raw form for replay and fault tolerance

**2. Transformation (Bronze → Silver)**

--> JSON parsing and schema enforcement
--> Data type casting and cleaning
--> Derived columns (date, hour, etc.)
--> Deduplication using window functions

**3. CDC Handling**

--> Processes INSERT / UPDATE / DELETE events
--> Collapses multiple events → latest state per key per batch
--> Ensures only one valid row per business key before      downstream
--> operations

**4. SCD Type 2 (Historical Table)**

<Maintains dim_employees_historical with:>

--> Surrogate key (auto-generated)
--> start_date, end_date
--> is_current, is_deleted

*Logic:*

--> New records → INSERT
--> Updates → expire old record + insert new version
--> Deletes → mark record inactive

<Implemented using MERGE inside foreachBatch for incremental updates.>

**5. Aggregations (Silver → Gold)**

--> Time-based window aggregations
--> Business metrics (counts, totals, KPIs)
--> Stored as Delta tables optimized for querying and dashboards

## Key Features

--> Structured Streaming with checkpointing
--> Watermarking for late data handling
--> Custom batch logic using foreachBatch
--> Delta Lake ACID transactions
--> Idempotent processing design

## Tech Stack

--> Apache Kafka
--> Apache Spark (Structured Streaming)
--> Delta Lake
--> Databricks

## Design Considerations

--> Streaming sources are treated as append-only
--> Deduplication is required before MERGE to avoid conflicts
--> Checkpoints must be reset if source tables are recreated
--> Gold layer should be idempotent to prevent duplicate outputs

## Execution Flow

--> Start Kafka producer (data generation)
--> Run Bronze ingestion stream
--> Run Silver transformation stream
--> Run SCD and Gold aggregation jobs
--> Monitor via checkpoints and logs