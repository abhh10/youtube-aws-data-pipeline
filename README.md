# YouTube AWS Data Engineering Pipeline

An end-to-end data engineering pipeline that ingests YouTube Data API data into AWS S3, processes it through Bronze, Silver, and Gold layers using PySpark, catalogs and queries the data with AWS Glue and Athena, and orchestrates the workflow with Apache Airflow.

---

## Architecture

```text
                    YouTube Data API
                           │
                           ▼
                    AWS Lambda
                 Data Ingestion
                           │
                           ▼
                  ┌─────────────────┐
                  │   S3 BRONZE     │
                  │                 │
                  │ Raw JSON        │
                  │ Historical data │
                  └────────┬────────┘
                           │
                           ▼
                    Apache Spark
                  Bronze → Silver
                           │
                           ▼
                  ┌─────────────────┐
                  │   S3 SILVER     │
                  │                 │
                  │ Parquet         │
                  │ Partitioned     │
                  │ Cleaned data    │
                  └────────┬────────┘
                           │
                           ▼
                    AWS Glue
                  Data Catalog
                           │
                           ▼
                     Amazon Athena
                           │
                           ▼
                    Apache Spark
                  Silver → Gold
                           │
                           ▼
                  ┌─────────────────┐
                  │    S3 GOLD      │
                  │                 │
                  │ Aggregations    │
                  │ Rankings        │
                  │ Analytics-ready │
                  └────────┬────────┘
                           │
                           ▼
                     Amazon Athena


                  Apache Airflow
                 ─────────────────
                 Orchestration
                       │
       ┌───────────────┼────────────────┐
       ▼               ▼                ▼
   Spark Jobs      Athena DDL       Partition
                                    Management
```

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline and Lambda development |
| PySpark | Data transformation |
| AWS S3 | Data lake storage |
| AWS Lambda | Serverless ingestion and processing |
| AWS Glue | Data catalog |
| Amazon Athena | Serverless SQL analytics |
| Apache Airflow | Workflow orchestration |
| Parquet | Columnar storage format |
| Boto3 | AWS SDK for Python |
| Git | Version control |

---

## Data Lake Design

The pipeline follows a layered Bronze, Silver, and Gold architecture.

### Bronze Layer

The Bronze layer stores raw data received from the YouTube Data API.

**S3 Bucket:**

```text
s3://abhiraj-yt-data-pipeline-bronze/
```

Raw statistics are organized using region, date, and hour partitions.

Example:

```text
youtube/raw_statistics/
└── region=in/
    └── date=2026-08-17/
        └── hour=15/
            └── 20260817_....json
```

The Bronze layer is intentionally close to the source data so that historical data can be retained and reprocessed when necessary.

---

### Silver Layer

The Silver layer contains cleaned and structured Parquet data.

**S3 Bucket:**

```text
s3://abhiraj-yt-data-pipeline-silver/
```

The main statistics dataset is partitioned by:

```text
region
ingestion_date
ingestion_hour
```

Example:

```text
youtube/statistics/
└── region=in/
    └── ingestion_date=2026-08-17/
        └── ingestion_hour=15/
            └── part-....parquet
```

The Silver layer provides a consistent schema for downstream analytical processing.

---

### Gold Layer

The Gold layer contains analytics-ready datasets.

**S3 Bucket:**

```text
s3://abhiraj-yt-data-pipeline-gold/
```

Current Gold datasets include:

- `gold_category_engagement`
- `gold_category_performance`
- `gold_category_region`
- `gold_video_rankings`

These datasets contain aggregations, rankings, and analytical transformations designed for downstream querying.

---

## Incremental Processing

The pipeline is designed to process new ingestion batches without rebuilding the entire historical dataset.

New batches are identified using:

```text
ingestion_date
ingestion_hour
```

For example:

```text
2026-08-17 / 15
```

The Bronze → Silver Spark job processes the incoming data and writes the corresponding Silver partitions.

The Silver → Gold Spark job then processes the relevant data and updates the affected Gold datasets.

This allows historical data to remain available while new data is incrementally processed.

---

## Airflow Orchestration

Apache Airflow orchestrates the end-to-end pipeline.

The main DAG is:

```text
youtube_data_pipeline
```

The workflow is approximately:

```text
Bronze → Silver Spark
        │
        ▼
Create / refresh Silver Athena metadata
        │
        ▼
Repair Silver partitions
        │
        ▼
Refresh Silver views
        │
        ▼
Silver → Gold Spark
        │
        ▼
Create Gold Athena tables
        │
        ▼
Repair Gold partitions where applicable
```

The DAG uses:

- `BashOperator` for Spark jobs
- `AthenaOperator` for Athena operations
- `aws_default` Airflow connection for AWS access

The DAG determines the project directory dynamically:

```python
PROJECT_DIR = Path(__file__).resolve().parents[2]
```

This avoids hard-coding the local project path inside the DAG.

---

## Spark Jobs

### Bronze → Silver

```text
scripts/spark/bronze_to_silver_json.py
```

Responsible for:

1. Reading raw JSON data from the Bronze S3 bucket.
2. Parsing and transforming the source data.
3. Standardizing the schema.
4. Writing structured Parquet data to Silver.
5. Partitioning the output by region, ingestion date, and ingestion hour.

An earlier Bronze → Silver implementation is also retained:

```text
scripts/spark/bronze_to_silver.py
```

### Silver → Gold

```text
scripts/spark/silver_to_gold.py
```

Responsible for:

1. Reading structured Silver data.
2. Performing analytical transformations.
3. Creating Gold datasets.
4. Writing analytics-ready Parquet data to S3.
5. Processing new batches incrementally.

Current Gold outputs include:

```text
gold_category_engagement
gold_category_performance
gold_category_region
gold_video_rankings
```

---

## AWS Lambda Functions

### YouTube API Ingestion

```text
scripts/lamdba/yt-api-data-injestion/lambda_function.py
```

The Lambda function:

1. Calls the YouTube Data API.
2. Retrieves trending video data for configured regions.
3. Writes raw JSON responses to the Bronze S3 bucket.

The YouTube API key is supplied through an environment variable:

```text
YOUTUBE_API_KEY
```

No API credentials are hard-coded in the source code.

---

### Reference Data Processing

```text
scripts/lamdba/json_to_parquet/
```

The reference-data Lambda processes YouTube category/reference JSON data and writes the resulting structured data to the Silver layer.

The implementation includes validation, deduplication, partition handling, and structured logging.

---

## AWS Glue and Athena

AWS Glue Data Catalog is used to maintain metadata for the S3 datasets.

Amazon Athena is used to query the Parquet data directly from S3.

The Athena SQL files are organized under:

```text
scripts/athena/
```

### Bronze SQL

```text
scripts/athena/BronzeDB/
```

Contains Bronze database-related SQL.

### Silver SQL

```text
scripts/athena/SilverDB/
```

Contains:

- Silver table definitions
- Silver views
- Partition management SQL

### Gold SQL

```text
scripts/athena/GoldDB/
```

Contains:

- Gold table definitions
- Gold partition management SQL

---

## Repository Structure

```text
YT-AWS-Pipeline/
│
├── airflow/
│   └── dags/
│       └── youtube_silver_pipeline.py
│
├── scripts/
│   │
│   ├── athena/
│   │   ├── BronzeDB/
│   │   ├── SilverDB/
│   │   └── GoldDB/
│   │
│   ├── lamdba/
│   │   ├── json_to_parquet/
│   │   └── yt-api-data-injestion/
│   │
│   └── spark/
│       ├── bronze_to_silver.py
│       ├── bronze_to_silver_json.py
│       └── silver_to_gold.py
│
├── .gitignore
├── info.md
├── requirements.txt
└── README.md
```

---

## S3 Buckets

| Layer | S3 Bucket |
|---|---|
| Bronze | `abhiraj-yt-data-pipeline-bronze` |
| Silver | `abhiraj-yt-data-pipeline-silver` |
| Gold | `abhiraj-yt-data-pipeline-gold` |
| Scripts | `abhiraj-yt-data-pipeline-scripts` |

---

## Running Locally

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd YT-AWS-Pipeline
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
```

### 3. Activate the environment

```bash
source .venv/bin/activate
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

Main project dependencies include:

```text
Apache Airflow
Apache Airflow Amazon Provider
Apache Airflow Standard Provider
PySpark
Boto3
Python-dotenv
```

---

## AWS Credentials

AWS credentials should be configured outside the repository.

For example:

```bash
aws configure
```

The pipeline uses AWS credentials through the local AWS credential/profile mechanism.

Credentials and secrets are intentionally excluded from Git.

---

## Running Spark Locally

### Bronze → Silver

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
  scripts/spark/bronze_to_silver_json.py
```

### Silver → Gold

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
  scripts/spark/silver_to_gold.py
```

The Spark jobs can also be executed through the Airflow DAG.

---

## Airflow Setup

Set the project-specific Airflow home:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
```

Initialize the Airflow metadata database if required:

```bash
airflow db migrate
```

Start Airflow:

```bash
airflow standalone
```

The DAG can then be triggered manually from the Airflow UI.

The pipeline currently uses manual triggering during development and testing rather than relying on a fixed schedule.

---

## Pipeline Execution

A typical execution looks like:

```text
1. YouTube API
       │
       ▼
2. Lambda ingestion
       │
       ▼
3. S3 Bronze
       │
       ▼
4. Spark Bronze → Silver
       │
       ▼
5. S3 Silver
       │
       ▼
6. Glue / Athena metadata
       │
       ▼
7. Spark Silver → Gold
       │
       ▼
8. S3 Gold
       │
       ▼
9. Athena analytical queries
```

Airflow coordinates the dependencies between these stages.

---

## Design Decisions

### Why S3?

Amazon S3 provides durable, scalable, and cost-effective object storage for the data lake.

It also allows the Bronze, Silver, and Gold datasets to remain separated while using the same underlying storage architecture.

### Why Parquet?

Parquet was selected because it provides:

- Columnar storage
- Compression
- Efficient analytical reads
- Efficient Spark processing
- Efficient Athena queries
- Native support for partitioned datasets

### Why Spark?

PySpark is used for the main data transformations between the Bronze, Silver, and Gold layers.

The transformation logic can be moved from local execution to a distributed Spark environment without fundamentally changing the data-processing architecture.

### Why Athena?

Athena provides serverless SQL access to the Parquet datasets stored in S3 without requiring a dedicated analytical database.

### Why Airflow?

Airflow is responsible for orchestration rather than performing the transformations itself.

It manages task dependencies such as:

```text
Spark → Athena → Spark → Athena
```

and provides visibility into task execution and failures.

---

## Key Data Engineering Concepts Demonstrated

This project demonstrates:

- Data lake architecture
- Medallion architecture
- AWS S3
- Serverless ingestion
- AWS Lambda
- YouTube Data API ingestion
- PySpark transformations
- Parquet
- Partitioned datasets
- Incremental processing
- AWS Glue Data Catalog
- Amazon Athena
- External tables
- SQL transformations
- Airflow DAGs
- Workflow orchestration
- AWS SDK / Boto3
- Separation of ingestion, transformation, and analytical layers

---

## Current Pipeline Status

The pipeline currently supports:

- YouTube Data API ingestion
- Raw Bronze storage
- Bronze → Silver transformation
- Silver Parquet partitioning
- Silver Athena tables
- Silver views
- Silver → Gold transformation
- Incremental Gold processing
- Gold Athena tables
- Airflow orchestration
- AWS S3 data lake architecture
- Manual end-to-end pipeline execution
- Dynamic project path detection in Airflow

---

## Future Improvements

Potential production improvements include:

- Automated Airflow scheduling
- Fully automated batch detection
- AWS-managed Spark execution
- Data quality checks
- Pipeline monitoring and alerting
- CI/CD
- Infrastructure as Code using Terraform
- Centralized configuration
- Improved retry and failure handling
- Cloud-based Airflow deployment
- Automated testing

---

## Author

**Abhiraj Karpe**

Built as a hands-on AWS Data Engineering project to explore data lake architecture, serverless ingestion, distributed data processing, analytical querying, incremental processing, and workflow orchestration.
