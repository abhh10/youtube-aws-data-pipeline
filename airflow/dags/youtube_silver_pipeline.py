from datetime import datetime
from pathlib import Path
import re

import boto3

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


# ============================================================
# PROJECT PATHS
# ============================================================

# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[2]

SPARK_DIR = PROJECT_DIR / "scripts" / "spark"

SILVER_DB_DIR = PROJECT_DIR / "scripts" / "athena" / "SilverDB"

GOLD_DB_DIR = PROJECT_DIR / "scripts" / "athena" / "GoldDB"


# ============================================================
# SILVER SQL FILES
# ============================================================

SILVER_TABLE_SQL = (
    f"{SILVER_DB_DIR}/silverDBV2.sql"
)

SILVER_REPAIR_SQL = (
    f"{SILVER_DB_DIR}/REPAIR.sql"
)

SILVER_VIEW_SQL = (
    f"{SILVER_DB_DIR}/combined.sql"
)


# ============================================================
# GOLD SQL FILES
# ============================================================

GOLD_ENGAGEMENT_SQL = (
    f"{GOLD_DB_DIR}/gold_category_engagement.sql"
)

GOLD_PERFORMANCE_SQL = (
    f"{GOLD_DB_DIR}/gold_category_performance.sql"
)

GOLD_REGION_SQL = (
    f"{GOLD_DB_DIR}/gold_category_region.sql"
)

GOLD_RANKINGS_SQL = (
    f"{GOLD_DB_DIR}/gold_video_rankings.sql"
)


# ============================================================
# ATHENA CONFIG
# ============================================================

ATHENA_DATABASE = "yt-pipeline-silver-db"

ATHENA_OUTPUT = (
    "s3://abhiraj-yt-pipeline-athena-query/"
)

AWS_CONN_ID = "aws_default"


# ============================================================
# BRONZE CONFIG
# ============================================================

BRONZE_BUCKET = "abhiraj-yt-data-pipeline-bronze"

BRONZE_PREFIX = "youtube/raw_statistics/"


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="youtube_data_pipeline",
    start_date=datetime(2026, 8, 16),
    schedule=None,
    catchup=False,
)
def youtube_data_pipeline():

    # ========================================================
    # 0. DETECT LATEST BRONZE BATCH
    # ========================================================

    @task
    def detect_latest_batch():

        s3 = boto3.client("s3")

        paginator = s3.get_paginator("list_objects_v2")

        pattern = re.compile(
            r"youtube/raw_statistics/"
            r"region=[^/]+/"
            r"date=(\d{4}-\d{2}-\d{2})/"
            r"hour=(\d{2})/"
            r".+\.json$"
        )

        batches = set()

        for page in paginator.paginate(
            Bucket=BRONZE_BUCKET,
            Prefix=BRONZE_PREFIX,
        ):

            for obj in page.get("Contents", []):

                key = obj["Key"]

                match = pattern.match(key)

                if match:

                    ingestion_date = match.group(1)
                    ingestion_hour = match.group(2)

                    batches.add(
                        (
                            ingestion_date,
                            ingestion_hour,
                        )
                    )

        if not batches:
            raise ValueError(
                "No valid Bronze statistics batches found."
            )

        latest_date, latest_hour = max(batches)

        print(
            f"Latest Bronze batch detected: "
            f"{latest_date} / {latest_hour}"
        )

        return {
            "date": latest_date,
            "hour": latest_hour,
        }


    batch = detect_latest_batch()


    # ========================================================
    # 1. BRONZE → SILVER
    # ========================================================

    run_spark_bronze_to_silver = BashOperator(
        task_id="run_spark_bronze_to_silver",

        bash_command=f"""
        cd {SPARK_DIR}

        spark-submit \
          --packages org.apache.hadoop:hadoop-aws:3.4.2 \
          --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
          bronze_to_silver_json.py \
          {{{{ ti.xcom_pull(task_ids='detect_latest_batch')['date'] }}}} \
          {{{{ ti.xcom_pull(task_ids='detect_latest_batch')['hour'] }}}}
        """,
    )


    # ========================================================
    # 2. CREATE SILVER EXTERNAL TABLE
    # ========================================================

    create_silver_external_table = AthenaOperator(
        task_id="create_silver_external_table",

        query=Path(
            SILVER_TABLE_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    # ========================================================
    # 3. REPAIR SILVER PARTITIONS
    # ========================================================

    repair_silver_partitions = AthenaOperator(
        task_id="repair_silver_partitions",

        query=Path(
            SILVER_REPAIR_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    # ========================================================
    # 4. REFRESH SILVER VIEW
    # ========================================================

    refresh_all_video_statistics_view = AthenaOperator(
        task_id="refresh_all_video_statistics_view",

        query=Path(
            SILVER_VIEW_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    # ========================================================
    # 5. SILVER → GOLD
    # ========================================================

    run_spark_silver_to_gold = BashOperator(
        task_id="run_spark_silver_to_gold",

        bash_command=f"""
        cd {SPARK_DIR}

        spark-submit \
          --packages org.apache.hadoop:hadoop-aws:3.4.2 \
          --conf spark.hadoop.fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider \
          silver_to_gold.py \
          {{{{ ti.xcom_pull(task_ids='detect_latest_batch')['date'] }}}} \
          {{{{ ti.xcom_pull(task_ids='detect_latest_batch')['hour'] }}}}
        """,
    )


    # ========================================================
    # 6. CREATE / REGISTER GOLD TABLES
    # ========================================================

    create_gold_category_engagement = AthenaOperator(
        task_id="create_gold_category_engagement",

        query=Path(
            GOLD_ENGAGEMENT_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    create_gold_category_performance = AthenaOperator(
        task_id="create_gold_category_performance",

        query=Path(
            GOLD_PERFORMANCE_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    create_gold_category_region = AthenaOperator(
        task_id="create_gold_category_region",

        query=Path(
            GOLD_REGION_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    create_gold_video_rankings = AthenaOperator(
        task_id="create_gold_video_rankings",

        query=Path(
            GOLD_RANKINGS_SQL
        ).read_text(),

        database=ATHENA_DATABASE,

        output_location=ATHENA_OUTPUT,

        aws_conn_id=AWS_CONN_ID,
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    # --------------------------------------------------------
    # Detect batch → Bronze → Silver → Gold
    # --------------------------------------------------------

    batch >> run_spark_bronze_to_silver

    (
        run_spark_bronze_to_silver
        >> create_silver_external_table
        >> repair_silver_partitions
        >> refresh_all_video_statistics_view
        >> run_spark_silver_to_gold
    )


    # --------------------------------------------------------
    # Spark Gold → Athena Gold tables
    # --------------------------------------------------------

    run_spark_silver_to_gold >> [
        create_gold_category_engagement,
        create_gold_category_performance,
        create_gold_category_region,
        create_gold_video_rankings,
    ]


# ============================================================
# DAG REGISTRATION
# ============================================================

youtube_data_pipeline()