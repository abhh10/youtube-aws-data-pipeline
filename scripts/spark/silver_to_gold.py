import sys
import boto3

from pyspark.sql import SparkSession


# ============================================================
# Arguments
# ============================================================

if len(sys.argv) != 3:
    print("Usage:")
    print("spark-submit silver_to_gold.py <ingestion_date> <ingestion_hour>")
    sys.exit(1)

INGESTION_DATE = sys.argv[1]
INGESTION_HOUR = int(sys.argv[2])


# ============================================================
# S3 paths
# ============================================================

SILVER_STATS_PATH = (
    "s3a://abhiraj-yt-data-pipeline-silver/"
    "youtube/video_statistics_v2/"
)

SILVER_REFERENCE_PATH = (
    "s3a://abhiraj-yt-data-pipeline-silver/"
    "youtube/reference_data/"
)

GOLD_BASE = (
    "s3a://abhiraj-yt-data-pipeline-gold/"
)


# ============================================================
# S3 configuration
# ============================================================

GOLD_BUCKET = "abhiraj-yt-data-pipeline-gold"

GOLD_PATHS = {
    "category_engagement": "category_engagement/",
    "category_region": "category_region/",
    "category_performance": "category_performance/",
    "video_rankings": "video_rankings/",
}


# ============================================================
# Spark
# ============================================================

spark = (
    SparkSession.builder
    .appName("YouTube-Silver-To-Gold")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    )
    .config(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("SILVER → GOLD")
print("=" * 70)
print(f"Ingestion date : {INGESTION_DATE}")
print(f"Ingestion hour : {INGESTION_HOUR}")
print("=" * 70)


# ============================================================
# Read Silver
# ============================================================

stats = spark.read.parquet(SILVER_STATS_PATH)

reference = spark.read.parquet(SILVER_REFERENCE_PATH)


# ============================================================
# Create temporary views
# ============================================================

stats.createOrReplaceTempView("video_statistics")

reference.createOrReplaceTempView("clean_reference_data")


# ============================================================
# Identify affected keys from NEW Silver batch
# ============================================================

new_batch = spark.sql(f"""
    SELECT *
    FROM video_statistics
    WHERE ingestion_date = DATE('{INGESTION_DATE}')
      AND ingestion_hour = {INGESTION_HOUR}
""")

new_batch.createOrReplaceTempView("new_batch")


affected_categories_df = spark.sql("""
    SELECT DISTINCT category_id
    FROM new_batch
    WHERE category_id IS NOT NULL
""")

affected_category_regions_df = spark.sql("""
    SELECT DISTINCT
        region,
        category_id
    FROM new_batch
    WHERE region IS NOT NULL
      AND category_id IS NOT NULL
""")


affected_categories = [
    row["category_id"]
    for row in affected_categories_df.collect()
]

affected_category_regions = [
    (row["region"], row["category_id"])
    for row in affected_category_regions_df.collect()
]


print(f"New Silver rows: {new_batch.count()}")
print(f"Affected categories: {len(affected_categories)}")
print(f"Affected region/category pairs: {len(affected_category_regions)}")


if not affected_categories:
    print("No new categories found. Nothing to process.")
    spark.stop()
    sys.exit(0)


# ============================================================
# AWS S3 helper
# ============================================================

s3 = boto3.client("s3")


def delete_prefix(prefix):
    """
    Delete every object under an S3 prefix.
    Used to remove only the Gold partitions
    that are about to be rewritten.
    """

    paginator = s3.get_paginator("list_objects_v2")

    objects_to_delete = []

    for page in paginator.paginate(
        Bucket=GOLD_BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            objects_to_delete.append(
                {"Key": obj["Key"]}
            )

    if not objects_to_delete:
        return

    for i in range(0, len(objects_to_delete), 1000):

        batch = objects_to_delete[i:i + 1000]

        s3.delete_objects(
            Bucket=GOLD_BUCKET,
            Delete={
                "Objects": batch,
                "Quiet": True,
            },
        )

    print(f"Deleted: s3://{GOLD_BUCKET}/{prefix}")


# ============================================================
# 1. CATEGORY ENGAGEMENT
# ============================================================

print("\n" + "=" * 70)
print("1. CATEGORY ENGAGEMENT")
print("=" * 70)


# ------------------------------------------------------------
# Delete affected Gold partitions
# ------------------------------------------------------------

for category_id in affected_categories:

    delete_prefix(
        f"{GOLD_PATHS['category_engagement']}"
        f"category_id={category_id}/"
    )


# ------------------------------------------------------------
# Exact business logic from your V2 Athena query
# ------------------------------------------------------------

engagement = spark.sql("""
    WITH categories AS (

        SELECT
            category_id,
            category_name

        FROM (
            SELECT
                id AS category_id,
                snippet_title AS category_name,

                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY region
                ) AS rn

            FROM clean_reference_data
        )

        WHERE rn = 1
    ),

    affected AS (

        SELECT DISTINCT
            category_id
        FROM new_batch
    )

    SELECT
        c.category_id,
        c.category_name,

        SUM(s.views) AS total_views,
        SUM(s.likes) AS total_likes,
        SUM(s.comment_count) AS total_comments,

        ROUND(
            (SUM(s.likes) + SUM(s.comment_count)) * 100.0
            / NULLIF(SUM(s.views), 0),
            2
        ) AS engagement_rate

    FROM categories AS c

    INNER JOIN affected AS a
        ON c.category_id = a.category_id

    INNER JOIN video_statistics AS s
        ON c.category_id = CAST(s.category_id AS STRING)

    GROUP BY
        c.category_id,
        c.category_name
""")


engagement.write \
    .mode("overwrite") \
    .partitionBy("category_id") \
    .parquet(
        f"{GOLD_BASE}category_engagement/"
    )


print("Category engagement complete.")


# ============================================================
# 2. CATEGORY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("2. CATEGORY PERFORMANCE")
print("=" * 70)


# ------------------------------------------------------------
# Delete affected partitions
# ------------------------------------------------------------

for category_id in affected_categories:

    delete_prefix(
        f"{GOLD_PATHS['category_performance']}"
        f"category_id={category_id}/"
    )


# ------------------------------------------------------------
# Exact V2 business logic
# ------------------------------------------------------------

performance = spark.sql("""
    WITH categories AS (

        SELECT
            category_id,
            category_name

        FROM (
            SELECT
                id AS category_id,
                snippet_title AS category_name,

                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY region
                ) AS rn

            FROM clean_reference_data
        )

        WHERE rn = 1
    ),

    latest_videos AS (

        SELECT
            video_id,
            title,
            category_id,
            views,
            likes,
            comment_count,
            region

        FROM (
            SELECT
                video_id,
                title,
                category_id,
                views,
                likes,
                comment_count,
                region,

                ROW_NUMBER() OVER (
                    PARTITION BY video_id, region
                    ORDER BY
                        ingestion_date DESC,
                        ingestion_hour DESC
                ) AS rn

            FROM video_statistics
        )

        WHERE rn = 1
    ),

    affected AS (

        SELECT DISTINCT
            category_id
        FROM new_batch
    )

    SELECT

        c.category_id,
        c.category_name,

        COUNT(v.video_id) AS video_count,

        SUM(v.views) AS total_views,
        AVG(v.views) AS avg_views,

        SUM(v.likes) AS total_likes,
        AVG(v.likes) AS avg_likes,

        SUM(v.comment_count) AS total_comments,
        AVG(v.comment_count) AS avg_comments

    FROM categories AS c

    INNER JOIN affected AS a
        ON c.category_id = a.category_id

    INNER JOIN latest_videos AS v
        ON c.category_id = CAST(v.category_id AS STRING)

    GROUP BY
        c.category_id,
        c.category_name
""")


performance.write \
    .mode("overwrite") \
    .partitionBy("category_id") \
    .parquet(
        f"{GOLD_BASE}category_performance/"
    )


print("Category performance complete.")


# ============================================================
# 3. CATEGORY REGION
# ============================================================

print("\n" + "=" * 70)
print("3. CATEGORY REGION")
print("=" * 70)


# ------------------------------------------------------------
# Delete affected region/category partitions
# ------------------------------------------------------------

for region, category_id in affected_category_regions:

    delete_prefix(
        f"{GOLD_PATHS['category_region']}"
        f"region={region}/"
        f"category_id={category_id}/"
    )


# ------------------------------------------------------------
# Exact V2 business logic
# ------------------------------------------------------------

category_region = spark.sql("""
    WITH categories AS (

        SELECT
            category_id,
            category_name

        FROM (
            SELECT
                id AS category_id,
                snippet_title AS category_name,

                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY region
                ) AS rn

            FROM clean_reference_data
        )

        WHERE rn = 1
    ),

    latest_videos AS (

        SELECT
            video_id,
            category_id,
            views,
            likes,
            comment_count,
            region

        FROM (
            SELECT
                video_id,
                category_id,
                views,
                likes,
                comment_count,
                region,

                ROW_NUMBER() OVER (
                    PARTITION BY video_id, region
                    ORDER BY
                        ingestion_date DESC,
                        ingestion_hour DESC
                ) AS rn

            FROM video_statistics
        )

        WHERE rn = 1
    ),

    affected AS (

        SELECT DISTINCT
            region,
            category_id
        FROM new_batch
    )

    SELECT

        v.region,
        v.category_id,
        c.category_name,

        COUNT(v.video_id) AS video_count,

        SUM(v.views) AS total_views,
        AVG(v.views) AS avg_views,

        SUM(v.likes) AS total_likes,
        SUM(v.comment_count) AS total_comments

    FROM latest_videos AS v

    INNER JOIN affected AS a
        ON v.region = a.region
       AND v.category_id = a.category_id

    INNER JOIN categories AS c
        ON c.category_id = CAST(v.category_id AS STRING)

    GROUP BY
        v.region,
        v.category_id,
        c.category_name
""")


category_region.write \
    .mode("overwrite") \
    .partitionBy("region", "category_id") \
    .parquet(
        f"{GOLD_BASE}category_region/"
    )


print("Category region complete.")


# ============================================================
# 4. VIDEO RANKINGS
# ============================================================

print("\n" + "=" * 70)
print("4. VIDEO RANKINGS")
print("=" * 70)


# ------------------------------------------------------------
# Delete affected category partitions
# ------------------------------------------------------------

for category_id in affected_categories:

    delete_prefix(
        f"{GOLD_PATHS['video_rankings']}"
        f"category_id={category_id}/"
    )


# ------------------------------------------------------------
# Exact V2 business logic
# ------------------------------------------------------------

rankings = spark.sql("""
    WITH categories AS (

        SELECT
            category_id,
            category_name

        FROM (
            SELECT
                id AS category_id,
                snippet_title AS category_name,

                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY region
                ) AS rn

            FROM clean_reference_data
        )

        WHERE rn = 1
    ),

    latest_videos AS (

        SELECT
            video_id,
            title,
            category_id,
            views,
            region

        FROM (
            SELECT
                video_id,
                title,
                category_id,
                views,
                region,

                ROW_NUMBER() OVER (
                    PARTITION BY video_id, region
                    ORDER BY
                        ingestion_date DESC,
                        ingestion_hour DESC
                ) AS rn

            FROM video_statistics
        )

        WHERE rn = 1
    ),

    affected AS (

        SELECT DISTINCT
            category_id
        FROM new_batch
    ),

    ranked AS (

        SELECT
            v.video_id,
            v.title,
            v.category_id,
            c.category_name,
            v.region,
            v.views,

            ROW_NUMBER() OVER (
                PARTITION BY v.category_id
                ORDER BY v.views DESC
            ) AS video_rank

        FROM latest_videos AS v

        INNER JOIN affected AS a
            ON v.category_id = a.category_id

        INNER JOIN categories AS c
            ON c.category_id = CAST(v.category_id AS STRING)
    )

    SELECT *
    FROM ranked
    WHERE video_rank <= 3
""")


rankings.write \
    .mode("overwrite") \
    .partitionBy("category_id") \
    .parquet(
        f"{GOLD_BASE}video_rankings/"
    )


print("Video rankings complete.")


# ============================================================
# SUCCESS
# ============================================================

print("\n")
print("=" * 70)
print("SUCCESS")
print("=" * 70)
print("Incremental Gold processing completed.")
print(f"Batch: {INGESTION_DATE} / {INGESTION_HOUR}")
print("=" * 70)


spark.stop()