from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    regexp_extract,
    input_file_name,
    trim
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("YT-Bronze-to-Silver")
    .getOrCreate()
)


# ============================================================
# S3 PATHS
# ============================================================

INGESTION_DATE = "2026-08-15"

BRONZE_PATH = (
    f"s3a://abhiraj-yt-data-pipeline-bronze/"
    f"youtube/raw_statistics/region=*/date={INGESTION_DATE}/*.csv"
)

SILVER_PATH = (
    "s3a://abhiraj-yt-data-pipeline-silver/"
    "youtube/video_statistics/"
)


# ============================================================
# READ BRONZE
# ============================================================

print("\nReading Bronze data...")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("pathGlobFilter", "*.csv")
    .option("recursiveFileLookup", "true")
    .csv(BRONZE_PATH)
)

print("\nBronze schema:")
df.printSchema()

bronze_count = df.count()

print(f"\nBronze rows read: {bronze_count}")


# ============================================================
# BRONZE → SILVER
# ============================================================

print("\nTransforming Bronze → Silver...")


silver_df = df.select(

    # --------------------------------------------------------
    # IDENTIFIERS
    # --------------------------------------------------------

    trim(col("video_id")).alias("video_id"),

    col("title"),

    col("channel_title"),


    # --------------------------------------------------------
    # TRENDING DATE
    #
    # Example:
    # 17.14.11 → 2017-11-14
    #
    # try_to_date is used through SQL expression so malformed
    # values become NULL instead of killing the job.
    # --------------------------------------------------------

    expr("""
        try_to_date(
            trim(trending_date),
            'yy.dd.MM'
        )
    """).alias("trending_date"),


    # --------------------------------------------------------
    # CATEGORY ID
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(category_id) AS INT
        )
    """).alias("category_id"),


    # --------------------------------------------------------
    # PUBLISH TIME
    #
    # IMPORTANT:
    # Use expr() instead of the Python try_to_timestamp()
    # because Spark 4.1.1 was interpreting the format string
    # incorrectly in the previous version.
    # --------------------------------------------------------

    expr("""
        try_to_timestamp(
            trim(publish_time),
            'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''
        )
    """).alias("publish_time"),


    # --------------------------------------------------------
    # TEXT
    # --------------------------------------------------------

    col("tags"),

    col("thumbnail_link"),

    col("description"),


    # --------------------------------------------------------
    # NUMERIC METRICS
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(views) AS BIGINT
        )
    """).alias("views"),

    expr("""
        try_cast(
            trim(likes) AS BIGINT
        )
    """).alias("likes"),

    expr("""
        try_cast(
            trim(dislikes) AS BIGINT
        )
    """).alias("dislikes"),

    expr("""
        try_cast(
            trim(comment_count) AS BIGINT
        )
    """).alias("comment_count"),


    # --------------------------------------------------------
    # BOOLEAN FLAGS
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(comments_disabled) AS BOOLEAN
        )
    """).alias("comments_disabled"),

    expr("""
        try_cast(
            trim(ratings_disabled) AS BOOLEAN
        )
    """).alias("ratings_disabled"),

    expr("""
        try_cast(
            trim(video_error_or_removed) AS BOOLEAN
        )
    """).alias("video_error_or_removed"),


    # --------------------------------------------------------
    # SOURCE FILE
    # --------------------------------------------------------

    input_file_name().alias("_source_file")
)


# ============================================================
# EXTRACT REGION
# ============================================================

silver_df = silver_df.withColumn(
    "region",
    regexp_extract(
        col("_source_file"),
        r"region=([^/]+)",
        1
    )
)


# ============================================================
# REMOVE INVALID VIDEO IDs
# ============================================================

print("\nRemoving records without video_id...")

before_id_filter = silver_df.count()

silver_df = silver_df.filter(
    col("video_id").isNotNull()
    & (trim(col("video_id")) != "")
)

after_id_filter = silver_df.count()

print(
    "Rows removed:",
    before_id_filter - after_id_filter
)


# ============================================================
# REMOVE INVALID REGIONS
# ============================================================

print("\nChecking regions...")

before_region_filter = silver_df.count()

silver_df = silver_df.filter(
    col("region").isNotNull()
    & (trim(col("region")) != "")
)

after_region_filter = silver_df.count()

print(
    "Rows removed:",
    before_region_filter - after_region_filter
)


# ============================================================
# DEDUPLICATION
# ============================================================

print("\nRemoving duplicates...")

before_dedup = silver_df.count()

silver_df = silver_df.dropDuplicates(
    [
        "video_id",
        "trending_date",
        "region"
    ]
)

after_dedup = silver_df.count()

print(
    "Duplicate rows removed:",
    before_dedup - after_dedup
)


# ============================================================
# FINAL COLUMN ORDER
# ============================================================

silver_df = silver_df.select(

    "video_id",
    "title",
    "channel_title",
    "trending_date",
    "category_id",
    "publish_time",
    "tags",
    "views",
    "likes",
    "dislikes",
    "comment_count",
    "thumbnail_link",
    "comments_disabled",
    "ratings_disabled",
    "video_error_or_removed",
    "description",
    "region",
    "_source_file"
)


# ============================================================
# FINAL SCHEMA
# ============================================================

print("\n========================================")
print("FINAL SILVER SCHEMA")
print("========================================")

silver_df.printSchema()


# ============================================================
# SAMPLE DATA
# ============================================================

print("\n========================================")
print("SAMPLE SILVER DATA")
print("========================================")

silver_df.show(
    10,
    truncate=False
)


# ============================================================
# FINAL COUNT
# ============================================================

silver_count = silver_df.count()

print("\n========================================")
print("FINAL COUNTS")
print("========================================")

print(f"Bronze rows: {bronze_count}")
print(f"Silver rows: {silver_count}")
print(f"Rows removed: {bronze_count - silver_count}")


# ============================================================
# WRITE SILVER
# ============================================================

print("\n========================================")
print("WRITING SILVER")
print("========================================")

(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(SILVER_PATH)
)

print("\n========================================")
print("SUCCESS")
print("========================================")

print(
    f"Silver data written to:\n{SILVER_PATH}"
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()