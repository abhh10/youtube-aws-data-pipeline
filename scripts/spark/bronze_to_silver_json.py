from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    regexp_extract,
    input_file_name,
    trim,
    lit,
    to_date,
    explode,
    to_json,
    get_json_object
)
import sys

# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("YT-JSON-Bronze-to-Silver")
    .getOrCreate()
)


# ============================================================
# INGESTION DATE / HOUR
# ============================================================
#
# Today's test batch.
#
# Later Airflow will pass these dynamically.
# ============================================================



INGESTION_DATE = sys.argv[1]
INGESTION_HOUR = sys.argv[2]


# ============================================================
# S3 PATHS
# ============================================================

BRONZE_PATH = (
    "s3a://abhiraj-yt-data-pipeline-bronze/"
    "youtube/raw_statistics/"
    f"region=*/date={INGESTION_DATE}/"
    f"hour={INGESTION_HOUR}/*.json"
)

SILVER_PATH = (
    "s3a://abhiraj-yt-data-pipeline-silver/"
    "youtube/video_statistics_v2/"
)


# ============================================================
# READ BRONZE JSON
# ============================================================
#
# Each Bronze file is a complete YouTube API response:
#
# {
#   "kind": "...",
#   "etag": "...",
#   "items": [
#       {...},
#       {...}
#   ]
# }
#
# multiLine=true is REQUIRED because the JSON is formatted
# across multiple lines.
# ============================================================

print("\n========================================")
print("READING BRONZE JSON")
print("========================================")

print(f"Ingestion date : {INGESTION_DATE}")
print(f"Ingestion hour : {INGESTION_HOUR}")
print(f"Bronze path    : {BRONZE_PATH}")


raw_df = (
    spark.read
    .option("multiLine", "true")
    .json(BRONZE_PATH)
)


print("\nRaw Bronze schema:")
raw_df.printSchema()


# ============================================================
# EXPLODE ITEMS
# ============================================================
#
# One API response contains:
#
# items = [
#     video1,
#     video2,
#     video3
# ]
#
# We want one Spark row per video.
# ============================================================

df = (
    raw_df
    .select(
        explode(
            col("items")
        ).alias("item")
    )
)


print("\nItems exploded successfully.")


# ============================================================
# CONVERT EACH ITEM TO JSON
# ============================================================
#
# This makes the extraction robust against optional fields.
#
# For example:
#
# statistics.dislikeCount
#
# may not exist.
#
# get_json_object() simply returns NULL instead of failing.
# ============================================================

df = df.withColumn(
    "_item_json",
    to_json(col("item"))
)


# ============================================================
# BRONZE → SILVER
# ============================================================

print("\n========================================")
print("TRANSFORMING BRONZE → SILVER")
print("========================================")


silver_df = df.select(

    # --------------------------------------------------------
    # VIDEO ID
    # --------------------------------------------------------

    trim(
        get_json_object(
            col("_item_json"),
            "$.id"
        )
    ).alias("video_id"),


    # --------------------------------------------------------
    # TITLE
    # --------------------------------------------------------

    get_json_object(
        col("_item_json"),
        "$.snippet.title"
    ).alias("title"),


    # --------------------------------------------------------
    # CHANNEL TITLE
    # --------------------------------------------------------

    get_json_object(
        col("_item_json"),
        "$.snippet.channelTitle"
    ).alias("channel_title"),


    # --------------------------------------------------------
    # TRENDING DATE
    # --------------------------------------------------------
    #
    # The current YouTube API JSON does NOT contain the old
    # CSV's trending_date field.
    #
    # Therefore we intentionally leave this NULL.
    #
    # DO NOT use publishedAt as trending_date.
    # They mean different things.
    # --------------------------------------------------------

    lit(None)
    .cast("date")
    .alias("trending_date"),


    # --------------------------------------------------------
    # CATEGORY ID
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(
                get_json_object(
                    _item_json,
                    '$.snippet.categoryId'
                )
            ) AS INT
        )
    """).alias("category_id"),


    # --------------------------------------------------------
    # PUBLISH TIME
    # --------------------------------------------------------
    #
    # Example:
    #
    # 2026-08-13T17:42:06Z
    # --------------------------------------------------------

    expr("""
        try_to_timestamp(
            get_json_object(
                _item_json,
                '$.snippet.publishedAt'
            ),
            'yyyy-MM-dd''T''HH:mm:ss''Z'''
        )
    """).alias("publish_time"),


    # --------------------------------------------------------
    # TAGS
    # --------------------------------------------------------
    #
    # get_json_object returns the JSON representation of the
    # array, e.g.
    #
    # ["music","official","video"]
    #
    # which is stored as STRING in Silver.
    # --------------------------------------------------------

    get_json_object(
        col("_item_json"),
        "$.snippet.tags"
    ).alias("tags"),


    # --------------------------------------------------------
    # THUMBNAIL
    #
    # Use default thumbnail if available.
    # If not, get_json_object returns NULL.
    # --------------------------------------------------------

    get_json_object(
        col("_item_json"),
        "$.snippet.thumbnails.default.url"
    ).alias("thumbnail_link"),


    # --------------------------------------------------------
    # VIEWS
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(
                get_json_object(
                    _item_json,
                    '$.statistics.viewCount'
                )
            ) AS BIGINT
        )
    """).alias("views"),


    # --------------------------------------------------------
    # LIKES
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(
                get_json_object(
                    _item_json,
                    '$.statistics.likeCount'
                )
            ) AS BIGINT
        )
    """).alias("likes"),


    # --------------------------------------------------------
    # DISLIKES
    # --------------------------------------------------------
    #
    # YouTube may not return this field.
    # If absent → NULL.
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(
                get_json_object(
                    _item_json,
                    '$.statistics.dislikeCount'
                )
            ) AS BIGINT
        )
    """).alias("dislikes"),


    # --------------------------------------------------------
    # COMMENTS
    # --------------------------------------------------------

    expr("""
        try_cast(
            trim(
                get_json_object(
                    _item_json,
                    '$.statistics.commentCount'
                )
            ) AS BIGINT
        )
    """).alias("comment_count"),


    # --------------------------------------------------------
    # BOOLEAN FLAGS
    # --------------------------------------------------------
    #
    # These existed in the old CSV but aren't necessarily
    # present in the current API response.
    #
    # Therefore NULL is safer than inventing values.
    # --------------------------------------------------------

    lit(None)
    .cast("boolean")
    .alias("comments_disabled"),

    lit(None)
    .cast("boolean")
    .alias("ratings_disabled"),

    lit(None)
    .cast("boolean")
    .alias("video_error_or_removed"),


    # --------------------------------------------------------
    # DESCRIPTION
    # --------------------------------------------------------

    get_json_object(
        col("_item_json"),
        "$.snippet.description"
    ).alias("description"),


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
# ADD INGESTION DATE
# ============================================================

silver_df = silver_df.withColumn(
    "ingestion_date",
    to_date(
        lit(INGESTION_DATE)
    )
)


# ============================================================
# ADD INGESTION HOUR
# ============================================================

silver_df = silver_df.withColumn(
    "ingestion_hour",
    lit(int(INGESTION_HOUR))
)


# ============================================================
# REMOVE INVALID VIDEO IDs
# ============================================================

print("\n========================================")
print("VALIDATING VIDEO IDs")
print("========================================")


before_id_filter = silver_df.count()


silver_df = silver_df.filter(
    col("video_id").isNotNull()
    &
    (
        trim(col("video_id")) != ""
    )
)


after_id_filter = silver_df.count()


print(
    "Rows removed:",
    before_id_filter - after_id_filter
)


# ============================================================
# REMOVE INVALID REGIONS
# ============================================================

print("\n========================================")
print("VALIDATING REGIONS")
print("========================================")


before_region_filter = silver_df.count()


silver_df = silver_df.filter(
    col("region").isNotNull()
    &
    (
        trim(col("region")) != ""
    )
)


after_region_filter = silver_df.count()


print(
    "Rows removed:",
    before_region_filter - after_region_filter
)


# ============================================================
# DEDUPLICATION
# ============================================================
#
# IMPORTANT:
#
# Multiple API snapshots can contain the same video.
#
# Example:
#
# 10:09 → video A → 1,000 views
# 10:15 → video A → 1,200 views
# 10:20 → video A → 1,400 views
#
# These are NOT duplicates.
#
# _source_file identifies the snapshot from which the row came.
#
# Therefore we only remove the exact same video appearing
# multiple times within the same source JSON file.
# ============================================================

print("\n========================================")
print("DEDUPLICATING")
print("========================================")


before_dedup = silver_df.count()


silver_df = silver_df.dropDuplicates(
    [
        "video_id",
        "region",
        "_source_file"
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
    "ingestion_date",
    "ingestion_hour",

    "_source_file"
)


# ============================================================
# FINAL SCHEMA
# ============================================================

print("\n========================================")
print("FINAL SILVER V2 SCHEMA")
print("========================================")

silver_df.printSchema()


# ============================================================
# SAMPLE DATA
# ============================================================

print("\n========================================")
print("SAMPLE SILVER V2 DATA")
print("========================================")


silver_df.show(
    10,
    truncate=False
)


# ============================================================
# FINAL COUNTS
# ============================================================

silver_count = silver_df.count()


print("\n========================================")
print("FINAL COUNTS")
print("========================================")


print(
    f"Bronze rows : {bronze_count if 'bronze_count' in globals() else 'N/A'}"
)

print(
    f"Silver rows : {silver_count}"
)


# ============================================================
# WRITE SILVER V2
# ============================================================

print("\n========================================")
print("WRITING SILVER V2")
print("========================================")

# Only overwrite partitions present in this batch.
# Existing Silver partitions remain untouched.
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

(
    silver_df
    .write
    .mode("overwrite")
    .partitionBy(
        "region",
        "ingestion_date",
        "ingestion_hour"
    )
    .parquet(
        SILVER_PATH
    )
)

print("\n========================================")
print("SUCCESS")
print("========================================")

print(
    f"Silver V2 written to:\n{SILVER_PATH}"
)