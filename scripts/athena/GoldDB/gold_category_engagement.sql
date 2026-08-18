CREATE EXTERNAL TABLE IF NOT EXISTS gold_category_engagement (
    category_name STRING,
    total_views BIGINT,
    total_likes BIGINT,
    total_comments BIGINT,
    engagement_rate DOUBLE
)
PARTITIONED BY (
    category_id STRING
)
STORED AS PARQUET
LOCATION 's3://abhiraj-yt-data-pipeline-gold/category_engagement/';