CREATE EXTERNAL TABLE IF NOT EXISTS gold_category_region (
    category_name STRING,
    video_count BIGINT,
    total_views BIGINT,
    avg_views DOUBLE,
    total_likes BIGINT,
    total_comments BIGINT
)
PARTITIONED BY (
    region STRING,
    category_id STRING
)
STORED AS PARQUET
LOCATION 's3://abhiraj-yt-data-pipeline-gold/category_region/';