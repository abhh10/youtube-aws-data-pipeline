CREATE EXTERNAL TABLE IF NOT EXISTS gold_category_performance (
    category_name STRING,
    video_count BIGINT,
    total_views BIGINT,
    avg_views DOUBLE,
    total_likes BIGINT,
    avg_likes DOUBLE,
    total_comments BIGINT,
    avg_comments DOUBLE
)
PARTITIONED BY (
    category_id STRING
)
STORED AS PARQUET
LOCATION 's3://abhiraj-yt-data-pipeline-gold/category_performance/';