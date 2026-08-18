CREATE EXTERNAL TABLE IF NOT EXISTS gold_video_rankings (
    video_id STRING,
    title STRING,
    category_name STRING,
    region STRING,
    views BIGINT,
    video_rank BIGINT
)
PARTITIONED BY (
    category_id STRING
)
STORED AS PARQUET
LOCATION 's3://abhiraj-yt-data-pipeline-gold/video_rankings/';