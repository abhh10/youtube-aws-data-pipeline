CREATE EXTERNAL TABLE IF NOT EXISTS video_statistics_v2 (
    video_id STRING,
    title STRING,
    channel_title STRING,
    trending_date DATE,
    category_id INT,
    publish_time TIMESTAMP,
    tags STRING,
    views BIGINT,
    likes BIGINT,
    dislikes BIGINT,
    comment_count BIGINT,
    thumbnail_link STRING,
    comments_disabled BOOLEAN,
    ratings_disabled BOOLEAN,
    video_error_or_removed BOOLEAN,
    description STRING
)
PARTITIONED BY (
    region STRING,
    ingestion_date DATE,
    ingestion_hour INT
)
STORED AS PARQUET
LOCATION 's3://abhiraj-yt-data-pipeline-silver/youtube/video_statistics_v2/';
