CREATE EXTERNAL TABLE raw_video_statistics (
    video_id STRING,
    trending_date STRING,
    title STRING,
    channel_title STRING,
    category_id STRING,
    publish_time STRING,
    tags STRING,
    views STRING,
    likes STRING,
    dislikes STRING,
    comment_count STRING,
    thumbnail_link STRING,
    comments_disabled STRING,
    ratings_disabled STRING,
    video_error_or_removed STRING,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://abhiraj-yt-data-pipeline-bronze/youtube/raw_statistics/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);