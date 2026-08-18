CREATE OR REPLACE VIEW all_video_statistics AS

SELECT
    video_id,
    title,
    channel_title,
    trending_date,
    category_id,
    publish_time,
    tags,
    views,
    likes,
    dislikes,
    comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    region,
    CAST(NULL AS DATE) AS ingestion_date,
    CAST(NULL AS INT) AS ingestion_hour
FROM video_statistics

UNION ALL

SELECT
    video_id,
    title,
    channel_title,
    trending_date,
    category_id,
    publish_time,
    tags,
    views,
    likes,
    dislikes,
    comment_count,
    thumbnail_link,
    comments_disabled,
    ratings_disabled,
    video_error_or_removed,
    description,
    region,
    ingestion_date,
    ingestion_hour
FROM video_statistics_v2;