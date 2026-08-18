"""
Lambda: JSON Reference Data -> Silver Layer (Parquet)

Purpose:
    Process YouTube category/reference JSON from Bronze S3 into
    a canonical Silver reference dataset.

Architecture:

    Bronze
        |
        | daily JSON
        v
    Lambda
        |
        | read existing Silver
        | merge + upsert
        v
    Silver
        |
        | Parquet
        | partitioned by region
        v
    Glue Catalog
        |
        v
    Athena

Silver strategy:
    - Bronze remains historical/date-wise.
    - Silver contains the current canonical reference data.
    - Silver is partitioned by region.
    - Business key = (region, id).
    - New incoming records replace older versions.
    - New categories are inserted.
    - Existing categories are preserved.
    - The complete merged dataset is written back to Silver.

Environment Variables:
    S3_BUCKET_SILVER
    GLUE_DB_SILVER
    GLUE_TABLE_REFERENCE
    SNS_ALERT_TOPIC_ARN (optional)
"""

import json
import os
import logging
import re
import time
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd


# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# =============================================================================
# CONFIGURATION
# =============================================================================

SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]

GLUE_DB = os.environ.get(
    "GLUE_DB_SILVER",
    "yt_pipeline_silver_dev"
)

GLUE_TABLE = os.environ.get(
    "GLUE_TABLE_REFERENCE",
    "clean_reference_data"
)

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
)

SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/youtube/reference_data/"
)


# =============================================================================
# AWS CLIENTS
# =============================================================================

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


# =============================================================================
# COLUMN NORMALIZATION
# =============================================================================

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names so Bronze and Silver use the same schema.

    Examples:

        snippet.title
            -> snippet_title

        snippet.channelId
            -> snippet_channelid

        weird-column.name
            -> weird_column_name

    If multiple columns become identical after normalization,
    their values are merged using the first non-null value.
    """

    if df.empty:
        return df.copy()

    df = df.copy()

    normalized_names = []

    for column in df.columns:

        column = str(column)

        # Replace non-alphanumeric characters with _
        column = re.sub(
            r"[^a-zA-Z0-9_]",
            "_",
            column
        )

        # Lowercase
        column = column.lower()

        # Collapse multiple underscores
        column = re.sub(
            r"_+",
            "_",
            column
        )

        # Remove leading/trailing underscores
        column = column.strip("_")

        normalized_names.append(column)

    df.columns = normalized_names

    # -------------------------------------------------------------------------
    # Handle duplicate columns after normalization
    # -------------------------------------------------------------------------

    duplicate_columns = (
        df.columns[
            df.columns.duplicated(keep=False)
        ]
        .unique()
        .tolist()
    )

    if duplicate_columns:

        logger.warning(
            f"Duplicate columns after normalization: "
            f"{duplicate_columns}"
        )

        for column_name in duplicate_columns:

            positions = [
                i
                for i, column in enumerate(df.columns)
                if column == column_name
            ]

            duplicate_data = df.iloc[:, positions]

            # Take first non-null value across duplicate columns
            merged_column = (
                duplicate_data
                .bfill(axis=1)
                .iloc[:, 0]
            )

            # Remove all occurrences
            df = df.drop(
                columns=[column_name]
            )

            # Add merged column back
            df[column_name] = merged_column

            logger.info(
                f"Merged duplicate column: "
                f"{column_name}"
            )

    return df


# =============================================================================
# READ JSON FROM S3
# =============================================================================

def read_json_from_s3(
    bucket: str,
    key: str
) -> dict:

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    return json.loads(content)


# =============================================================================
# READ EXISTING SILVER
# =============================================================================

def read_existing_silver() -> pd.DataFrame:
    """
    Read existing canonical Silver data.

    If Silver does not exist yet, return an empty DataFrame.
    """

    try:

        df = wr.s3.read_parquet(
            path=SILVER_PATH,
            dataset=True
        )

        logger.info(
            f"Existing Silver rows: {len(df)}"
        )

        logger.info(
            f"Existing Silver columns: "
            f"{list(df.columns)}"
        )

        # Make sure existing Silver follows the same
        # column naming convention.
        df = normalize_column_names(df)

        return df

    except Exception as e:

        logger.info(
            "No existing Silver dataset found. "
            f"Starting from empty dataset. Reason: {e}"
        )

        return pd.DataFrame()


# =============================================================================
# VALIDATION
# =============================================================================

def validate_category_data(
    df: pd.DataFrame
) -> pd.DataFrame:

    if df.empty:

        raise ValueError(
            "Empty DataFrame - no category items found"
        )

    required_columns = {
        "id",
        "snippet_title"
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:

        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}. "
            f"Available columns: "
            f"{list(df.columns)}"
        )

    return df


# =============================================================================
# EXTRACT REGION
# =============================================================================

def extract_region(key: str) -> str:

    for part in key.split("/"):

        if part.startswith("region="):

            return part.split(
                "=",
                1
            )[1]

    return "unknown"


# =============================================================================
# SNS ALERT
# =============================================================================

def send_alert(
    subject: str,
    message: str
):

    if not SNS_TOPIC:
        return

    sns_client.publish(
        TopicArn=SNS_TOPIC,
        Subject=subject[:100],
        Message=message
    )


# =============================================================================
# WRITE SILVER WITH RETRY
# =============================================================================

def write_silver_with_retry(
    df: pd.DataFrame
):
    """
    Write Silver dataset.

    Glue Catalog can occasionally reject simultaneous UpdateTable
    operations with ConcurrentModificationException.

    Retry those transient failures with exponential backoff.

    Attempts:

        1 -> wait 2 sec
        2 -> wait 4 sec
        3 -> wait 8 sec
        4 -> wait 16 sec
        5 -> fail
    """

    max_retries = 5

    for attempt in range(
        1,
        max_retries + 1
    ):

        try:

            logger.info(
                f"Silver write attempt "
                f"{attempt}/{max_retries}"
            )

            wr.s3.to_parquet(

                df=df,

                path=SILVER_PATH,

                dataset=True,

                database=GLUE_DB,

                table=GLUE_TABLE,

                partition_cols=[
                    "region"
                ],

                mode="overwrite",

                schema_evolution=True
            )

            logger.info(
                "Silver write successful"
            )

            return

        except Exception as e:

            error_message = str(e)

            # Only retry Glue concurrency conflicts.
            if (
                "ConcurrentModificationException"
                not in error_message
            ):
                raise

            if attempt == max_retries:

                logger.error(
                    "Glue UpdateTable still failing "
                    f"after {max_retries} attempts."
                )

                raise

            wait_seconds = 2 ** attempt

            logger.warning(
                "Glue ConcurrentModificationException "
                f"detected. Waiting {wait_seconds} seconds "
                "before retry."
            )

            time.sleep(
                wait_seconds
            )


# =============================================================================
# LAMBDA HANDLER
# =============================================================================

def lambda_handler(
    event,
    context
):

    # -------------------------------------------------------------------------
    # Extract S3 records
    # -------------------------------------------------------------------------

    records = event.get(
        "Records",
        []
    )

    # Support direct invocation containing an S3 event.
    if not records:

        if "s3" in event:

            records = [event]

        else:

            records = []


    processed = []
    errors = []


    # -------------------------------------------------------------------------
    # Process records
    # -------------------------------------------------------------------------

    for record in records:

        key = "unknown"

        try:

            # =================================================================
            # S3 INFORMATION
            # =================================================================

            s3_info = record["s3"]

            bucket = (
                s3_info["bucket"]["name"]
            )

            key = unquote_plus(
                s3_info["object"]["key"]
            )

            logger.info(
                f"Processing: "
                f"s3://{bucket}/{key}"
            )


            # =================================================================
            # REGION
            # =================================================================

            region = extract_region(
                key
            )

            logger.info(
                f"Detected region: {region}"
            )


            # =================================================================
            # READ BRONZE JSON
            # =================================================================

            raw_data = read_json_from_s3(
                bucket,
                key
            )


            # =================================================================
            # EXTRACT ITEMS
            # =================================================================

            if (
                "items" in raw_data
                and isinstance(
                    raw_data["items"],
                    list
                )
            ):

                df = pd.json_normalize(
                    raw_data["items"]
                )

            else:

                df = pd.json_normalize(
                    raw_data
                )


            logger.info(
                f"Raw shape: {df.shape}"
            )

            logger.info(
                f"Raw columns: "
                f"{list(df.columns)}"
            )


            # =================================================================
            # NORMALIZE COLUMNS
            # =================================================================

            df = normalize_column_names(
                df
            )

            logger.info(
                f"Normalized columns: "
                f"{list(df.columns)}"
            )


            # =================================================================
            # VALIDATE
            # =================================================================

            df = validate_category_data(
                df
            )


            # =================================================================
            # ADD METADATA
            # =================================================================

            df["_ingestion_timestamp"] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            df["_source_file"] = key

            df["region"] = region


            # =================================================================
            # STANDARDIZE BUSINESS KEY TYPES
            # =================================================================

            df["id"] = (
                df["id"]
                .astype(str)
                .str.strip()
            )

            df["region"] = (
                df["region"]
                .astype(str)
                .str.strip()
                .str.lower()
            )


            # =================================================================
            # DEDUPLICATE INCOMING DATA
            # =================================================================

            incoming_before = len(df)

            df = (
                df
                .drop_duplicates(
                    subset=[
                        "region",
                        "id"
                    ],
                    keep="last"
                )
                .reset_index(drop=True)
            )

            logger.info(
                f"Incoming rows: "
                f"{incoming_before}"
            )

            logger.info(
                f"Incoming duplicate rows removed: "
                f"{incoming_before - len(df)}"
            )


            # =================================================================
            # READ EXISTING SILVER
            # =================================================================

            existing_df = (
                read_existing_silver()
            )


            # =================================================================
            # NORMALIZE EXISTING SILVER
            # =================================================================

            if not existing_df.empty:

                existing_df = (
                    normalize_column_names(
                        existing_df
                    )
                )

                # Normalize key types.
                if "id" in existing_df.columns:

                    existing_df["id"] = (
                        existing_df["id"]
                        .astype(str)
                        .str.strip()
                    )

                if "region" in existing_df.columns:

                    existing_df["region"] = (
                        existing_df["region"]
                        .astype(str)
                        .str.strip()
                        .str.lower()
                    )


            # =================================================================
            # MERGE EXISTING SILVER + NEW BRONZE
            # =================================================================
            #
            # Existing comes FIRST.
            # New Bronze comes SECOND.
            #
            # Therefore keep="last" means:
            #
            # NEW DATA WINS.
            # =================================================================

            if existing_df.empty:

                combined_df = df.copy()

            else:

                combined_df = pd.concat(
                    [
                        existing_df,
                        df
                    ],
                    ignore_index=True,
                    sort=False
                )


            logger.info(
                f"Rows before final deduplication: "
                f"{len(combined_df)}"
            )


            # =================================================================
            # NORMALIZE AFTER MERGE
            # =================================================================

            combined_df = (
                normalize_column_names(
                    combined_df
                )
            )


            # =================================================================
            # FINAL UPSERT
            # =================================================================

            combined_df = (
                combined_df
                .drop_duplicates(
                    subset=[
                        "region",
                        "id"
                    ],
                    keep="last"
                )
                .reset_index(drop=True)
            )


            logger.info(
                f"Final Silver rows: "
                f"{len(combined_df)}"
            )

            logger.info(
                f"Final Silver columns: "
                f"{list(combined_df.columns)}"
            )


            # =================================================================
            # FINAL SAFETY CHECK
            # =================================================================

            if combined_df.columns.duplicated().any():

                duplicate_columns = (
                    combined_df.columns[
                        combined_df.columns.duplicated()
                    ]
                    .tolist()
                )

                raise ValueError(
                    "Duplicate columns remain before "
                    f"Silver write: {duplicate_columns}"
                )


            # =================================================================
            # WRITE SILVER
            # =================================================================

            write_silver_with_retry(
                combined_df
            )


            # =================================================================
            # SUCCESS
            # =================================================================

            processed.append(
                {
                    "key": key,
                    "region": region,
                    "incoming_rows": len(df),
                    "final_rows": len(combined_df)
                }
            )


        # =====================================================================
        # ERROR HANDLING
        # =====================================================================

        except Exception as e:

            logger.error(
                f"Error processing {key}: {e}",
                exc_info=True
            )

            errors.append(
                {
                    "key": key,
                    "error": str(e)
                }
            )


    # =========================================================================
    # SNS ALERT
    # =========================================================================

    if errors:

        send_alert(

            subject=(
                "[YT Pipeline] "
                "Silver reference transform failed"
            ),

            message=json.dumps(
                errors,
                indent=2
            )
        )


    # =========================================================================
    # RESPONSE
    # =========================================================================

    return {

        "statusCode": 200,

        "processed": processed,

        "errors": errors

    }