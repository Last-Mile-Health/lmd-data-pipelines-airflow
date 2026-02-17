"""
Raw → Processed Glue Spark Job.

Dynamically reads raw data (JSON/CSV), flattens nested structures,
cleans column names, deduplicates, trims strings, drops empty columns,
and writes clean Parquet to the Processed layer.

No hardcoded schemas — adapts to whatever columns the source provides.

Arguments:
    --source_path:      S3 path to raw data
    --target_bucket:    S3 bucket for Processed output
    --pipeline_name:    Pipeline identifier
    --execution_id:     Unique execution ID
    --country:          Country code
    --ingestion_time:   ISO timestamp of ingestion
    --dedup_key:        Column to deduplicate on (optional)
    --load_mode:        full | incremental
    --metadata_table:   DynamoDB metadata table name
    --source_format:    json | csv (default: json)
"""
import sys
import json
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, StringType
import boto3


# ============================================
# HELPER FUNCTIONS
# ============================================
def clean_column_name(name):
    """Clean column names for Parquet/Glue compatibility."""
    return (
        name.replace("/", "_")
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .lower()
    )

def read_sql_from_s3(s3_path):
    """Read SQL content from S3."""
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")



def flatten_dataframe(df, max_depth=10):
    """Recursively flatten StructType fields, convert ArrayType to JSON strings."""
    for _ in range(max_depth):
        complex_fields = [
            (f.name, f.dataType)
            for f in df.schema.fields
            if isinstance(f.dataType, (StructType, ArrayType))
        ]
        if not complex_fields:
            break

        for field_name, field_type in complex_fields:
            if isinstance(field_type, StructType):
                expanded = [
                    F.col(f"`{field_name}`.`{sub.name}`").alias(f"{field_name}_{sub.name}")
                    for sub in field_type
                ]
                df = df.select("*", *expanded).drop(field_name)
            elif isinstance(field_type, ArrayType):
                df = df.withColumn(field_name, F.to_json(F.col(field_name)))

    return df


def trim_string_columns(df):
    """Trim whitespace from all string columns."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(f"`{field.name}`")))
    return df


def drop_null_columns(df):
    """Drop columns where every value is null."""
    row_count = df.count()
    if row_count == 0:
        return df
    null_counts = df.select(
        [F.count(F.when(F.col(f"`{c}`").isNull(), c)).alias(c) for c in df.columns]
    ).collect()[0]
    drop_cols = [c for c in df.columns if null_counts[c] == row_count]
    if drop_cols:
        print(f"Dropping {len(drop_cols)} fully-null columns: {drop_cols}")
        df = df.drop(*drop_cols)
    return df


def build_output_path(country, pipeline_name, execution_id, ingestion_time):
    """Build standardized output path."""
    ts = datetime.fromisoformat(ingestion_time)
    date_path = ts.strftime("%Y/%m/%d")
    return f"{country}/{pipeline_name}/{date_path}/{execution_id}/"


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "source_path",
        "target_bucket",
        "pipeline_name",
        "execution_id",
        "country",
        "ingestion_time",
        "custom_sql_path",
        "dedup_key",
        "load_mode",
        "metadata_table",
        "source_format",
    ])
    job.init(args["JOB_NAME"], args)

    dynamodb = boto3.resource("dynamodb")
    metadata_table = dynamodb.Table(args["metadata_table"])

    print("=" * 70)
    print(f"RAW -> PROCESSED | Pipeline: {args['pipeline_name']}")
    print(f"Source: {args['source_path']}")
    print(f"Mode: {args['load_mode']}")
    print("=" * 70)

    try:
        # STEP 1: Read raw data
        source_format = args.get("source_format", "json")
        if source_format == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(args["source_path"])
        else:
            df = spark.read.option("multiLine", "true").json(args["source_path"])

        row_count = df.count()
        print(f"Read {row_count:,} records with {len(df.columns)} columns")
        print(f"Source columns: {df.columns}")

        if row_count == 0:
            print("WARNING: Source data is empty — writing empty Parquet")

        # STEP 2: Explode 'results' array if present (common in API responses)
        if "results" in df.columns:
            df = df.select(F.explode("results").alias("record")).select("record.*")
            row_count = df.count()
            print(f"Exploded results array: {row_count:,} records")

        # STEP 3: Flatten nested structures
        df = flatten_dataframe(df)
        print(f"After flatten: {len(df.columns)} columns")

        # STEP 4: Clean column names
        for old_name in df.columns:
            new_name = clean_column_name(old_name)
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)

        # STEP 5: Trim whitespace on all string columns
        df = trim_string_columns(df)

        # STEP 6: Drop fully-null columns
        df = drop_null_columns(df)

        # STEP 7: Deduplicate
        dedup_key = args.get("dedup_key", "")
        if dedup_key and dedup_key in df.columns:
            before = df.count()
            df = df.dropDuplicates([dedup_key])
            after = df.count()
            print(f"Deduplicated on '{dedup_key}': {before:,} -> {after:,}")
            row_count = after

        # STEP 8: Add metadata columns
        df = (
            df.withColumn("_pipeline_execution_id", F.lit(args["execution_id"]))
            .withColumn("_ingestion_timestamp", F.lit(args["ingestion_time"]))
            .withColumn("_pipeline_name", F.lit(args["pipeline_name"]))
            .withColumn("_country", F.lit(args["country"]))
            .withColumn("_processed_at", F.lit(datetime.utcnow().isoformat()))
        )

        # STEP 9: Apply custom SQL (optional — for pipeline-specific cleaning)
        custom_sql_path = args.get("custom_sql_path", "")
        if custom_sql_path:
            print(f"Applying custom SQL from: {custom_sql_path}")
            sql_content = read_sql_from_s3(custom_sql_path)
            df.createOrReplaceTempView("source_data")
            df = spark.sql(sql_content)
            row_count = df.count()
            print(f"After custom SQL: {row_count:,} records")

        # STEP 10: Write Parquet
        output_key = build_output_path(
            args["country"], args["pipeline_name"],
            args["execution_id"], args["ingestion_time"],
        )
        s3_output = f"s3://{args['target_bucket']}/{output_key}"
        write_mode = "overwrite" if args["load_mode"] == "full" else "append"

        df.write.mode(write_mode).option("compression", "snappy").parquet(s3_output)
        print(f"Parquet written to: {s3_output}")

        # STEP 10: Update metadata
        col_count = len(df.columns)
        row_count = df.count()
        metadata_table.update_item(
            Key={
                "pipeline_name": args["pipeline_name"],
                "execution_id": args["execution_id"],
            },
            UpdateExpression="""
                SET #status = :status,
                    transform_end_time = :end_time,
                    row_count = :row_count,
                    column_count = :column_count,
                    processed_s3_path = :s3_path,
                    schema_columns = :columns
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "processed_complete",
                ":end_time": datetime.utcnow().isoformat(),
                ":row_count": row_count,
                ":column_count": col_count,
                ":s3_path": s3_output,
                ":columns": json.dumps(df.columns),
            },
        )

        print("=" * 70)
        print(f"RAW -> PROCESSED COMPLETE | {row_count:,} rows, {col_count} cols")
        print("=" * 70)

    except Exception as e:
        print(f"RAW -> PROCESSED FAILED: {e}")
        try:
            metadata_table.update_item(
                Key={
                    "pipeline_name": args["pipeline_name"],
                    "execution_id": args["execution_id"],
                },
                UpdateExpression="SET #status = :status, error_message = :error",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": "processed_failed",
                    ":error": str(e),
                },
            )
        except Exception:
            pass
        raise
    finally:
        job.commit()
