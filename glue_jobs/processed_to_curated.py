"""
Processed → Curated Glue Spark Job.

Reads cleaned Parquet from Processed layer, applies business logic SQL transformations,
and writes curated Parquet to Curated layer.

Arguments:
    --source_path:      S3 path to Processed Parquet data
    --target_bucket:    S3 bucket for Curated output
    --pipeline_name:    Pipeline identifier
    --execution_id:     Unique execution ID
    --country:          Country code
    --ingestion_time:   ISO timestamp of ingestion
    --custom_sql_path:  S3 path to custom SQL file (optional)
    --metadata_table:   DynamoDB metadata table name
"""
import sys
import json
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3


def read_sql_from_s3(s3_path):
    """Read SQL content from S3."""
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def load_sql_transformations(sql_content):
    """
    Parse SQL file into sequential transformations.

    Format:
        -- transform_step_name
        SELECT ... FROM __TABLE__;

    Each step reads from __TABLE__ (the previous step's result)
    and produces a new __TABLE__ for the next step.
    """
    transformations = []
    current_sql = []
    current_name = None

    for line in sql_content.split("\n"):
        if line.strip().startswith("-- transform_"):
            if current_sql and current_name:
                transformations.append({
                    "name": current_name,
                    "sql": "\n".join(current_sql).strip(),
                })
            current_name = line.strip()[3:]  # Remove "-- "
            current_sql = []
        elif line.strip() and not line.strip().startswith("--"):
            current_sql.append(line)

    if current_sql and current_name:
        transformations.append({
            "name": current_name,
            "sql": "\n".join(current_sql).strip(),
        })

    return transformations


def build_output_path(country, pipeline_name, execution_id, ingestion_time):
    """Build standardized output path."""
    ts = datetime.fromisoformat(ingestion_time)
    date_path = ts.strftime("%Y/%m/%d")
    return f"{country}/{pipeline_name}/{date_path}/{execution_id}/"


# ============================================
# MAIN
# ============================================
if __name__ == "__main__":
    from pyspark.sql.types import StructType, ArrayType, MapType

    def ensure_primitive_types(df):
        """Convert any remaining complex types to JSON strings."""
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType, MapType)):
                print(f"[Type Safety] Converting complex column '{field.name}' to JSON string")
                df = df.withColumn(field.name, F.to_json(F.col(f"`{field.name}`")))
        return df

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    # Write timestamps as TIMESTAMP_MICROS (Redshift COPY cannot read INT96)
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
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
        "metadata_table",
    ])
    job.init(args["JOB_NAME"], args)

    dynamodb = boto3.resource("dynamodb")
    metadata_table = dynamodb.Table(args["metadata_table"])

    print("=" * 70)
    print(f"PROCESSED -> CURATED | Pipeline: {args['pipeline_name']}")
    print(f"Source: {args['source_path']}")
    print("=" * 70)

    try:
        # STEP 1: Read Processed Parquet
        df = spark.read.parquet(args["source_path"])
        input_count = df.count()
        print(f"Read {input_count:,} rows with {len(df.columns)} columns")

        # STEP 2: Apply custom SQL transformations
        custom_sql_path = args.get("custom_sql_path", "")
        if custom_sql_path:
            print(f"Loading SQL from: {custom_sql_path}")
            sql_content = read_sql_from_s3(custom_sql_path)
            transformations = load_sql_transformations(sql_content)

            if transformations:
                print(f"Applying {len(transformations)} transformations")
                for i, transform in enumerate(transformations, 1):
                    print(f"  [{i}/{len(transformations)}] {transform['name']}")
                    df.createOrReplaceTempView("__TABLE__")
                    df = spark.sql(transform["sql"])
            else:
                # Single SQL without transform markers — run directly
                df.createOrReplaceTempView("processed_data")
                df = spark.sql(sql_content)
        else:
            print("No custom SQL — passing through Processed data as-is")

        # STEP 3: Add curated metadata
        df = (
            df.withColumn("_curated_at", F.lit(datetime.utcnow().isoformat()))
            .withColumn("_curated_execution_id", F.lit(args["execution_id"]))
        )

        # Ensure all columns are primitive types (Redshift COPY requirement)
        df = ensure_primitive_types(df)

        final_count = df.count()

        # STEP 4: Write Curated Parquet
        output_key = build_output_path(
            args["country"], args["pipeline_name"],
            args["execution_id"], args["ingestion_time"],
        )
        s3_output = f"s3://{args['target_bucket']}/{output_key}"

        df.write.mode("overwrite").option("compression", "snappy").parquet(s3_output)
        print(f"Curated Parquet written to: {s3_output}")

        # STEP 5: Update metadata
        metadata_table.update_item(
            Key={
                "pipeline_name": args["pipeline_name"],
                "execution_id": args["execution_id"],
            },
            UpdateExpression="""
                SET #status = :status,
                    curated_end_time = :end_time,
                    curated_row_count = :row_count,
                    curated_s3_path = :s3_path
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "curated_complete",
                ":end_time": datetime.utcnow().isoformat(),
                ":row_count": final_count,
                ":s3_path": s3_output,
            },
        )

        print("=" * 70)
        print(f"PROCESSED -> CURATED COMPLETE | {input_count:,} -> {final_count:,} rows")
        print("=" * 70)

    except Exception as e:
        print(f"PROCESSED -> CURATED FAILED: {e}")
        try:
            metadata_table.update_item(
                Key={
                    "pipeline_name": args["pipeline_name"],
                    "execution_id": args["execution_id"],
                },
                UpdateExpression="SET #status = :status, error_message = :error",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":status": "curated_failed",
                    ":error": str(e),
                },
            )
        except Exception:
            pass
        raise
    finally:
        job.commit()
