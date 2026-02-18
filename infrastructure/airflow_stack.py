"""
CDK Stack for Airflow pipeline infrastructure.

Creates all AWS resources for the ETL pipelines:
    - S3 buckets (raw, processed, curated, assets)
    - DynamoDB metadata table
    - Glue catalog database
    - Glue jobs (raw_to_processed, processed_to_curated) per pipeline
    - IAM roles for Glue jobs
"""
import os
from pathlib import Path

from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    Tags,
)
from constructs import Construct
from typing import List

# Project root is one level up from infrastructure/
PROJECT_ROOT = Path(__file__).resolve().parent.parent


class AirflowPipelineStack(Stack):
    """CDK stack for Airflow ETL pipeline resources."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        project_code: str = "lmd-dp-airflow-v1",
        pipeline_names: List[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.deploy_env = environment
        self.project_code = project_code
        self.prefix = f"{project_code}-{environment}"
        self.pipeline_names = pipeline_names or ["lib_ifi_pipeline"]

        # ── S3 Buckets ──
        self.raw_bucket = self._create_bucket("raw")
        self.processed_bucket = self._create_bucket("processed")
        self.curated_bucket = self._create_bucket("curated")
        self.assets_bucket = self._create_bucket("assets")

        # ── Upload Glue scripts & SQL to assets bucket ──
        self._deploy_assets()

        # ── DynamoDB Metadata Table ──
        self.metadata_table = self._create_metadata_table()

        # ── Glue Catalog Database ──
        self.glue_database = self._create_glue_database()

        # ── Glue IAM Role (shared across pipelines) ──
        self.glue_role = self._create_glue_role()

        # ── Glue Jobs per pipeline ──
        for pipeline_name in self.pipeline_names:
            self._create_pipeline_glue_jobs(pipeline_name)

        # ── Tags ──
        Tags.of(self).add("Project", project_code)
        Tags.of(self).add("ManagedBy", "CDK")
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "airflow")

    def _create_bucket(self, suffix: str) -> s3.Bucket:
        """Create an S3 bucket with standard config."""
        bucket_name = f"{self.prefix}-{suffix}"
        return s3.Bucket(
            self,
            f"{suffix.capitalize()}Bucket",
            bucket_name=bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _deploy_assets(self):
        """Upload Glue scripts and SQL files to the assets bucket."""
        # Glue job scripts → s3://{prefix}-assets/glue_jobs/
        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[s3deploy.Source.asset(
                str(PROJECT_ROOT / "glue_jobs"),
                exclude=["__pycache__", "*.pyc", "__init__.py"],
            )],
            destination_bucket=self.assets_bucket,
            destination_key_prefix="glue_jobs",
            prune=False,
        )

        # SQL files → s3://{prefix}-assets/sql/
        s3deploy.BucketDeployment(
            self,
            "DeploySqlScripts",
            sources=[s3deploy.Source.asset(
                str(PROJECT_ROOT / "sql"),
                exclude=["__pycache__", "*.pyc"],
            )],
            destination_bucket=self.assets_bucket,
            destination_key_prefix="sql",
            prune=False,
        )

    def _create_metadata_table(self) -> dynamodb.Table:
        """Create DynamoDB table for pipeline watermarks and metadata."""
        return dynamodb.Table(
            self,
            "MetadataTable",
            table_name=f"{self.prefix}-pipeline-metadata",
            partition_key=dynamodb.Attribute(
                name="pipeline_name",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="execution_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _create_glue_database(self) -> glue.CfnDatabase:
        """Create Glue catalog database."""
        db_name = f"{self.prefix}".replace("-", "_")
        return glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=db_name,
                description=f"Glue catalog database for {self.project_code} ({self.deploy_env})",
            ),
        )

    def _create_glue_role(self) -> iam.Role:
        """Create IAM role for Glue ETL jobs."""
        role = iam.Role(
            self,
            "GlueETLRole",
            role_name=f"{self.prefix}-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # S3 access to all pipeline buckets
        for bucket in [
            self.raw_bucket,
            self.processed_bucket,
            self.curated_bucket,
            self.assets_bucket,
        ]:
            bucket.grant_read_write(role)

        # DynamoDB access for metadata updates
        self.metadata_table.grant_read_write_data(role)

        # CloudWatch logs
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:*"],
            )
        )

        return role

    def _create_pipeline_glue_jobs(self, pipeline_name: str):
        """Create Raw->Processed and Processed->Curated Glue jobs for a pipeline."""

        # Raw -> Processed
        glue.CfnJob(
            self,
            f"{pipeline_name}RawToProcessed",
            name=f"{self.prefix}-{pipeline_name}-raw-to-processed",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.prefix}-assets/glue_jobs/{pipeline_name}/raw_to_processed.py",
                python_version="3",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=60,
            max_retries=1,
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--TempDir": f"s3://{self.prefix}-assets/glue_tmp/{pipeline_name}/",
                "--pipeline_name": pipeline_name,
            },
        )

        # Processed -> Curated
        glue.CfnJob(
            self,
            f"{pipeline_name}ProcessedToCurated",
            name=f"{self.prefix}-{pipeline_name}-processed-to-curated",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{self.prefix}-assets/glue_jobs/{pipeline_name}/processed_to_curated.py",
                python_version="3",
            ),
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            timeout=60,
            max_retries=1,
            default_arguments={
                "--job-language": "python",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--TempDir": f"s3://{self.prefix}-assets/glue_tmp/{pipeline_name}/",
                "--pipeline_name": pipeline_name,
            },
        )
