"""
CDK Stack for Airflow pipeline infrastructure.

Creates all AWS resources for the ETL pipelines:
    - S3 buckets (raw, processed, curated, assets)
    - DynamoDB metadata table
    - Glue catalog database
    - Glue jobs (raw_to_processed, processed_to_curated) per pipeline

If a resource already exists, it is imported. For IAM roles, the trust
policy and permissions are always enforced via CfnRole to prevent drift.
"""
import os
import time
from pathlib import Path

from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_dynamodb as dynamodb,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    Tags,
)
from constructs import Construct
from typing import List

from resource_lookup import bucket_exists, dynamodb_table_exists

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

        # ── S3 Buckets (import if exists, safe for data buckets) ──
        self.raw_bucket = self._create_or_import_bucket("raw")
        self.processed_bucket = self._create_or_import_bucket("processed")
        self.curated_bucket = self._create_or_import_bucket("curated")
        self.assets_bucket = self._create_or_import_bucket("assets")

        # ── Upload Glue scripts & SQL to assets bucket ──
        self._deploy_assets()

        # ── DynamoDB Metadata Table (import if exists, safe) ──
        self.metadata_table = self._create_or_import_metadata_table()

        # ── Glue Catalog Database ──
        self.glue_database = self._create_glue_database()

        # ── Glue IAM Role (always enforce trust policy + permissions) ──
        self.glue_role = self._ensure_glue_role()

        # ── Redshift Spectrum IAM Role (S3 + Glue catalog access) ──
        self.redshift_role = self._ensure_redshift_role()

        # ── Glue Jobs per pipeline ──
        for pipeline_name in self.pipeline_names:
            self._create_pipeline_glue_jobs(pipeline_name)

        # ── Tags ──
        Tags.of(self).add("Project", project_code)
        Tags.of(self).add("ManagedBy", "CDK")
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "airflow")

        # ── Outputs ──
        CfnOutput(self, "RedshiftSpectrumRoleArn",
                  value=self.redshift_role.role_arn,
                  export_name=f"{self.prefix}-redshift-spectrum-role-arn")

    def _create_or_import_bucket(self, suffix: str) -> s3.IBucket:
        """Create or import an S3 bucket."""
        bucket_name = f"{self.prefix}-{suffix}"
        construct_id = f"{suffix.capitalize()}Bucket"

        if bucket_exists(bucket_name):
            return s3.Bucket.from_bucket_name(self, construct_id, bucket_name)

        return s3.Bucket(
            self,
            construct_id,
            bucket_name=bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _deploy_assets(self):
        """Upload Glue scripts and SQL files to the assets bucket.

        A deploy-time marker (timestamp) is included as an extra source so the
        asset hash always changes, forcing CDK to re-upload on every deploy even
        if file contents haven't changed.
        """
        deploy_marker = s3deploy.Source.data("_deploy_marker.txt", str(time.time()))

        s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",
            sources=[
                s3deploy.Source.asset(
                    str(PROJECT_ROOT / "glue_jobs"),
                    exclude=["__pycache__", "*.pyc", "__init__.py"],
                ),
                deploy_marker,
            ],
            destination_bucket=self.assets_bucket,
            destination_key_prefix="glue_jobs",
            prune=False,
        )

        s3deploy.BucketDeployment(
            self,
            "DeploySqlScripts",
            sources=[
                s3deploy.Source.asset(
                    str(PROJECT_ROOT / "sql"),
                    exclude=["__pycache__", "*.pyc"],
                ),
                deploy_marker,
            ],
            destination_bucket=self.assets_bucket,
            destination_key_prefix="sql",
            prune=False,
        )

    def _create_or_import_metadata_table(self) -> dynamodb.ITable:
        """Create or import the DynamoDB metadata table."""
        table_name = f"{self.prefix}-pipeline-metadata"

        if dynamodb_table_exists(table_name):
            return dynamodb.Table.from_table_name(
                self, "MetadataTable", table_name,
            )

        return dynamodb.Table(
            self,
            "MetadataTable",
            table_name=table_name,
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

    def _ensure_glue_role(self) -> iam.IRole:
        """Ensure the Glue IAM role exists with correct trust policy and permissions.

        Uses CfnRole to always enforce the trust policy and managed policies,
        whether the role is new or already exists. CfnRole with the same name
        will update in-place, not fail on existence.
        """
        glue_role_name = f"{self.prefix}-glue-role"

        # CfnRole updates in-place if the role already exists (idempotent)
        cfn_role = iam.CfnRole(
            self,
            "GlueETLRoleCfn",
            role_name=glue_role_name,
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            managed_policy_arns=[
                "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
            ],
            policies=[
                iam.CfnRole.PolicyProperty(
                    policy_name="GlueETLInlinePolicy",
                    policy_document={
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject*", "s3:PutObject*",
                                    "s3:DeleteObject*", "s3:ListBucket",
                                    "s3:GetBucketLocation",
                                ],
                                "Resource": [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ] + [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}/*"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ],
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "dynamodb:GetItem", "dynamodb:PutItem",
                                    "dynamodb:UpdateItem", "dynamodb:Query",
                                ],
                                "Resource": f"arn:aws:dynamodb:*:*:table/{self.prefix}-pipeline-metadata",
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "logs:CreateLogGroup",
                                    "logs:CreateLogStream",
                                    "logs:PutLogEvents",
                                ],
                                "Resource": "arn:aws:logs:*:*:*",
                            },
                        ],
                    },
                ),
            ],
        )

        # Return an IRole reference for use by Glue jobs
        return iam.Role.from_role_arn(
            self, "GlueETLRole",
            role_arn=f"arn:aws:iam::{self.account}:role/{glue_role_name}",
        )

    def _ensure_redshift_role(self) -> iam.IRole:
        """Ensure the Redshift IAM role exists with S3 + Glue catalog permissions.

        Used by Redshift Serverless for:
        - Spectrum queries via Glue Data Catalog (external tables)
        - S3 read access for data lake buckets
        """
        role_name = f"{self.prefix}-redshift-spectrum-role"
        glue_db_prefix = self.prefix.replace("-", "_")

        iam.CfnRole(
            self,
            "RedshiftSpectrumRoleCfn",
            role_name=role_name,
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "redshift.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            policies=[
                iam.CfnRole.PolicyProperty(
                    policy_name="RedshiftSpectrumPolicy",
                    policy_document={
                        "Version": "2012-10-17",
                        "Statement": [
                            # S3 read access for Spectrum and COPY/UNLOAD
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject", "s3:GetObject*",
                                    "s3:ListBucket", "s3:GetBucketLocation",
                                ],
                                "Resource": [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ] + [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}/*"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ],
                            },
                            # Glue Data Catalog for Spectrum external tables
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "glue:GetTable", "glue:GetTables",
                                    "glue:GetDatabase", "glue:GetDatabases",
                                    "glue:GetPartition", "glue:GetPartitions",
                                ],
                                "Resource": [
                                    f"arn:aws:glue:{self.region}:{self.account}:catalog",
                                    f"arn:aws:glue:{self.region}:{self.account}:database/{glue_db_prefix}*",
                                    f"arn:aws:glue:{self.region}:{self.account}:table/{glue_db_prefix}*/*",
                                ],
                            },
                        ],
                    },
                ),
            ],
        )

        return iam.Role.from_role_arn(
            self, "RedshiftSpectrumRole",
            role_arn=f"arn:aws:iam::{self.account}:role/{role_name}",
        )

    def _create_pipeline_glue_jobs(self, pipeline_name: str):
        """Create Raw->Processed and Processed->Curated Glue jobs for a pipeline."""

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
