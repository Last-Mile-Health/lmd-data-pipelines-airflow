"""
CDK Stack A: MWAA Foundation — S3 bucket, VPC, security group, execution role.

Deploy this FIRST so all resources exist before the MWAA environment
is created in Stack B (mwaa_env_stack.py).

S3/VPC/SG use import-if-exists (safe). IAM roles use CfnRole to always
enforce trust policy and permissions on every deploy, preventing drift.
"""
from pathlib import Path

from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    Tags,
)
from constructs import Construct

from resource_lookup import bucket_exists, vpc_exists, security_group_exists

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class MwaaFoundationStack(Stack):
    """Foundation resources that must exist before the MWAA environment."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        project_code: str = "lmd-dp-airflow-v1",
        vpc_id: str = None,
        security_group_ids: list = None,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.deploy_env = environment
        self.project_code = project_code
        self.prefix = f"{project_code}-{environment}"

        # ── S3 Bucket for MWAA (DAGs, plugins, requirements) ──
        mwaa_bucket_name = f"{self.prefix}-mwaa"
        if bucket_exists(mwaa_bucket_name):
            self.mwaa_bucket = s3.Bucket.from_bucket_name(
                self, "MwaaBucket", mwaa_bucket_name,
            )
        else:
            self.mwaa_bucket = s3.Bucket(
                self,
                "MwaaBucket",
                bucket_name=mwaa_bucket_name,
                versioned=True,
                encryption=s3.BucketEncryption.S3_MANAGED,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                removal_policy=RemovalPolicy.RETAIN,
            )

        # ── VPC (import existing — never create new) ──
        if vpc_id:
            self.vpc = ec2.Vpc.from_lookup(
                self, "MwaaVpc", vpc_id=vpc_id,
            )
        else:
            vpc_name = f"{self.prefix}-mwaa-vpc"
            existing_vpc_id = vpc_exists(vpc_name)
            if existing_vpc_id:
                self.vpc = ec2.Vpc.from_lookup(
                    self, "MwaaVpc", vpc_id=existing_vpc_id,
                )
            else:
                raise ValueError(
                    f"No VPC found. Pass vpc_id parameter or ensure VPC '{vpc_name}' exists."
                )

        # ── Security Groups (import existing from VPC) ──
        if security_group_ids:
            self.security_group_ids = security_group_ids
            # Import first SG as the primary reference for CfnOutputs
            self.security_group = ec2.SecurityGroup.from_security_group_id(
                self, "MwaaSg", security_group_ids[0],
                allow_all_outbound=True,
            )
        else:
            sg_name = f"{self.prefix}-mwaa-sg"
            existing_sg_id = security_group_exists(sg_name, vpc_id=vpc_id)
            if existing_sg_id:
                self.security_group_ids = [existing_sg_id]
                self.security_group = ec2.SecurityGroup.from_security_group_id(
                    self, "MwaaSg", existing_sg_id,
                    allow_all_outbound=True,
                )
            else:
                sg = ec2.SecurityGroup(
                    self,
                    "MwaaSg",
                    vpc=self.vpc,
                    security_group_name=sg_name,
                    description="Security group for MWAA environment",
                    allow_all_outbound=True,
                )
                sg.add_ingress_rule(
                    sg, ec2.Port.all_traffic(),
                    "Allow MWAA internal traffic",
                )
                self.security_group = sg
                self.security_group_ids = [sg.security_group_id]

        # ── Upload DAGs, config, plugins, requirements to MWAA bucket ──
        self._deploy_mwaa_assets()

        # ── IAM Execution Role (always enforce via CfnRole) ──
        self.execution_role = self._ensure_execution_role()

        # ── Tags ──
        Tags.of(self).add("Project", project_code)
        Tags.of(self).add("ManagedBy", "CDK")
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "mwaa-foundation")

        # ── Outputs (consumed by MwaaEnvironmentStack) ──
        CfnOutput(self, "MwaaBucketArn",
                  value=self.mwaa_bucket.bucket_arn,
                  export_name=f"{self.prefix}-mwaa-bucket-arn")
        CfnOutput(self, "MwaaBucketName",
                  value=self.mwaa_bucket.bucket_name,
                  export_name=f"{self.prefix}-mwaa-bucket-name")
        CfnOutput(self, "ExecutionRoleArn",
                  value=self.execution_role.role_arn,
                  export_name=f"{self.prefix}-mwaa-execution-role-arn")
        CfnOutput(self, "SecurityGroupIds",
                  value=",".join(self.security_group_ids),
                  export_name=f"{self.prefix}-mwaa-sg-ids")

        private_subnets = self.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        )
        CfnOutput(self, "PrivateSubnet1",
                  value=private_subnets.subnet_ids[0],
                  export_name=f"{self.prefix}-mwaa-subnet-1")
        CfnOutput(self, "PrivateSubnet2",
                  value=private_subnets.subnet_ids[1],
                  export_name=f"{self.prefix}-mwaa-subnet-2")

    def _deploy_mwaa_assets(self):
        """Upload DAGs, config, and requirements to the MWAA S3 bucket."""
        self.deploy_dags = s3deploy.BucketDeployment(
            self,
            "DeployDags",
            sources=[s3deploy.Source.asset(
                str(PROJECT_ROOT / "dags"),
                exclude=["__pycache__", "*.pyc"],
            )],
            destination_bucket=self.mwaa_bucket,
            destination_key_prefix="dags",
            prune=False,
        )

        self.deploy_config = s3deploy.BucketDeployment(
            self,
            "DeployConfig",
            sources=[s3deploy.Source.asset(
                str(PROJECT_ROOT / "config"),
                exclude=["__pycache__", "*.pyc"],
            )],
            destination_bucket=self.mwaa_bucket,
            destination_key_prefix="dags/config",
            prune=False,
        )

        self.deploy_sql = s3deploy.BucketDeployment(
            self,
            "DeploySql",
            sources=[s3deploy.Source.asset(
                str(PROJECT_ROOT / "sql"),
                exclude=["__pycache__", "*.pyc"],
            )],
            destination_bucket=self.mwaa_bucket,
            destination_key_prefix="dags/sql",
            prune=False,
        )

        requirements_content = (PROJECT_ROOT / "requirements.txt").read_text()
        self.deploy_requirements = s3deploy.BucketDeployment(
            self,
            "DeployRequirements",
            sources=[s3deploy.Source.data("requirements.txt", requirements_content)],
            destination_bucket=self.mwaa_bucket,
            prune=False,
        )

    def _ensure_execution_role(self) -> iam.IRole:
        """Ensure the MWAA execution role has correct trust policy and permissions.

        Uses CfnRole which updates in-place if the role already exists.
        This prevents trust policy drift on every cdk deploy.
        """
        role_name = f"{self.prefix}-mwaa-execution-role"

        iam.CfnRole(
            self,
            "MwaaExecutionRoleCfn",
            role_name=role_name,
            assume_role_policy_document={
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": [
                                "airflow.amazonaws.com",
                                "airflow-env.amazonaws.com",
                            ]
                        },
                        "Action": "sts:AssumeRole",
                    }
                ],
            },
            policies=[
                iam.CfnRole.PolicyProperty(
                    policy_name="MwaaExecutionPolicy",
                    policy_document={
                        "Version": "2012-10-17",
                        "Statement": [
                            # MWAA bucket access
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject*", "s3:ListBucket",
                                    "s3:GetBucketPublicAccessBlock",
                                    "s3:GetBucketAcl",
                                    "s3:GetEncryptionConfiguration",
                                    "s3:GetBucketVersioning",
                                ],
                                "Resource": [
                                    f"arn:aws:s3:::{self.prefix}-mwaa",
                                    f"arn:aws:s3:::{self.prefix}-mwaa/*",
                                ],
                            },
                            # MWAA requires these account-level S3 checks for validation
                            {
                                "Effect": "Allow",
                                "Action": "s3:GetAccountPublicAccessBlock",
                                "Resource": "*",
                            },
                            # Pipeline S3 buckets
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:GetObject*", "s3:PutObject*",
                                    "s3:DeleteObject*", "s3:ListBucket",
                                ],
                                "Resource": [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ] + [
                                    f"arn:aws:s3:::{self.prefix}-{suffix}/*"
                                    for suffix in ["raw", "processed", "curated", "assets"]
                                ],
                            },
                            # DynamoDB
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "dynamodb:GetItem", "dynamodb:PutItem",
                                    "dynamodb:UpdateItem", "dynamodb:Query",
                                ],
                                "Resource": f"arn:aws:dynamodb:*:*:table/{self.prefix}-pipeline-metadata",
                            },
                            # Glue
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "glue:StartJobRun", "glue:GetJobRun", "glue:GetJob",
                                    "glue:CreateCrawler", "glue:UpdateCrawler", "glue:StartCrawler",
                                    "glue:GetCrawler", "glue:GetTable", "glue:GetTables",
                                    "glue:GetPartitions", "glue:GetPartition",
                                ],
                                "Resource": "*",
                            },
                            # Redshift Data API
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "redshift-data:ExecuteStatement",
                                    "redshift-data:DescribeStatement",
                                    "redshift-data:GetStatementResult",
                                ],
                                "Resource": "*",
                            },
                            # Secrets Manager
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "secretsmanager:GetSecretValue",
                                    "secretsmanager:DescribeSecret",
                                ],
                                "Resource": f"arn:aws:secretsmanager:*:*:secret:lmd-*",
                            },
                            # IAM PassRole for Glue
                            {
                                "Effect": "Allow",
                                "Action": "iam:PassRole",
                                "Resource": f"arn:aws:iam::*:role/{self.prefix}-glue-role",
                            },
                            # CloudWatch Logs
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "logs:CreateLogStream", "logs:CreateLogGroup",
                                    "logs:PutLogEvents", "logs:GetLogEvents",
                                    "logs:GetLogRecord", "logs:GetLogGroupFields",
                                    "logs:GetQueryResults",
                                ],
                                "Resource": f"arn:aws:logs:*:*:log-group:airflow-{self.prefix}-*",
                            },
                            # SQS (MWAA Celery)
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "sqs:ChangeMessageVisibility", "sqs:DeleteMessage",
                                    "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
                                    "sqs:ReceiveMessage", "sqs:SendMessage",
                                ],
                                "Resource": f"arn:aws:sqs:*:*:airflow-celery-*",
                            },
                            # KMS for SQS
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "kms:Decrypt", "kms:DescribeKey",
                                    "kms:GenerateDataKey*", "kms:Encrypt",
                                ],
                                "Resource": "*",
                                "Condition": {
                                    "StringLike": {
                                        "kms:ViaService": "sqs.*.amazonaws.com",
                                    }
                                },
                            },
                            # SES (email notifications)
                            {
                                "Effect": "Allow",
                                "Action": ["ses:SendEmail", "ses:SendRawEmail"],
                                "Resource": "*",
                            },
                        ],
                    },
                ),
            ],
        )

        # Return an IRole reference for use by MWAA environment
        return iam.Role.from_role_arn(
            self, "MwaaExecutionRole",
            role_arn=f"arn:aws:iam::{self.account}:role/{role_name}",
        )
