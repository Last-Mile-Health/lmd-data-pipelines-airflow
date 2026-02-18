"""
CDK Stack A: MWAA Foundation — S3 bucket, VPC, security group, execution role.

Deploy this FIRST so all resources exist before the MWAA environment
is created in Stack B (mwaa_env_stack.py).  This avoids CloudFormation
early-validation errors like AWS::EarlyValidation::ResourceExistenceCheck.
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class MwaaFoundationStack(Stack):
    """Foundation resources that must exist before the MWAA environment."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: str,
        project_code: str = "lmd-dp-airflow-v1",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.deploy_env = environment
        self.project_code = project_code
        self.prefix = f"{project_code}-{environment}"

        # ── S3 Bucket for MWAA (DAGs, plugins, requirements) ──
        self.mwaa_bucket = s3.Bucket(
            self,
            "MwaaBucket",
            bucket_name=f"{self.prefix}-mwaa",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── VPC ──
        self.vpc = ec2.Vpc(
            self,
            "MwaaVpc",
            vpc_name=f"{self.prefix}-mwaa-vpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # ── Security Group ──
        self.security_group = ec2.SecurityGroup(
            self,
            "MwaaSg",
            vpc=self.vpc,
            security_group_name=f"{self.prefix}-mwaa-sg",
            description="Security group for MWAA environment",
            allow_all_outbound=True,
        )
        self.security_group.add_ingress_rule(
            self.security_group,
            ec2.Port.all_traffic(),
            "Allow MWAA internal traffic",
        )

        # ── Upload DAGs, config, plugins, requirements to MWAA bucket ──
        self._deploy_mwaa_assets()

        # ── IAM Execution Role ──
        self.execution_role = self._create_execution_role()

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
        CfnOutput(self, "SecurityGroupId",
                  value=self.security_group.security_group_id,
                  export_name=f"{self.prefix}-mwaa-sg-id")

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

        requirements_content = (PROJECT_ROOT / "requirements.txt").read_text()
        self.deploy_requirements = s3deploy.BucketDeployment(
            self,
            "DeployRequirements",
            sources=[s3deploy.Source.data("requirements.txt", requirements_content)],
            destination_bucket=self.mwaa_bucket,
            prune=False,
        )

    def _create_execution_role(self) -> iam.Role:
        """Create IAM execution role for MWAA."""
        role = iam.Role(
            self,
            "MwaaExecutionRole",
            role_name=f"{self.prefix}-mwaa-execution-role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
        )

        # MWAA bucket access (DAGs, plugins, requirements)
        self.mwaa_bucket.grant_read(role)

        # Pipeline S3 buckets access
        for suffix in ["raw", "processed", "curated", "assets"]:
            bucket_arn = f"arn:aws:s3:::{self.prefix}-{suffix}"
            role.add_to_policy(iam.PolicyStatement(
                actions=["s3:GetObject*", "s3:PutObject*", "s3:DeleteObject*", "s3:ListBucket"],
                resources=[bucket_arn, f"{bucket_arn}/*"],
            ))

        # DynamoDB (watermark + metadata)
        role.add_to_policy(iam.PolicyStatement(
            actions=["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:Query"],
            resources=[f"arn:aws:dynamodb:*:*:table/{self.prefix}-pipeline-metadata"],
        ))

        # Glue (jobs + crawlers + catalog)
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:StartJobRun", "glue:GetJobRun", "glue:GetJob",
                "glue:CreateCrawler", "glue:UpdateCrawler", "glue:StartCrawler",
                "glue:GetCrawler", "glue:GetTable", "glue:GetTables",
            ],
            resources=["*"],
        ))

        # Redshift Data API
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "redshift-data:ExecuteStatement",
                "redshift-data:DescribeStatement",
                "redshift-data:GetStatementResult",
            ],
            resources=["*"],
        ))

        # Secrets Manager (Redshift, Kobo, DHIS2 credentials)
        role.add_to_policy(iam.PolicyStatement(
            actions=["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
            resources=[f"arn:aws:secretsmanager:*:*:secret:lmd-*"],
        ))

        # IAM PassRole (for Glue jobs/crawlers)
        role.add_to_policy(iam.PolicyStatement(
            actions=["iam:PassRole"],
            resources=[f"arn:aws:iam::*:role/{self.prefix}-glue-role"],
        ))

        # CloudWatch Logs (MWAA requirement)
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "logs:CreateLogStream", "logs:CreateLogGroup",
                "logs:PutLogEvents", "logs:GetLogEvents",
                "logs:GetLogRecord", "logs:GetLogGroupFields",
                "logs:GetQueryResults",
            ],
            resources=[f"arn:aws:logs:*:*:log-group:airflow-{self.prefix}-*"],
        ))

        # SQS + KMS (MWAA internal requirements)
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "sqs:ChangeMessageVisibility", "sqs:DeleteMessage",
                "sqs:GetQueueAttributes", "sqs:GetQueueUrl",
                "sqs:ReceiveMessage", "sqs:SendMessage",
            ],
            resources=[f"arn:aws:sqs:*:*:airflow-celery-*"],
        ))
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt",
            ],
            resources=["*"],
            conditions={"StringLike": {"kms:ViaService": "sqs.*.amazonaws.com"}},
        ))

        # SES (email notifications)
        role.add_to_policy(iam.PolicyStatement(
            actions=["ses:SendEmail", "ses:SendRawEmail"],
            resources=["*"],
        ))

        return role
