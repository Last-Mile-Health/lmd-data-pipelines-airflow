"""
CDK App entry point for Airflow pipeline infrastructure.

Stacks (deploy order):
    1. AirflowPipelineStack  — S3 data buckets, DynamoDB, Glue jobs
    2. MwaaFoundationStack   — MWAA S3 bucket, VPC, security group, execution role
    3. MwaaEnvironmentStack  — MWAA environment (depends on #2)

Usage:
    cdk deploy --context env=dev                                           # Deploy all stacks
    cdk deploy {PROJECT_CODE}-{env} --context env=dev                      # Pipeline resources only
    cdk deploy {PROJECT_CODE}-{env}-mwaa-foundation --context env=dev      # MWAA foundation only
    cdk deploy {PROJECT_CODE}-{env}-mwaa --context env=dev                 # MWAA environment only
"""
import os
import aws_cdk as cdk
from airflow_stack import AirflowPipelineStack
from mwaa_foundation_stack import MwaaFoundationStack
from mwaa_env_stack import MwaaEnvironmentStack


app = cdk.App()

# Read environment from context or env var
environment = app.node.try_get_context("env") or os.getenv("LMD_ENVIRONMENT", "dev")

# AWS account/region mapping
ACCOUNTS = {
    "dev": {"account": "002190277880", "region": "us-east-1"},
    "test": {"account": "576140831944", "region": "us-east-1"},
    "prod": {"account": "301323023124", "region": "us-east-1"},
}

account_config = ACCOUNTS.get(environment, ACCOUNTS["dev"])

# Pipelines to create Glue jobs for
PIPELINE_NAMES = ["lib_ifi_pipeline", "lib_dhis2_pipeline"]  # Add more as you create pipeline YAMLs

PROJECT_CODE = "lmd-dp-airflow-v1"

cdk_env = cdk.Environment(
    account=account_config["account"],
    region=account_config["region"],
)

# Stack 1: Pipeline resources (S3 data buckets, DynamoDB, Glue jobs, IAM)
AirflowPipelineStack(
    app,
    f"{PROJECT_CODE}-{environment}",
    environment=environment,
    project_code=PROJECT_CODE,
    pipeline_names=PIPELINE_NAMES,
    env=cdk_env,
)

# Existing VPC and security groups per environment (reuse existing MWAA networking)
VPC_IDS = {
    "dev": "vpc-06e2afb2131117b11",   # MWAAEnvironment
}
SECURITY_GROUP_IDS = {
    "dev": ["sg-02ac083456cb37e4e", "sg-055a2707b998827ba"],
}

# Stack 2: MWAA foundation (S3 MWAA bucket, VPC, security group, execution role)
# Deploy this FIRST — ensures bucket/role/subnets exist before MWAA validates them.
foundation = MwaaFoundationStack(
    app,
    f"{PROJECT_CODE}-{environment}-mwaa-foundation",
    environment=environment,
    project_code=PROJECT_CODE,
    vpc_id=VPC_IDS.get(environment),
    security_group_ids=SECURITY_GROUP_IDS.get(environment),
    env=cdk_env,
)

# Stack 3: MWAA environment (depends on foundation stack)
MwaaEnvironmentStack(
    app,
    f"{PROJECT_CODE}-{environment}-mwaa",
    foundation=foundation,
    environment=environment,
    project_code=PROJECT_CODE,
    env=cdk_env,
)

app.synth()
