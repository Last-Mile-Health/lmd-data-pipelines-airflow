"""
CDK App entry point for Airflow pipeline infrastructure.

Usage:
    cdk deploy --context env=dev                          # Deploy Glue jobs + S3 + DynamoDB
    cdk deploy --context env=dev --all                    # Deploy everything including MWAA
    cdk deploy {PROJECT_CODE}-{env}-mwaa --context env=dev  # Deploy only MWAA
"""
import os
import aws_cdk as cdk
from airflow_stack import AirflowPipelineStack
from mwaa_stack import MwaaStack


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

# Stack 1: Pipeline resources (S3, DynamoDB, Glue jobs, IAM)
AirflowPipelineStack(
    app,
    f"{PROJECT_CODE}-{environment}",
    environment=environment,
    project_code=PROJECT_CODE,
    pipeline_names=PIPELINE_NAMES,
    env=cdk_env,
)

# Stack 2: MWAA environment (VPC, IAM, MWAA)
MwaaStack(
    app,
    f"{PROJECT_CODE}-{environment}-mwaa",
    environment=environment,
    project_code=PROJECT_CODE,
    env=cdk_env,
)

app.synth()
