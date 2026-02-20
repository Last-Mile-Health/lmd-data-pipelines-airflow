"""
Synth-time resource existence checks using boto3.

Each function returns True if the resource already exists in AWS,
so the calling stack can decide to import it instead of creating it.
Uses best-effort lookups — if AWS credentials are unavailable (e.g.
local-only synth), defaults to False (create as normal).
"""
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError


def _get_region():
    """Best-effort region detection."""
    session = boto3.session.Session()
    return session.region_name or "us-east-1"


def bucket_exists(bucket_name: str) -> bool:
    """Check if an S3 bucket already exists."""
    try:
        s3 = boto3.client("s3", region_name=_get_region())
        s3.head_bucket(Bucket=bucket_name)
        print(f"[lookup] S3 bucket '{bucket_name}' already exists — will import")
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            return False
        if code in ("403", "AccessDenied"):
            # Bucket exists but we don't own it, or no permission — treat as exists
            print(f"[lookup] S3 bucket '{bucket_name}' exists (403) — will import")
            return True
        return False
    except (NoCredentialsError, BotoCoreError):
        return False


def role_exists(role_name: str) -> bool:
    """Check if an IAM role already exists."""
    try:
        iam = boto3.client("iam", region_name=_get_region())
        iam.get_role(RoleName=role_name)
        print(f"[lookup] IAM role '{role_name}' already exists — will import")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            return False
        return False
    except (NoCredentialsError, BotoCoreError):
        return False


def dynamodb_table_exists(table_name: str) -> bool:
    """Check if a DynamoDB table already exists."""
    try:
        ddb = boto3.client("dynamodb", region_name=_get_region())
        resp = ddb.describe_table(TableName=table_name)
        status = resp["Table"]["TableStatus"]
        if status in ("ACTIVE", "UPDATING"):
            print(f"[lookup] DynamoDB table '{table_name}' already exists — will import")
            return True
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return False
        return False
    except (NoCredentialsError, BotoCoreError):
        return False


def vpc_exists(vpc_name: str) -> Optional[str]:
    """Check if a VPC with the given Name tag exists. Returns VPC ID or None."""
    try:
        ec2 = boto3.client("ec2", region_name=_get_region())
        resp = ec2.describe_vpcs(
            Filters=[{"Name": "tag:Name", "Values": [vpc_name]}]
        )
        vpcs = resp.get("Vpcs", [])
        if vpcs:
            vpc_id = vpcs[0]["VpcId"]
            print(f"[lookup] VPC '{vpc_name}' already exists ({vpc_id}) — will import")
            return vpc_id
        return None
    except (ClientError, NoCredentialsError, BotoCoreError):
        return None


def security_group_exists(sg_name: str, vpc_id: str = None) -> Optional[str]:
    """Check if a security group exists. Returns SG ID or None."""
    try:
        ec2 = boto3.client("ec2", region_name=_get_region())
        filters = [{"Name": "group-name", "Values": [sg_name]}]
        if vpc_id:
            filters.append({"Name": "vpc-id", "Values": [vpc_id]})
        resp = ec2.describe_security_groups(Filters=filters)
        sgs = resp.get("SecurityGroups", [])
        if sgs:
            sg_id = sgs[0]["GroupId"]
            print(f"[lookup] Security group '{sg_name}' already exists ({sg_id}) — will import")
            return sg_id
        return None
    except (ClientError, NoCredentialsError, BotoCoreError):
        return None
