"""
CDK Stack B: MWAA Environment — depends on MwaaFoundationStack.

Deploy this AFTER mwaa_foundation_stack so the S3 bucket, execution role,
VPC subnets, and security group already exist.  This prevents CloudFormation
early-validation errors (AWS::EarlyValidation::ResourceExistenceCheck).
"""
from aws_cdk import (
    Stack,
    CfnOutput,
    aws_ec2 as ec2,
    aws_mwaa as mwaa,
    Tags,
)
from constructs import Construct

from mwaa_foundation_stack import MwaaFoundationStack


class MwaaEnvironmentStack(Stack):
    """MWAA environment that consumes foundation resources from Stack A."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        foundation: MwaaFoundationStack,
        environment: str,
        project_code: str = "lmd-dp-airflow-v1",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.deploy_env = environment
        self.project_code = project_code
        self.prefix = f"{project_code}-{environment}"

        # Resolve foundation resources
        bucket_arn = foundation.mwaa_bucket.bucket_arn
        execution_role_arn = foundation.execution_role.role_arn
        sg_id = foundation.security_group.security_group_id

        private_subnets = foundation.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        )

        # ── MWAA Environment ──
        self.mwaa_env = mwaa.CfnEnvironment(
            self,
            "MwaaEnvironment",
            name=f"{self.prefix}-mwaa",
            airflow_version="2.9.2",
            environment_class="mw1.small",
            max_workers=5,
            min_workers=1,
            schedulers=2,
            source_bucket_arn=bucket_arn,
            dag_s3_path="dags",
            requirements_s3_path="requirements.txt",
            execution_role_arn=execution_role_arn,
            webserver_access_mode="PUBLIC_ONLY",
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                subnet_ids=private_subnets.subnet_ids[:2],
                security_group_ids=[sg_id],
            ),
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO",
                ),
                scheduler_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO",
                ),
                task_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO",
                ),
                webserver_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="WARNING",
                ),
                worker_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True, log_level="INFO",
                ),
            ),
            airflow_configuration_options={
                "core.default_timezone": "utc",
                "core.load_examples": "false",
                "core.dagbag_import_timeout": "120",
                "smtp.smtp_port": "587",
                "smtp.smtp_starttls": "true",
                "smtp.smtp_mail_from": "lmdadmin@lastmilehealth.org",
            },
        )

        # Ensure MWAA waits for all S3 uploads from the foundation stack
        self.mwaa_env.node.add_dependency(foundation.deploy_dags)
        self.mwaa_env.node.add_dependency(foundation.deploy_config)
        self.mwaa_env.node.add_dependency(foundation.deploy_requirements)

        # ── Tags ──
        Tags.of(self).add("Project", project_code)
        Tags.of(self).add("ManagedBy", "CDK")
        Tags.of(self).add("Environment", environment)
        Tags.of(self).add("Component", "mwaa")

        # ── Outputs ──
        CfnOutput(self, "MwaaEnvironmentName", value=self.mwaa_env.name)
        CfnOutput(self, "MwaaWebserverUrl",
                  value=f"https://{self.mwaa_env.attr_webserver_url}")
