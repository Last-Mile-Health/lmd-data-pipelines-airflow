"""
Custom Airflow REST API plugin for React management app.

Extends Airflow's built-in API with pipeline-specific endpoints:
    GET  /api/v1/pipeline/metadata/{pipeline_name}   — watermark & run history
    GET  /api/v1/pipeline/configs                     — list all pipeline configs
    GET  /api/v1/pipeline/configs/{pipeline_name}     — get single pipeline config
    GET  /api/v1/pipeline/health                      — overall pipeline health

The React app uses these alongside Airflow's native API:
    GET  /api/v1/dags                   — list DAGs
    POST /api/v1/dags/{id}/dagRuns      — trigger a run
    GET  /api/v1/dags/{id}/dagRuns      — list runs
"""
import os
import glob
import json

import yaml
import boto3
from flask import Blueprint, jsonify, request
from airflow.plugins_manager import AirflowPlugin


pipeline_api = Blueprint(
    "pipeline_api",
    __name__,
    url_prefix="/api/v1/pipeline",
)

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "config", "pipelines")


@pipeline_api.route("/health", methods=["GET"])
def health():
    """Overall pipeline system health check."""
    return jsonify({"status": "healthy", "service": "lmd-data-pipelines-airflow"})


@pipeline_api.route("/configs", methods=["GET"])
def list_configs():
    """Return all pipeline YAML configs for the React config editor."""
    configs = []
    pattern = os.path.join(CONFIG_DIR, "*.yml")
    for filepath in sorted(glob.glob(pattern)):
        filename = os.path.basename(filepath)
        if filename.startswith("_"):
            continue
        with open(filepath) as f:
            config = yaml.safe_load(f)
            if config and "pipeline" in config:
                configs.append({
                    "name": config["pipeline"]["name"],
                    "description": config["pipeline"].get("description", ""),
                    "source_type": config.get("source", {}).get("type", ""),
                    "schedule": config.get("schedule", {}).get("cron", ""),
                    "load_mode": config.get("redshift", {}).get("load_mode", ""),
                    "ingestion_mode": config.get("ingestion", {}).get("mode", ""),
                })
    return jsonify({"pipelines": configs})


@pipeline_api.route("/configs/<pipeline_name>", methods=["GET"])
def get_config(pipeline_name):
    """Return full YAML config for a single pipeline."""
    filepath = os.path.join(CONFIG_DIR, f"{pipeline_name}.yml")
    if not os.path.exists(filepath):
        return jsonify({"error": f"Pipeline '{pipeline_name}' not found"}), 404
    with open(filepath) as f:
        config = yaml.safe_load(f)
    return jsonify(config)


@pipeline_api.route("/metadata/<pipeline_name>", methods=["GET"])
def get_metadata(pipeline_name):
    """Return watermark, last run, row counts from DynamoDB."""
    env = os.getenv("LMD_ENVIRONMENT", "dev")
    project_code = os.getenv("LMD_PROJECT_CODE", "lmd-dp-airflow-v1")
    table_name = f"{project_code}-{env}-pipeline-metadata"

    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)

        # Get watermark record
        watermark = table.get_item(
            Key={"pipeline_name": pipeline_name, "execution_id": "watermark"}
        ).get("Item", {})

        # Get recent executions (last 10)
        response = table.query(
            KeyConditionExpression="pipeline_name = :pn",
            ExpressionAttributeValues={":pn": pipeline_name},
            ScanIndexForward=False,
            Limit=10,
        )
        executions = response.get("Items", [])

        return jsonify({
            "pipeline_name": pipeline_name,
            "watermark": watermark,
            "recent_executions": executions,
        })
    except Exception as e:
        return jsonify({
            "pipeline_name": pipeline_name,
            "error": str(e),
            "watermark": {},
            "recent_executions": [],
        })


class PipelineAPIPlugin(AirflowPlugin):
    name = "pipeline_api"
    flask_blueprints = [pipeline_api]
