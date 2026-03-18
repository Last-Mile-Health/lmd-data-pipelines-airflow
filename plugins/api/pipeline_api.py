"""
Custom Airflow REST API plugin for React management app.

Extends Airflow's built-in API with pipeline-specific endpoints:
    GET  /api/v1/pipeline/health                         — health check
    GET  /api/v1/pipeline/configs                        — list all pipeline configs
    GET  /api/v1/pipeline/configs/{name}                 — full YAML config for one pipeline
    GET  /api/v1/pipeline/metadata/{name}                — watermark & run history from DynamoDB
    GET  /api/v1/pipeline/status                         — all pipelines with last Airflow run status
    GET  /api/v1/pipeline/status/{name}                  — single pipeline: config + run + metadata
    POST /api/v1/pipeline/trigger/{name}                 — trigger a DAG run (proxy, keeps creds server-side)
                                                           optional JSON body: {"conf": {...}}

    GET  /pipeline-docs                                  — Swagger UI (interactive docs)
    GET  /pipeline-docs/openapi.json                     — raw OpenAPI spec

The React app uses these alongside Airflow's native API for anything not covered here:
    GET  /api/v1/dags                   — list DAGs
    GET  /api/v1/dags/{id}/dagRuns      — list runs with pagination
"""
import os
import glob
import logging

import yaml
import boto3
from boto3.dynamodb.conditions import Key
from flask import Blueprint, jsonify, request
from airflow.plugins_manager import AirflowPlugin
from airflow.models import DagRun
from airflow.api.common.trigger_dag import trigger_dag

log = logging.getLogger(__name__)

pipeline_api = Blueprint(
    "pipeline_api",
    __name__,
    url_prefix="/api/v1/pipeline",
)

docs_api = Blueprint(
    "docs_api",
    __name__,
    url_prefix="/pipeline-docs",
)

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "config", "pipelines")


# ── CORS ──────────────────────────────────────────────────────────────────────

@pipeline_api.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@pipeline_api.route("/<path:path>", methods=["OPTIONS"])
def options_handler(path):
    return jsonify({}), 200


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dag_id(pipeline_name):
    return f"etl_{pipeline_name}"


def _load_configs():
    """Return list of parsed YAML config dicts for all non-template pipelines."""
    configs = []
    pattern = os.path.join(CONFIG_DIR, "*.yml")
    for filepath in sorted(glob.glob(pattern)):
        if os.path.basename(filepath).startswith("_"):
            continue
        with open(filepath) as f:
            config = yaml.safe_load(f)
        if config and "pipeline" in config:
            configs.append(config)
    return configs


def _get_latest_dag_run(pipeline_name):
    """Return serializable dict of the most recent DagRun, or None."""
    runs = DagRun.find(dag_id=_dag_id(pipeline_name)) or []
    if not runs:
        return None
    runs.sort(key=lambda r: r.execution_date, reverse=True)
    run = runs[0]
    return {
        "dag_id": run.dag_id,
        "run_id": run.run_id,
        "state": run.state,
        "execution_date": run.execution_date.isoformat() if run.execution_date else None,
        "start_date": run.start_date.isoformat() if run.start_date else None,
        "end_date": run.end_date.isoformat() if run.end_date else None,
        "run_type": run.run_type,
    }


def _get_dynamodb_metadata(pipeline_name):
    """Return (watermark, recent_executions) from DynamoDB. Returns ({}, []) on error."""
    env = os.getenv("LMD_ENVIRONMENT", "dev")
    project_code = os.getenv("LMD_PROJECT_CODE", "lmd-dp-airflow-v1")
    table_name = f"{project_code}-{env}-pipeline-metadata"
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(table_name)
        watermark = table.get_item(
            Key={"pipeline_name": pipeline_name, "execution_id": "watermark"}
        ).get("Item", {})
        response = table.query(
            KeyConditionExpression=Key("pipeline_name").eq(pipeline_name),
            ScanIndexForward=False,
            Limit=10,
        )
        return watermark, response.get("Items", [])
    except Exception as e:
        log.warning("DynamoDB fetch failed for %s: %s", pipeline_name, e)
        return {}, []


# ── Endpoints ─────────────────────────────────────────────────────────────────

@pipeline_api.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "lmd-data-pipelines-airflow"})


@pipeline_api.route("/configs", methods=["GET"])
def list_configs():
    """List all pipeline configs (summary fields only)."""
    pipelines = []
    for config in _load_configs():
        pipelines.append({
            "name": config["pipeline"]["name"],
            "description": config["pipeline"].get("description", ""),
            "source_type": config.get("source", {}).get("type", ""),
            "schedule": config.get("schedule", {}).get("cron", ""),
            "load_mode": config.get("redshift", {}).get("load_mode", ""),
            "ingestion_mode": config.get("ingestion", {}).get("mode", ""),
        })
    return jsonify({"pipelines": pipelines})


@pipeline_api.route("/configs/<pipeline_name>", methods=["GET"])
def get_config(pipeline_name):
    """Return the full YAML config for a single pipeline."""
    filepath = os.path.join(CONFIG_DIR, f"{pipeline_name}.yml")
    if not os.path.exists(filepath):
        return jsonify({"error": f"Pipeline '{pipeline_name}' not found"}), 404
    with open(filepath) as f:
        config = yaml.safe_load(f)
    return jsonify(config)


@pipeline_api.route("/metadata/<pipeline_name>", methods=["GET"])
def get_metadata(pipeline_name):
    """Return watermark and last 10 executions from DynamoDB."""
    watermark, executions = _get_dynamodb_metadata(pipeline_name)
    return jsonify({
        "pipeline_name": pipeline_name,
        "watermark": watermark,
        "recent_executions": executions,
    })


@pipeline_api.route("/status", methods=["GET"])
def list_statuses():
    """All pipelines with their last Airflow run state — single call for the React dashboard."""
    pipelines = []
    for config in _load_configs():
        name = config["pipeline"]["name"]
        pipelines.append({
            "pipeline_name": name,
            "description": config["pipeline"].get("description", ""),
            "source_type": config.get("source", {}).get("type", ""),
            "schedule": config.get("schedule", {}).get("cron", ""),
            "dag_id": _dag_id(name),
            "latest_run": _get_latest_dag_run(name),
        })
    return jsonify({"pipelines": pipelines})


@pipeline_api.route("/status/<pipeline_name>", methods=["GET"])
def get_status(pipeline_name):
    """Single pipeline: config summary + latest Airflow run + DynamoDB watermark/history."""
    filepath = os.path.join(CONFIG_DIR, f"{pipeline_name}.yml")
    if not os.path.exists(filepath):
        return jsonify({"error": f"Pipeline '{pipeline_name}' not found"}), 404
    with open(filepath) as f:
        config = yaml.safe_load(f)

    watermark, executions = _get_dynamodb_metadata(pipeline_name)

    return jsonify({
        "pipeline_name": pipeline_name,
        "description": config["pipeline"].get("description", ""),
        "source_type": config.get("source", {}).get("type", ""),
        "schedule": config.get("schedule", {}).get("cron", ""),
        "dag_id": _dag_id(pipeline_name),
        "latest_run": _get_latest_dag_run(pipeline_name),
        "watermark": watermark,
        "recent_executions": executions,
    })


@pipeline_api.route("/trigger/<pipeline_name>", methods=["POST"])
def trigger_pipeline(pipeline_name):
    """
    Trigger a DAG run for the given pipeline.
    Optional JSON body: {"conf": {"load_mode": "full", ...}}
    Returns 201 with run details on success.
    """
    filepath = os.path.join(CONFIG_DIR, f"{pipeline_name}.yml")
    if not os.path.exists(filepath):
        return jsonify({"error": f"Pipeline '{pipeline_name}' not found"}), 404

    body = request.get_json(silent=True) or {}
    run_conf = body.get("conf", {})
    dag_id = _dag_id(pipeline_name)

    try:
        run = trigger_dag(dag_id=dag_id, conf=run_conf, replace_microseconds=False)
        return jsonify({
            "triggered": True,
            "dag_id": dag_id,
            "run_id": run.run_id,
            "execution_date": run.execution_date.isoformat() if run.execution_date else None,
            "state": run.state,
        }), 201
    except Exception as e:
        log.error("Failed to trigger DAG %s: %s", dag_id, e)
        return jsonify({"triggered": False, "error": str(e)}), 500


# ── Docs (Swagger UI) ─────────────────────────────────────────────────────────

OPENAPI_SPEC = {
    "openapi": "3.0.3",
    "info": {
        "title": "LMD Data Pipelines API",
        "version": "1.0.0",
        "description": (
            "Custom pipeline management API for the LMD Data Pipelines Airflow platform. "
            "All endpoints require HTTP Basic Auth (Airflow username/password). "
            "Base URL: `http://localhost:8080`"
        ),
    },
    "servers": [{"url": "http://localhost:8080", "description": "Local Airflow"}],
    "components": {
        "securitySchemes": {
            "basicAuth": {"type": "http", "scheme": "basic"}
        },
        "schemas": {
            "DagRun": {
                "type": "object",
                "properties": {
                    "dag_id":        {"type": "string"},
                    "run_id":        {"type": "string"},
                    "state":         {"type": "string", "enum": ["queued", "running", "success", "failed"]},
                    "execution_date":{"type": "string", "format": "date-time"},
                    "start_date":    {"type": "string", "format": "date-time", "nullable": True},
                    "end_date":      {"type": "string", "format": "date-time", "nullable": True},
                    "run_type":      {"type": "string"},
                },
            },
            "PipelineSummary": {
                "type": "object",
                "properties": {
                    "pipeline_name": {"type": "string"},
                    "description":   {"type": "string"},
                    "source_type":   {"type": "string"},
                    "schedule":      {"type": "string", "description": "Cron expression"},
                    "dag_id":        {"type": "string"},
                    "latest_run":    {"$ref": "#/components/schemas/DagRun", "nullable": True},
                },
            },
            "PipelineDetail": {
                "allOf": [
                    {"$ref": "#/components/schemas/PipelineSummary"},
                    {
                        "type": "object",
                        "properties": {
                            "watermark":         {"type": "object"},
                            "recent_executions": {"type": "array", "items": {"type": "object"}},
                        },
                    },
                ]
            },
            "TriggerRequest": {
                "type": "object",
                "properties": {
                    "conf": {
                        "type": "object",
                        "description": "Optional run config passed to the DAG (e.g. `{\"load_mode\": \"full\"}`)",
                        "example": {"load_mode": "full"},
                    }
                },
            },
            "TriggerResponse": {
                "type": "object",
                "properties": {
                    "triggered":      {"type": "boolean"},
                    "dag_id":         {"type": "string"},
                    "run_id":         {"type": "string"},
                    "execution_date": {"type": "string", "format": "date-time"},
                    "state":          {"type": "string"},
                },
            },
            "Error": {
                "type": "object",
                "properties": {"error": {"type": "string"}},
            },
        },
    },
    "security": [{"basicAuth": []}],
    "paths": {
        "/api/v1/pipeline/health": {
            "get": {
                "tags": ["System"],
                "summary": "Health check",
                "responses": {
                    "200": {
                        "description": "Service is healthy",
                        "content": {"application/json": {"example": {"status": "healthy", "service": "lmd-data-pipelines-airflow"}}},
                    }
                },
            }
        },
        "/api/v1/pipeline/status": {
            "get": {
                "tags": ["Pipeline Status"],
                "summary": "All pipelines with last run state",
                "description": "Returns every pipeline config combined with its most recent Airflow DAG run. Use this for the React dashboard overview.",
                "responses": {
                    "200": {
                        "description": "List of pipeline statuses",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "pipelines": {
                                            "type": "array",
                                            "items": {"$ref": "#/components/schemas/PipelineSummary"},
                                        }
                                    },
                                }
                            }
                        },
                    }
                },
            }
        },
        "/api/v1/pipeline/status/{pipeline_name}": {
            "get": {
                "tags": ["Pipeline Status"],
                "summary": "Single pipeline — config + last run + DynamoDB metadata",
                "description": "All data needed for a pipeline detail page in one call.",
                "parameters": [
                    {"name": "pipeline_name", "in": "path", "required": True, "schema": {"type": "string"}, "example": "lib_ifi_pipeline"}
                ],
                "responses": {
                    "200": {
                        "description": "Pipeline detail",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/PipelineDetail"}}},
                    },
                    "404": {"description": "Pipeline not found", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                },
            }
        },
        "/api/v1/pipeline/trigger/{pipeline_name}": {
            "post": {
                "tags": ["Pipeline Control"],
                "summary": "Trigger a pipeline run",
                "description": "Starts a manual DAG run. Optionally pass `conf` to override run parameters (e.g. force a full reload).",
                "parameters": [
                    {"name": "pipeline_name", "in": "path", "required": True, "schema": {"type": "string"}, "example": "lib_ifi_pipeline"}
                ],
                "requestBody": {
                    "required": False,
                    "content": {"application/json": {"schema": {"$ref": "#/components/schemas/TriggerRequest"}}},
                },
                "responses": {
                    "201": {
                        "description": "Run triggered successfully",
                        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/TriggerResponse"}}},
                    },
                    "404": {"description": "Pipeline not found", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                    "500": {"description": "Trigger failed", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                },
            }
        },
        "/api/v1/pipeline/configs": {
            "get": {
                "tags": ["Pipeline Config"],
                "summary": "List all pipeline configs (summary fields)",
                "responses": {
                    "200": {
                        "description": "Config summaries",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "pipelines": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "name":           {"type": "string"},
                                                    "description":    {"type": "string"},
                                                    "source_type":    {"type": "string"},
                                                    "schedule":       {"type": "string"},
                                                    "load_mode":      {"type": "string"},
                                                    "ingestion_mode": {"type": "string"},
                                                },
                                            },
                                        }
                                    },
                                }
                            }
                        },
                    }
                },
            }
        },
        "/api/v1/pipeline/configs/{pipeline_name}": {
            "get": {
                "tags": ["Pipeline Config"],
                "summary": "Full YAML config for one pipeline",
                "parameters": [
                    {"name": "pipeline_name", "in": "path", "required": True, "schema": {"type": "string"}, "example": "lib_ifi_pipeline"}
                ],
                "responses": {
                    "200": {"description": "Full config object"},
                    "404": {"description": "Pipeline not found", "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}},
                },
            }
        },
        "/api/v1/pipeline/metadata/{pipeline_name}": {
            "get": {
                "tags": ["Pipeline Metadata"],
                "summary": "Watermark and last 10 executions from DynamoDB",
                "parameters": [
                    {"name": "pipeline_name", "in": "path", "required": True, "schema": {"type": "string"}, "example": "lib_ifi_pipeline"}
                ],
                "responses": {
                    "200": {
                        "description": "DynamoDB metadata",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "pipeline_name":     {"type": "string"},
                                        "watermark":         {"type": "object"},
                                        "recent_executions": {"type": "array", "items": {"type": "object"}},
                                    },
                                }
                            }
                        },
                    }
                },
            }
        },
    },
}


@docs_api.route("", methods=["GET"])
@docs_api.route("/", methods=["GET"])
def swagger_ui():
    """
    Serve Swagger UI with two specs selectable via dropdown:
      1. LMD Pipeline API  — our custom endpoints
      2. Airflow REST API  — all built-in Airflow endpoints
    """
    html = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>LMD Data Pipelines — API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    body { margin: 0; }
    .topbar-wrapper img { display: none; }
    .topbar-wrapper::after {
      content: "LMD Data Pipelines API";
      color: white;
      font-size: 1.2em;
      font-weight: bold;
    }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      urls: [
        { url: "/pipeline-docs/openapi.json", name: "LMD Pipeline API (custom)" },
        { url: "/api/v1/openapi.json",         name: "Airflow REST API (built-in)" },
      ],
      "urls.primaryName": "LMD Pipeline API (custom)",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout",
      deepLinking: true,
      tryItOutEnabled: true,
    });
  </script>
</body>
</html>"""
    from flask import Response
    return Response(html, mimetype="text/html")


@docs_api.route("/openapi.json", methods=["GET"])
def openapi_spec():
    """Serve the raw OpenAPI 3.0 spec as JSON."""
    return jsonify(OPENAPI_SPEC)


class PipelineAPIPlugin(AirflowPlugin):
    name = "pipeline_api"
    flask_blueprints = [pipeline_api, docs_api]
