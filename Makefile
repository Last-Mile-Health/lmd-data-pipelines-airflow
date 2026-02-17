.PHONY: up down build restart logs shell init test lint clean

# ── Docker Commands ──
up:
	docker compose up -d

up-local:
	docker compose --profile local up -d

down:
	docker compose down

build:
	docker compose build --no-cache

restart:
	docker compose down
	docker compose restart airflow-webserver airflow-scheduler

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

logs-scheduler:
	docker compose logs -f airflow-scheduler

logs-webserver:
	docker compose logs -f airflow-webserver

# ── Development ──
shell:
	docker compose exec airflow-webserver bash

init:
	cp -n .env.example .env || true
	docker compose up airflow-init

# ── Testing ──
test:
	docker compose exec airflow-webserver pytest /opt/airflow/tests/ -v

test-local:
	python -m pytest tests/ -v

# ── Linting ──
lint:
	python -m flake8 dags/ plugins/ glue_jobs/ --max-line-length=120
	python -m flake8 tests/ --max-line-length=120

# ── Cleanup ──
clean:
	docker compose down -v
	rm -rf logs/__pycache__ dags/__pycache__
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ── Deploy Glue Scripts to S3 ──
deploy-glue:
	@echo "Uploading Glue scripts to S3..."
	aws s3 sync glue_jobs/ s3://$${LMD_PROJECT_CODE}-$${LMD_ENVIRONMENT}-assets/glue_jobs/ --exclude "__pycache__/*" --exclude "*.pyc"
	@echo "Done."

# ── Deploy SQL to S3 ──
deploy-sql:
	@echo "Uploading SQL scripts to S3..."
	aws s3 sync sql/ s3://$${LMD_PROJECT_CODE}-$${LMD_ENVIRONMENT}-assets/sql/ --exclude "__pycache__/*"
	@echo "Done."

deploy-assets: deploy-glue deploy-sql

# ── Deploy to MWAA ──
# Syncs dags/, plugins/, config/, and requirements.txt to the MWAA S3 bucket
MWAA_BUCKET = $${LMD_PROJECT_CODE}-$${LMD_ENVIRONMENT}-mwaa

deploy-mwaa: deploy-assets
	@echo "Packaging plugins..."
	cd plugins && zip -r ../plugins.zip . -x "__pycache__/*" "*.pyc" && cd ..
	@echo "Uploading DAGs to s3://$(MWAA_BUCKET)/dags/..."
	aws s3 sync dags/ s3://$(MWAA_BUCKET)/dags/ \
		--exclude "__pycache__/*" --exclude "*.pyc" --exclude ".DS_Store"
	@echo "Uploading config to s3://$(MWAA_BUCKET)/dags/config/..."
	aws s3 sync config/ s3://$(MWAA_BUCKET)/dags/config/ \
		--exclude "__pycache__/*"
	@echo "Uploading plugins.zip..."
	aws s3 cp plugins.zip s3://$(MWAA_BUCKET)/plugins.zip
	@echo "Uploading requirements.txt..."
	aws s3 cp requirements.txt s3://$(MWAA_BUCKET)/requirements.txt
	rm -f plugins.zip
	@echo "MWAA deployment complete. MWAA will pick up changes within ~30s."

# Deploy MWAA infrastructure via CDK
deploy-mwaa-infra:
	cd infrastructure && cdk deploy $${LMD_PROJECT_CODE}-$${LMD_ENVIRONMENT}-mwaa --context env=$${LMD_ENVIRONMENT}

# Deploy everything (infra + code)
deploy-all:
	cd infrastructure && cdk deploy --all --context env=$${LMD_ENVIRONMENT}
	$(MAKE) deploy-mwaa
