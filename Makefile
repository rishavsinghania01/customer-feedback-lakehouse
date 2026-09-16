.PHONY: install test lint demo dbt api dashboard docker-build terraform-check clean

PYTHON ?= python3
DB_PATH ?= build/lakehouse.duckdb

install:
	$(PYTHON) -m pip install -e ".[dev,dashboard]"

test:
	$(PYTHON) -m pytest --cov=feedback_lakehouse --cov-report=term-missing

lint:
	$(PYTHON) -m ruff check .

demo:
	$(PYTHON) -m feedback_lakehouse.cli demo --database $(DB_PATH)

dbt:
	cd dbt && LAKEHOUSE_DB=../$(DB_PATH) dbt build --profiles-dir .

api:
	uvicorn feedback_lakehouse.api:app --reload --port 8000

dashboard:
	LAKEHOUSE_DB=$(DB_PATH) streamlit run app/dashboard.py

docker-build:
	docker build -t customer-feedback-lakehouse:local .

terraform-check:
	terraform -chdir=infra/terraform fmt -check -recursive
	terraform -chdir=infra/terraform init -backend=false
	terraform -chdir=infra/terraform validate

clean:
	rm -rf build runtime .pytest_cache .ruff_cache .coverage htmlcov dbt/logs dbt/target
