"""Finite maintenance workflow for the continuously ingested lakehouse."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

with DAG(
    dag_id="feedback_lakehouse_maintenance",
    description="Enrich, compact, test and publish customer-feedback lakehouse data",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["feedback", "iceberg", "data-quality"],
) as dag:
    enrich_aspects = BashOperator(
        task_id="enrich_aspects",
        bash_command="spark-submit /opt/lakehouse/jobs/spark_enrichment_job.py",
    )
    compact_iceberg = BashOperator(
        task_id="compact_iceberg",
        bash_command=(
            'spark-sql -e "CALL lakehouse.system.rewrite_data_files('
            "table => 'silver.feedback', options => map('target-file-size-bytes','134217728'))\""
        ),
    )
    build_marts = BashOperator(
        task_id="build_marts",
        bash_command="cd /opt/lakehouse/dbt && dbt build --profiles-dir .",
    )
    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command="feedback-lakehouse report --database /opt/lakehouse/build/lakehouse.duckdb",
    )

    enrich_aspects >> compact_iceberg >> build_marts >> quality_gate
