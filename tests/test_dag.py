"""The maintenance DAG parses and chains its tasks in the documented order.

Airflow is the runtime the DAG is deployed into, not a project dependency, so
this module only runs where Airflow is installed. CI installs it with the
official constraints file in its own job; locally the module is skipped.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow is installed only in the CI dag job")

DAGS_DIR = str(Path(__file__).resolve().parents[1] / "dags")
EXPECTED_ORDER = ["enrich_aspects", "compact_iceberg", "build_marts", "quality_gate"]


def load_dag():
    sys.path.insert(0, DAGS_DIR)
    import feedback_lakehouse_maintenance as module

    return module.dag


def test_dag_imports_and_chains_the_maintenance_steps_in_order() -> None:
    dag = load_dag()
    assert dag.dag_id == "feedback_lakehouse_maintenance"
    assert sorted(dag.task_ids) == sorted(EXPECTED_ORDER)
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:], strict=True):
        assert downstream in dag.get_task(upstream).downstream_task_ids, (upstream, downstream)
    assert dag.get_task("enrich_aspects").upstream_task_ids == set()
    assert dag.get_task("quality_gate").downstream_task_ids == set()


def test_dag_runs_finite_jobs_only_and_gates_on_the_report_exit_code() -> None:
    dag = load_dag()
    assert dag.timetable.summary == "15 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    # The streaming job is deliberately absent: Airflow runs finite work, the stream runs alone.
    commands = {task.task_id: task.bash_command for task in dag.tasks}
    assert not any("spark_streaming_job" in command for command in commands.values())
    # The gate is `report`, which exits 1 on a failed invariant; a print-only gate cannot gate.
    assert commands["quality_gate"].startswith("feedback-lakehouse report")
