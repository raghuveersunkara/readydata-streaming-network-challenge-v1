"""Load every DAG file the way Airflow does. A syntax error or bad import in a DAG file
passes the unit tests of its helpers but hides the DAG from Airflow; this catches it.
Also pins the brief's contract: the three DAGs exist and are manual-only."""
import os

import pytest

try:
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # older Airflow layout
    from airflow.models.dagbag import DagBag

from lakehouse_ops.export import list_reports

DAGS_FOLDER = os.getenv("SOLUTION_DAGS_FOLDER", "/opt/airflow/dags")
SQL_ROOT = os.getenv("READYDATA_SQL_ROOT", "/opt/airflow/sql")
EXPECTED_DAGS = {"manual_sql_to_csv", "lakehouse_quality", "iceberg_maintenance"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_FOLDER)  # example DAGs are off via AIRFLOW__CORE__LOAD_EXAMPLES


def test_every_dag_file_imports(dagbag):
    assert dagbag.import_errors == {}


def test_the_three_required_dags_exist(dagbag):
    assert EXPECTED_DAGS <= set(dagbag.dag_ids)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAGS))
def test_dags_are_manual_only_and_do_not_backfill(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    # schedule=None gives a NullTimetable (airflow.sdk's class in Airflow 3, so compare by name).
    assert type(dag.timetable).__name__ == "NullTimetable", f"{dag_id} must only run when triggered"
    assert dag.catchup is False
    assert dag.tasks, f"{dag_id} has no tasks"


def test_export_dag_offers_exactly_the_checked_in_reports(dagbag):
    report_param = dagbag.get_dag("manual_sql_to_csv").params.get_param("report")
    assert report_param.schema["enum"] == list_reports(SQL_ROOT)


def test_maintenance_defaults_to_plan_only(dagbag):
    assert dagbag.get_dag("iceberg_maintenance").params["mode"] == "plan"
