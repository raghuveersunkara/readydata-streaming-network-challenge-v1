"""lakehouse_quality: check reference completeness and relationships, and reconcile
captured event quality. Read-only.

Tasks:
- reference_checks: reference row counts against the supplied manifest.json files,
  duplicate keys, orphaned relationships, devices without an upstream.
- event_checks: events from unknown devices or with the wrong site, replay and
  source duplicates, reject rate, ingestion freshness, source clock skew, silent devices.
- summarize (always runs): prints every finding as a table and fails the run only if
  a "fail" finding fired. "warn" findings are investigation items; the run succeeds.

Findings are in the task logs and in XCom (return values). See lakehouse_ops/quality.py
for each check's threshold and meaning.
"""
import json
import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

from lakehouse_ops.quality import evaluate, event_checks, manifest_counts, reference_checks
from lakehouse_ops.quality import summarize as combine_findings
from lakehouse_ops.trino_client import CATALOG, SCHEMA, fetch_all

REFERENCE_ROOT = os.getenv("READYDATA_REFERENCE_ROOT", "/opt/airflow/reference-data")
REFERENCE_VERSION = os.getenv("READYDATA_REFERENCE_VERSION", "v1")
QUALIFIED_SCHEMA = f"{CATALOG}.{SCHEMA}"


def _manifest_counts() -> dict:
    def load(path):
        with open(os.path.join(REFERENCE_ROOT, path, REFERENCE_VERSION, "manifest.json")) as f:
            return json.load(f)["counts"]
    return manifest_counts(load("inventory"), load("census"))


def _run(checks) -> list:
    findings = []
    for check in checks:
        try:
            value = fetch_all(check.sql)[0][0]
            finding = evaluate(check, None if value is None else float(value))
        except Exception as e:  # one broken check must not hide the others
            finding = evaluate(check, None, error=str(e)[:300])
        print(f"{finding['status'].upper():5} {finding['name']}: {finding['value']}")
        findings.append(finding)
    return findings


@dag(
    dag_id="lakehouse_quality",
    description="Reference completeness/relationships and event quality reconciliation",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["readydata", "quality"],
    params={
        "window_hours": Param(6, type="integer", minimum=1, maximum=48,
                              description="Event checks cover records that arrived in the last N hours"),
    },
    doc_md=__doc__,
)
def lakehouse_quality():

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def reference() -> list:
        return _run(reference_checks(QUALIFIED_SCHEMA, _manifest_counts()))

    @task(retries=1, retry_delay=timedelta(seconds=30))
    def events() -> list:
        window = get_current_context()["params"]["window_hours"]
        return _run(event_checks(QUALIFIED_SCHEMA, window))

    @task(trigger_rule="all_done")
    def summarize(reference_findings: list, event_findings: list) -> dict:
        result = combine_findings(reference_findings, event_findings)
        print(f"\n{'status':<7}{'category':<11}{'check':<34}{'value':>14}{'threshold':>11}  description")
        for f in result["findings"]:
            value = "ERROR" if f["error"] else ("-" if f["value"] is None else f"{f['value']:.3f}")
            print(f"{f['status']:<7}{f['category']:<11}{f['name']:<34}{value:>14}{f['threshold']:>11}  "
                  f"{f['error'] or f['description']}")
        print(f"\nSummary: {result['summary']}  missing check groups: {result['missing_groups'] or 'none'}")
        if result["failed"]:
            raise AirflowFailException(f"Quality failures: {result['summary']}, "
                                       f"missing groups: {result['missing_groups']} (see the table above)")
        return result

    summarize(reference(), events())


lakehouse_quality()
