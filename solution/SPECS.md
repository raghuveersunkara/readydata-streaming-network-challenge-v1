# Specifications: review guide and index

The solution is reviewed spec-first. Each module has a `SPEC.md` saying what it
must do, as numbered requirements, and **how each requirement is verified**. A
reviewer approves a module by checking its spec, not by reading its code.

## How to review

1. **Read the spec's Purpose and Non-goals.** Is this the right thing to build?
2. **Read the requirements.** Is each one correct, complete and unambiguous?
   Are any missing?
3. **Check the traceability.** Each requirement names how it's verified:
   - **a test name** (`test_…`): an automated test in the module's `tests/`
     folder proves it. Run the suites (below) and check they pass;
   - **Live**: checked by running the real stack (the date and evidence are
     given);
   - **Review**: a small, local fact that's quicker to read than to test (a
     configuration value, for example). The spec says exactly where to look.
4. **Read the Known limitations.** Are the accepted trade-offs acceptable?
5. **Fill in the Sign-off table** at the end of the spec: approve, or request
   changes with notes.

Code needs reading only where a requirement is marked **Review**, or when a
test's name and the requirement it verifies don't obviously match.

**Conventions:**

- Requirement IDs are stable (`ING-10`, `EXP-06`, …), so reviews and later
  changes can refer to them.
- **MUST** is a hard requirement.
- Changes follow the spec first: update the requirement, then the test, then
  the code.

## Index

| Spec | Scope | Requirements | Tests | Sign-off |
| --- | --- | --- | --- | --- |
| [ingestion/SPEC.md](ingestion/SPEC.md) | Kafka → Iceberg streaming ingestion and table setup | ING-01…22 | `ingestion/tests` | ☐ |
| [ingestion/REFERENCE_LOADER_SPEC.md](ingestion/REFERENCE_LOADER_SPEC.md) | Validating and loading the four reference tables | REF-01…12 | `ingestion/tests` | ☐ |
| [airflow/SPEC.md](airflow/SPEC.md) | The three DAGs, plus rules shared by all DAGs | DAG-, EXP-, QUA-, MNT-* | `airflow/tests` | ☐ shared ☐ export ☐ quality ☐ maintenance |
| [sql/SPEC.md](sql/SPEC.md) | The ten analytical reports | SQL-01…08, plus per-report acceptance | `airflow/tests/test_sql_reports.py` | ☐ |
| [query/SPEC.md](query/SPEC.md) | Developer query tool (not a deliverable) | QRY-01…05 | manual | ☐ |
| [Runtime overlay](#spec-runtime-overlay-solutiondocker-composeyml) (below) | `solution/docker-compose.yml` and the Trino override | RUN-01…08 | live, review | ☐ |

**Supporting documents** (for background, not sign-off):

- [README.md](README.md): tables, layout and DAG behaviour.
- [TRADEOFFS.md](TRADEOFFS.md): trade-offs, incidents and production design
  considerations.
- [sql/README.md](sql/README.md): report definitions and thresholds.
- [docs/exploring.md](docs/exploring.md): a hands-on tour of the data.

## Running the verification

The test names referenced in the specs come from these two suites (91 and 71
tests). Run them from the repository root:

```bash
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps ingestion python -m pytest -q tests
docker compose -f docker-compose.yml -f solution/docker-compose.yml run --rm --no-deps \
  -e PYTHONPATH=/opt/airflow/dags -v "$PWD/solution/airflow/tests:/opt/solution-airflow-tests:ro" \
  --entrypoint python airflow-scheduler -m pytest -q -p no:cacheprovider /opt/solution-airflow-tests
```

Or run both suites with `./solution/run.sh test`, or one suite with
`./solution/ingestion/run.sh test -v` or `./solution/airflow/run.sh test -v`.
Add `-v` to any of these commands to list every test name, so you can check them
against the specs.

---

## Spec: runtime overlay (`solution/docker-compose.yml`)

**Purpose:** add the solution's services to the supplied stack without editing
anything outside `solution/`, so the whole system starts with one command.

| ID | Requirement | Verified by |
| --- | --- | --- |
| RUN-01 | `docker compose -f docker-compose.yml -f solution/docker-compose.yml up -d --build` MUST start the supplied platform plus `ingestion`, and run `reference-loader` once. There must be no undocumented dependencies on the host. | Live, repeatedly since 2026-09-27 |
| RUN-02 | `ingestion` MUST wait for Kafka, Nessie and Garage to be healthy **and for the supplied `kafka-init` to have created the topics** (`service_completed_successfully`), restart automatically (`unless-stopped`), and join the supplied `challenge` network. | Review of the overlay's `depends_on`, `restart` and `networks`. Found by a clean-start test on 2026-09-30: ingestion subscribed 5 s before `kafka-init` finished and missed `network.infrastructure` |
| RUN-03 | `reference-loader` MUST run once (`restart: "no"`), so a validation failure stays visible instead of restarting in a loop. | Review |
| RUN-04 | `query` MUST be in the `tools` profile, so `up` doesn't start it. | Review |
| RUN-05 | Trino's heap MUST be capped at 2 GB by mounting [`trino/jvm.config`](trino/jvm.config), which is identical to the supplied config except for `-Xms1G -Xmx2G`. Without the cap, the heap could grow to about 6 GB of an 8 GB Docker VM, and the VM's OOM killer repeatedly killed Trino. | Review (compare with the supplied config); live: 0 Trino restarts after the cap while running all reports and compacting 64,808 files |
| RUN-06 | No secrets beyond the supplied local-development defaults. The storage keys use the same `${S3_ACCESS_KEY:-…}` defaults as the supplied `docker-compose.yml` and `.env.example`. | Review; a credential scan before committing found nothing new |
| RUN-08 | `ingestion` MUST have `stop_grace_period: 30s`, so the final write and commit on shutdown finish before Docker force-kills the container (default 10 s). | Review of the overlay; live: a stop on 2026-09-30 finished cleanly in about 1 s |
| RUN-07 | Python dependencies MUST be pinned: `ingestion/requirements.txt` (including `pyiceberg-core`), `query/requirements.txt`, and the Airflow image (`trino`, `pytest`, with `apache-airflow==3.3.0` pinned alongside). | Review; `pyiceberg-core` is needed to write partitioned tables, and its absence only fails at runtime |

**Known limitations:** everything runs on one Docker VM with shared memory. Its
effects are recorded in
[TRADEOFFS.md → Scaling the system](TRADEOFFS.md#scaling-the-system).

**Sign-off:**

| Reviewer | Date | Decision (approve / changes requested) | Notes |
| --- | --- | --- | --- |
| | | | |
