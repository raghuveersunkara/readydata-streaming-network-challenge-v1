#!/usr/bin/env bash
# Airflow DAGs. export/quality/maintenance run the DAG right away and print its result
# (airflow dags test); use `trigger` to queue a normal run you can watch in the UI.
#
#   ./solution/airflow/run.sh list                          the solution DAGs and whether they're paused
#   ./solution/airflow/run.sh export <report.sql>           manual_sql_to_csv, e.g. 04_technology_statistics.sql
#   ./solution/airflow/run.sh quality [window_hours]        lakehouse_quality (default 6 hours)
#   ./solution/airflow/run.sh maintenance [plan|apply] [table ...]
#                                                           iceberg_maintenance (default: plan, all tables)
#   ./solution/airflow/run.sh trigger <dag_id> [json_conf]  queue a scheduler run (see it at http://localhost:8081)
#   ./solution/airflow/run.sh test [pytest args]            run the Airflow test suite
#
# Airflow UI: http://localhost:8081 (airflow / airflow)

source "$(dirname "$0")/../scripts/lib.sh"

airflow() { dc exec -T airflow-scheduler airflow "$@"; }

# Run a DAG now and keep only its useful output lines (the full log is very verbose).
run_now() {
  local dag="$1" conf="$2" filter="$3"
  airflow dags test "$dag" --conf "$conf" 2>&1 | grep -E "$filter|DagRun Finished" \
    | sed -E 's/^.*DagRun Finished:.*state=([a-z]+).*$/Run finished: \1/'
}

case "${1:-help}" in
  list)
    airflow dags list 2>/dev/null | grep -E 'dag_id|manual_sql_to_csv|lakehouse_quality|iceberg_maintenance' \
      | awk -F'|' '{print $1 "|" $4}' | sort -u ;;
  export)
    [ -n "${2:-}" ] || { echo "usage: $0 export <report.sql>   (see: ./solution/sql/run.sh list)"; exit 1; }
    run_now manual_sql_to_csv "{\"report\": \"$2\"}" '"host_path"|"rows"|"sha256"|sha256_matches|Unknown report' ;;
  quality)
    run_now lakehouse_quality "{\"window_hours\": ${2:-6}}" '^(fail|warn|ok) |^status |^Summary:' ;;
  maintenance)
    mode="${2:-plan}"; shift $(( $# < 2 ? $# : 2 ))
    tables=""; for t in "$@"; do tables="$tables\"$t\","; done
    conf="{\"mode\": \"$mode\"${tables:+, \"tables\": [${tables%,}]}}"
    run_now iceberg_maintenance "$conf" '^table |^[a-z_]+ +(compact|skip) |files before|^[a-z_]+ +[0-9]+ +[0-9]+ |status.: .(ok|failed|unverified)' ;;
  trigger)
    [ -n "${2:-}" ] || { echo "usage: $0 trigger <dag_id> [json_conf]"; exit 1; }
    airflow dags trigger "$2" --conf "${3:-{\}}" 2>/dev/null | grep -oE 'manual__[^ |]+' | head -1 \
      | sed 's/^/Queued run: /; s/$/   (watch it at http:\/\/localhost:8081)/' ;;
  test)
    shift
    dc run --rm --no-deps $(tty_flag) -e PYTHONPATH=/opt/airflow/dags \
      -v "$SOLUTION_DIR/airflow/tests:/opt/solution-airflow-tests:ro" \
      --entrypoint python airflow-scheduler -m pytest -q -p no:cacheprovider -p no:warnings \
      /opt/solution-airflow-tests "$@" ;;
  *) usage "$0" ;;
esac
