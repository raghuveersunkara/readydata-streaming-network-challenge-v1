#!/usr/bin/env bash
# Run the checked-in SQL reports through Trino (via the query container).
#
#   ./solution/sql/run.sh list                       the reports
#   ./solution/sql/run.sh <report> [--csv]           run one, e.g. 04_technology_statistics (".sql" optional)
#                                                    --csv prints CSV, e.g. `... --csv > out.csv`
#   ./solution/sql/run.sh check <name>               run a data check from checks/, e.g. duplicate_origin
#   ./solution/sql/run.sh all                        run every report and show row counts and timings
#
# For a checksummed, never-overwritten export, use: ./solution/airflow/run.sh export <report.sql>

source "$(dirname "$0")/../scripts/lib.sh"

query() { dc run --rm $(tty_flag) query "$@"; }
report_file() { local n="${1%.sql}"; [ -f "$SOLUTION_DIR/sql/$n.sql" ] || { echo "No report $n.sql (see: $0 list)" >&2; exit 1; }; echo "$n.sql"; }

case "${1:-help}" in
  list)  ls "$SOLUTION_DIR/sql" | grep -E '^[0-9]{2}_.*\.sql$' ;;
  check) f="${2%.sql}"; query -f "/sql/checks/$f.sql" ;;
  all)
    for f in $(ls "$SOLUTION_DIR/sql" | grep -E '^[0-9]{2}_.*\.sql$'); do
      printf '%-42s ' "$f"
      query -f "/sql/$f" --max-rows 1 2>&1 | grep -oE '^\([0-9]+ rows?.*\)$|Query failed.*' | tail -1
    done ;;
  help|-h|--help) usage "$0" ;;
  *)
    f="$(report_file "$1")"; shift
    if [ "${1:-}" = "--csv" ]; then query --csv -f "/sql/$f"; else query -f "/sql/$f" "$@"; fi ;;
esac
