#!/usr/bin/env bash
# Run SQL against Trino with the Python query tool (query.py). Arguments are passed through.
#
#   ./solution/query/run.sh "SELECT count(*) FROM sites"     one or more statements
#   ./solution/query/run.sh -f /sql/checks/duplicate_origin.sql   a file (solution/sql is mounted at /sql)
#   ./solution/query/run.sh --csv "SELECT * FROM sites" > sites.csv
#   ./solution/query/run.sh                                   interactive prompt (end with ';', 'quit' to exit)
#   ./solution/query/run.sh scratch                           run your local scratch.py (edits apply without a rebuild)

source "$(dirname "$0")/../scripts/lib.sh"

case "${1:-}" in
  -h|--help|help) usage "$0" ;;
  scratch) dc run --rm $(tty_flag) -v "$SOLUTION_DIR/query:/app" --entrypoint python query scratch.py ;;
  *)       dc run --rm $(tty_flag) query "$@" ;;
esac
