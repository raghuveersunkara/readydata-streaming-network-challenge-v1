#!/usr/bin/env bash
# Streaming ingestion (solution-ingestion) and the reference-data loader.
#
#   ./solution/ingestion/run.sh start            build and start ingestion (starts Kafka, Nessie, Garage if needed)
#   ./solution/ingestion/run.sh stop             stop ingestion only (buffered rows are written and committed first)
#   ./solution/ingestion/run.sh restart          rebuild and restart after a code change
#   ./solution/ingestion/run.sh status           container state and the latest writes
#   ./solution/ingestion/run.sh logs             follow the ingestion log
#   ./solution/ingestion/run.sh test [pytest args]   run the ingestion test suite
#   ./solution/ingestion/run.sh load-reference   validate and (re)load the reference tables (safe to repeat)

source "$(dirname "$0")/../scripts/lib.sh"

case "${1:-help}" in
  start)   dc up -d --build ingestion ;;
  stop)    dc stop ingestion ;;
  restart) dc up -d --build --no-deps --force-recreate ingestion ;;
  status)
    dc ps ingestion --format 'table {{.Service}}\t{{.Status}}'
    echo "Latest writes:"
    docker logs --since 2m solution-ingestion 2>&1 | grep -E 'Appended|ERROR|WARNING' | tail -5 || echo "  (none in the last 2 minutes)"
    ;;
  logs)    dc logs -f --tail 100 ingestion ;;
  test)    shift; dc build -q ingestion && dc run --rm --no-deps $(tty_flag) ingestion python -m pytest -q -p no:warnings tests "$@" ;;
  load-reference) dc run --rm $(tty_flag) reference-loader ;;
  *)       usage "$0" ;;
esac
