#!/usr/bin/env bash
# Whole stack: the supplied platform plus the solution services.
#
#   ./solution/run.sh up        build and start everything (ingestion, reference load, Airflow, Trino, ...)
#   ./solution/run.sh down      stop and remove the containers; data volumes are KEPT
#   ./solution/run.sh status    containers and their health
#   ./solution/run.sh logs [service]   follow logs (all services, or one, e.g. ingestion)
#   ./solution/run.sh test      run both test suites (ingestion and Airflow)
#
# Full reset (deletes all data) is deliberately not a command here:
#   docker compose -f docker-compose.yml -f solution/docker-compose.yml down -v

source "$(dirname "$0")/scripts/lib.sh"

case "${1:-help}" in
  up)     dc up -d --build ;;
  down)   dc down ;;
  status) dc ps --format 'table {{.Service}}\t{{.Status}}' ;;
  logs)   shift; dc logs -f --tail 100 "$@" ;;
  test)   "$SOLUTION_DIR/ingestion/run.sh" test && "$SOLUTION_DIR/airflow/run.sh" test ;;
  *)      usage "$0" ;;
esac
