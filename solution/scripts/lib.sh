#!/usr/bin/env bash
# Shared helpers for the solution's run.sh scripts. Source this file; don't run it.
# It finds the repository root from its own location, so the scripts work from
# any directory, and defines `dc` as the one compose command they all use.
set -euo pipefail

SOLUTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$SOLUTION_DIR/.." && pwd)"

# docker compose with the supplied stack plus the solution overlay. Relative paths
# in both files resolve against the repository root, and .env is read from there.
dc() {
  docker compose --project-directory "$REPO_ROOT" \
    -f "$REPO_ROOT/docker-compose.yml" -f "$SOLUTION_DIR/docker-compose.yml" "$@"
}

# Print the calling script's leading comment block as its help text.
usage() {
  sed -n '2,/^$/{s/^# \{0,1\}//;p;}' "$1"
}

# `docker compose run` allocates a TTY by default, which turns \n into \r\n.
# When our output goes to a file or a pipe (e.g. `> out.csv`), ask for no TTY.
tty_flag() {
  if [ -t 1 ]; then echo ""; else echo "-T"; fi
}
