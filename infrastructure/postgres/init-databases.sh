#!/usr/bin/env bash
set -Eeuo pipefail

create_user_and_database() {
  local database_name="$1"
  local user_name="$2"
  local user_password="$3"

  psql \
    --username "${POSTGRES_USER}" \
    --dbname "${POSTGRES_DB}" \
    --set=ON_ERROR_STOP=1 \
    --set=database_name="${database_name}" \
    --set=user_name="${user_name}" \
    --set=user_password="${user_password}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'user_name', :'user_password')
WHERE NOT EXISTS (
  SELECT FROM pg_catalog.pg_roles WHERE rolname = :'user_name'
) \gexec

SELECT format('CREATE DATABASE %I OWNER %I', :'database_name', :'user_name')
WHERE NOT EXISTS (
  SELECT FROM pg_catalog.pg_database WHERE datname = :'database_name'
) \gexec
SQL
}

create_user_and_database airflow airflow "${AIRFLOW_DB_PASSWORD}"
create_user_and_database nessie nessie "${NESSIE_DB_PASSWORD}"
