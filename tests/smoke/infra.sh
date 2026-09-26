#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPOSITORY_ROOT}"

healthy_services=(
  metadata-db
  kafka
  mosquitto
  garage
  nessie
  trino
  mqtt-kafka-bridge
  simulator
  airflow-api-server
  airflow-scheduler
  airflow-dag-processor
)

fail() {
  printf 'not ok - %s\n' "$1" >&2
  exit 1
}

pass() {
  printf 'ok - %s\n' "$1"
}

container_id() {
  docker compose ps -a -q "$1"
}

wait_for_healthy_services() {
  local deadline=$((SECONDS + 180))
  local service
  local id
  local status

  while (( SECONDS < deadline )); do
    local all_healthy=true

    for service in "${healthy_services[@]}"; do
      id="$(container_id "${service}")"
      if [[ -z "${id}" ]]; then
        all_healthy=false
        break
      fi

      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${id}")"
      if [[ "${status}" != "healthy" ]]; then
        all_healthy=false
        break
      fi
    done

    if [[ "${all_healthy}" == true ]]; then
      return 0
    fi

    sleep 2
  done

  docker compose ps -a >&2
  return 1
}

command -v docker >/dev/null || fail "Docker is not installed"
docker compose config --quiet || fail "Compose configuration is invalid"
pass "Compose configuration is valid"

wait_for_healthy_services || fail "long-running services did not become healthy"
pass "long-running services are healthy"

for service in kafka-init airflow-init; do
  id="$(container_id "${service}")"
  [[ -n "${id}" ]] || fail "${service} container does not exist"
  [[ "$(docker inspect --format '{{.State.ExitCode}}' "${id}")" == "0" ]] || fail "${service} did not exit successfully"
done
pass "one-shot initialization services succeeded"

expected_topics=$'network.connectivity\nnetwork.device_health\nnetwork.infrastructure\nnetwork.state\nnetwork.throughput'
actual_topics="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list | grep '^network\.' | LC_ALL=C sort)"
[[ "${actual_topics}" == "${expected_topics}" ]] || fail "Kafka topic bootstrap differs from the contract"
pass "Kafka topics are bootstrapped"

for topic in network.connectivity network.throughput network.device_health network.state network.infrastructure; do
  topic_config="$(docker compose exec -T kafka /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server kafka:29092 \
    --describe \
    --entity-type topics \
    --entity-name "${topic}")"
  grep -q 'retention.ms=7200000' <<<"${topic_config}" || fail "${topic} lacks two-hour retention"
  grep -q 'segment.ms=900000' <<<"${topic_config}" || fail "${topic} lacks 15-minute segments"
done
pass "Kafka source-topic retention is bounded"

docker compose exec -T garage /garage bucket info warehouse >/dev/null || fail "Garage warehouse bucket is unavailable"
pass "Garage warehouse bucket is available"

database_names="$(docker compose exec -T metadata-db psql -U challenge_admin -d postgres -Atc "SELECT datname FROM pg_database WHERE datname IN ('airflow','nessie') ORDER BY datname;")"
[[ "${database_names}" == $'airflow\nnessie' ]] || fail "metadata databases are unavailable"
pass "Airflow and Nessie metadata databases are available"

docker compose exec -T nessie curl --fail --silent http://localhost:9000/q/health/ready >/dev/null || fail "Nessie is not ready"
docker compose exec -T nessie curl --fail --silent http://localhost:19120/iceberg/v1/config >/dev/null || fail "Nessie Iceberg REST configuration is unavailable"
pass "Nessie and Iceberg REST are available"

docker compose exec -T trino trino --execute "SELECT 1" >/dev/null || fail "Trino query failed"
docker compose exec -T trino trino --execute "SHOW CATALOGS" | grep -q '"iceberg"' || fail "Trino did not load the Iceberg catalog"
pass "Trino queries and exposes the Iceberg catalog"

airflow_health="$(docker compose exec -T airflow-api-server curl --fail --silent http://localhost:8080/api/v2/monitor/health)"
grep -q '"metadatabase":{"status":"healthy"}' <<<"${airflow_health}" || fail "Airflow metadata database is unhealthy"
grep -q '"scheduler":{"status":"healthy"' <<<"${airflow_health}" || fail "Airflow scheduler is unhealthy"
grep -q '"dag_processor":{"status":"healthy"' <<<"${airflow_health}" || fail "Airflow DAG processor is unhealthy"
pass "Airflow control-plane components are healthy"

message="$(docker compose exec -T mosquitto sh -c 'mosquitto_sub -h localhost -t infra/smoke -C 1 -W 5 & subscriber=$!; sleep 1; mosquitto_pub -h localhost -t infra/smoke -m ready; wait "$subscriber"')"
[[ "${message}" == "ready" ]] || fail "MQTT publish/subscribe round trip failed"
pass "Mosquitto publish/subscribe round trip succeeded"

simulator_event="$(docker compose exec -T mosquitto mosquitto_sub -h localhost -t 'network/+/+/connectivity' -C 20 -W 10)"
grep -q '"event_type":"connectivity_metric"' <<<"${simulator_event}" || fail "simulator connectivity event was not observed"
grep -q '"schema_version":1' <<<"${simulator_event}" || fail "simulator event schema version is incorrect"
pass "simulator publishes connectivity telemetry"

throughput_event="$(docker compose exec -T mosquitto mosquitto_sub -h localhost -t 'network/+/+/throughput' -C 10 -W 10)"
grep -q '"event_type":"throughput_test"' <<<"${throughput_event}" || fail "simulator throughput event was not observed"
health_event="$(docker compose exec -T mosquitto mosquitto_sub -h localhost -t 'network/+/+/health' -C 10 -W 10)"
grep -q '"event_type":"device_health"' <<<"${health_event}" || fail "simulator health event was not observed"
pass "simulator publishes throughput and health telemetry"

kafka_event="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic network.connectivity \
  --formatter-property print.key=true \
  --formatter-property key.separator=$'\t' \
  --max-messages 20 \
  --timeout-ms 10000 2>/dev/null)"
kafka_record="$(awk '/"event_type":"connectivity_metric"/{print; exit}' <<<"${kafka_event}")"
kafka_key="${kafka_record%%$'\t'*}"
kafka_value="${kafka_record#*$'\t'}"
[[ "${kafka_key}" == device-* ]] || fail "bridge did not use device_id as the Kafka key"
grep -q '"event_type":"connectivity_metric"' <<<"${kafka_value}" || fail "bridge connectivity event was not observed in Kafka"
grep -q "\"device_id\":\"${kafka_key}\"" <<<"${kafka_value}" || fail "Kafka key does not match the event device_id"
pass "bridge forwards connectivity telemetry to Kafka with a device key"

for specification in 'network.throughput:throughput_test' 'network.device_health:device_health'; do
  kafka_topic="${specification%%:*}"
  event_type="${specification#*:}"
  kafka_event="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:29092 \
    --topic "${kafka_topic}" \
    --formatter-property print.key=true \
    --formatter-property key.separator=$'\t' \
    --max-messages 10 \
    --timeout-ms 10000 2>/dev/null)"
  kafka_record="$(awk -v event_type="${event_type}" 'index($0, "\"event_type\":\"" event_type "\""){print; exit}' <<<"${kafka_event}")"
  kafka_key="${kafka_record%%$'\t'*}"
  kafka_value="${kafka_record#*$'\t'}"
  [[ -n "${kafka_key}" ]] || fail "${kafka_topic} record lacks a device key"
  grep -q "\"event_type\":\"${event_type}\"" <<<"${kafka_value}" || fail "${kafka_topic} event was not observed"
  grep -q "\"device_id\":\"${kafka_key}\"" <<<"${kafka_value}" || fail "${kafka_topic} key does not match device_id"
done
pass "bridge forwards throughput and health telemetry with device keys"

pass "infrastructure smoke suite passed"
