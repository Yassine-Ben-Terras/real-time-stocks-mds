#!/usr/bin/env bash
# Render the connector templates with values from the environment and POST
# them to Kafka Connect. Idempotent: re-running updates the existing config.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "${HERE}/.env" ]]; then
  set -a; source "${HERE}/.env"; set +a
fi

: "${SNOWFLAKE_ACCOUNT:?set SNOWFLAKE_ACCOUNT in .env}"
: "${SNOWFLAKE_USER:?set SNOWFLAKE_USER in .env}"
: "${SNOWFLAKE_PRIVATE_KEY:?set SNOWFLAKE_PRIVATE_KEY in .env (PKCS#8 body, no header/footer, no newlines)}"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="${SNOWFLAKE_PRIVATE_KEY_PASSPHRASE:-}"

wait_for_connect() {
  echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
  for _ in $(seq 1 60); do
    if curl -sf "${CONNECT_URL}/connectors" >/dev/null; then echo "connect is up"; return 0; fi
    sleep 5
  done
  echo "Kafka Connect never became ready" >&2; exit 1
}

register() {
  local tmpl="$1" rendered name
  rendered="$(mktemp)"
  envsubst < "${tmpl}" > "${rendered}"
  name="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["name"])' "${rendered}")"
  echo "registering ${name}"
  curl -sS -X PUT -H 'Content-Type: application/json' \
    --data "$(python3 -c 'import json,sys;print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "${rendered}")" \
    "${CONNECT_URL}/connectors/${name}/config" | python3 -m json.tool
  rm -f "${rendered}"
}

wait_for_connect
register "${HERE}/connect/snowflake-sink.json.tmpl"
register "${HERE}/connect/minio-archive-sink.json.tmpl"

echo
echo "connector status:"
curl -sS "${CONNECT_URL}/connectors?expand=status" | python3 -m json.tool
