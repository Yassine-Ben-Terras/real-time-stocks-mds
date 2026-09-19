#!/usr/bin/env bash
# Snowpipe Streaming requires key-pair auth -- passwords are not supported.
# This generates an unencrypted PKCS#8 keypair and prints what to paste where.
set -euo pipefail

OUT_DIR="${1:-./.secrets}"
mkdir -p "${OUT_DIR}"; chmod 700 "${OUT_DIR}"

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out "${OUT_DIR}/snowflake_key.p8" -nocrypt
openssl rsa -in "${OUT_DIR}/snowflake_key.p8" -pubout -out "${OUT_DIR}/snowflake_key.pub"
chmod 600 "${OUT_DIR}/snowflake_key.p8"

PUB_BODY="$(grep -v 'PUBLIC KEY' "${OUT_DIR}/snowflake_key.pub" | tr -d '\n')"
PRIV_BODY="$(grep -v 'PRIVATE KEY' "${OUT_DIR}/snowflake_key.p8" | tr -d '\n')"

cat <<MSG

1) Run this in Snowflake:

   ALTER USER ${SNOWFLAKE_USER:-<YOUR_USER>} SET RSA_PUBLIC_KEY='${PUB_BODY}';

2) Put this in your .env (single line, no quotes needed):

   SNOWFLAKE_PRIVATE_KEY=${PRIV_BODY}

${OUT_DIR}/ is gitignored. Never commit these files.
MSG
