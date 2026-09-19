# Real-Time Stocks Pipeline

A streaming data platform that takes live trade events from Finnhub's
WebSocket feed to an analytics-ready warehouse in under a minute, with the
latency measured by the pipeline itself rather than asserted in a README.

[![CI](https://github.com/OWNER/real-time-stocks-mds/actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)
[![CD](https://github.com/OWNER/real-time-stocks-mds/actions/workflows/cd.yml/badge.svg)](../../actions/workflows/cd.yml)

---

## What it does

```
Finnhub WebSocket  ──▶  Kafka (KRaft)  ──▶  Snowpipe Streaming  ──▶  Snowflake
                            │                                          │
                            └──▶  S3 sink ──▶ MinIO (archive)          ▼
                                                              dynamic tables
                                                             bronze/silver/gold
                                                                       │
                                                                       ▼
                                                                   Power BI
```

Trade events, not polled snapshots. Kafka Connect, not a hand-written
consumer. Snowflake dynamic tables, not a cron-driven dbt run. Airflow owns
data quality, deployment and cost telemetry — the things that genuinely are
batch concerns.

See **[docs/architecture.md](docs/architecture.md)** for the full diagram,
the latency budget, and the reasoning behind each component.

---

## Quick start

**Prerequisites:** Docker 24+, a free [Finnhub](https://finnhub.io) API key,
and a Snowflake account (the 30-day trial is enough).

```bash
git clone https://github.com/Yassine-Ben-Terras/real-time-stocks-mds.git
cd real-time-stocks-mds

# 1. Configure
make env                    # creates .env from the template
$EDITOR .env                # add your Finnhub key and Snowflake account

# 2. Set up Snowflake (Terraform, or snowflake/00_bootstrap.sql by hand)
make keypair                # Snowpipe Streaming requires key-pair auth
cd terraform && terraform init && terraform apply && cd ..

# 3. Start everything
make up                     # Kafka, Connect, producer, MinIO, Airflow
make connectors             # register the Snowflake and MinIO sinks

# 4. Build the models
make dbt-deps && make dbt-build
```

| Service | URL | Purpose |
|---|---|---|
| Kafka UI | http://localhost:8081 | topics, lag, connector status |
| Kafka Connect | http://localhost:8083 | connector REST API |
| MinIO console | http://localhost:9001 | archived Parquet partitions |
| Airflow | http://localhost:8080 | quality and maintenance DAGs |

Verify the stream is flowing with the queries in
[`snowflake/01_verify_streaming.sql`](snowflake/01_verify_streaming.sql).

---

## Repository layout

```text
.
├── producer/               # Finnhub WebSocket → Kafka
│   ├── config.py           #   env-only settings, no hardcoded secrets
│   ├── normalize.py        #   the topic's wire contract, unit tested
│   ├── stream.py           #   reconnect + heartbeat handling
│   └── sink.py             #   idempotent producer, keyed by symbol
├── connect/                # Kafka Connect sink configs (templated)
├── dbt_stocks/
│   ├── models/bronze/      #   typed view over the connector's landing table
│   ├── models/silver/      #   deduplicated, validated (dynamic table)
│   ├── models/gold/        #   OHLC candles, KPIs, volatility, pipeline health
│   ├── tests/              #   singular tests: candle coherence, latency SLA
│   └── macros/             #   schema naming, CI row caps
├── airflow/dags/           # quality checks, model deployment, FinOps
├── terraform/              # Snowflake warehouse, database, roles, grants
├── snowflake/              # manual bootstrap + streaming diagnostics
├── tests/                  # pytest unit tests + DAG integrity tests
├── docker/                 # one Dockerfile per image
├── .github/workflows/      # CI, CD, Terraform plan
└── docs/architecture.md
```

---

## The data model

| Layer | Model | Materialisation | What it is |
|---|---|---|---|
| bronze | `bronze_stock_trades` | view | Typed 1:1 projection of the Kafka landing table |
| silver | `silver_stock_trades` | dynamic table, 1 min | Deduplicated and validated; adds ingest latency |
| gold | `gold_ohlc_1m` | dynamic table, 1 min | True OHLC candles from real trades |
| gold | `gold_latest_quote` | view | Current price and change vs. previous close |
| gold | `gold_symbol_volatility` | dynamic table, 5 min | Coefficient of variation per symbol |
| gold | `gold_pipeline_health` | view | Per-symbol ingestion latency and freshness |

Materialisation is chosen per model rather than applied uniformly. Dynamic
tables refresh incrementally but only when the query is an aggregate, so
`silver_stock_trades` dedupes with `GROUP BY` instead of `qualify
row_number()`. The two models that genuinely need window functions are views
over already-materialised aggregates, which keeps them cheap and always
current. Each model file explains its own choice.

### Data quality

- Generic tests: `not_null`, `unique`, `accepted_range`, uniqueness across
  the Kafka `(topic, partition, offset)` triple and the trade natural key.
- Singular tests: candle bounds must be coherent (high ≥ low, open and close
  inside the band), no trades timestamped in the future, p95 ingest latency
  under 60 seconds.
- Source freshness warns at 5 minutes and fails at 30 — the signal that the
  Kafka connector has stopped writing.
- Failures are persisted to a `dbt_test_failures` schema rather than just
  logged, so a failing row can be inspected after the fact.

---

## CI/CD

**CI** (every pull request):

| Job | What it catches |
|---|---|
| lint | ruff, sqlfluff, yamllint |
| unit-tests | pytest with an 80% coverage floor on the producer |
| dag-validation | DAG import errors, cycles, missing retries or tags |
| dbt-parse | broken refs, sources and Jinja — no warehouse needed |
| dbt-slim-ci | `dbt build --select state:modified+` in a per-PR schema, dropped afterwards |
| security | gitleaks secret scanning, pip-audit |
| docker-build | all three images build |
| compose-validate | `docker compose config` is valid |

**CD** (merge to `main`): builds and pushes images to GHCR tagged by commit
SHA, runs `dbt build` against production, uploads the production `manifest.json`
as an artifact so the next pull request can do a state comparison, and
publishes dbt docs to GitHub Pages.

Dependabot watches pip, Docker, Actions and Terraform.

---

## What changed from v1, and why

| v1 | v2 | Reason |
|---|---|---|
| Polled `/quote` every 6s | Trade WebSocket | `/quote` is a snapshot endpoint; polling it is not streaming |
| Custom Kafka consumer | Snowflake Kafka Connector | v1 auto-committed before writing — at-most-once, silent data loss |
| Airflow DAG every 60s re-reading the whole bucket | Snowpipe Streaming | v1's work grew linearly forever and `list_objects_v2` silently capped at 1000 keys |
| One JSON file per record | Parquet, hourly partitions | small-files problem |
| Manual `dbt run` | Dynamic tables + Airflow for quality | transformation had no orchestration at all |
| Credentials in source files | Env vars, key-pair auth, gitleaks | the fastest way to fail a code review |
| Zookeeper | KRaft | Zookeeper is removed in Kafka 4.0 |
| Candle open = first polled price | Candle open = first trade in the minute | v1's "open" was not the market open |
| Epoch seconds and timestamps used interchangeably | Explicit `trade_ts_ms` plus parsed `trade_ts` | v1's bronze and gold layers disagreed about the unit |
| No tests, no CI, no IaC | pytest, dbt tests, 8-job CI, Terraform | — |

---

## Local development

```bash
make install      # dependencies + pre-commit hooks
make ci           # lint and test exactly as CI does
make fmt          # auto-fix formatting
make lint         # ruff + sqlfluff + yamllint
make test         # pytest with coverage
make dbt-build    # build and test every model
make status       # connector health
make clean        # tear down including volumes
```

---

## Security

No credential appears in any committed file. Every secret is read from the
environment: docker compose injects from `.env` (gitignored), CI injects from
GitHub secrets, Terraform marks them sensitive. Snowpipe Streaming uses
key-pair authentication with three separate Snowflake roles — ingestion can
only write to `RAW`, the transformer can only read `RAW` and own the modelled
schemas, the reader gets `SELECT` on `GOLD` and nothing else. gitleaks runs
in CI and as a pre-commit hook.

---

## License

MIT — see [LICENSE](LICENSE).
