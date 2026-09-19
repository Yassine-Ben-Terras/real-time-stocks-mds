"""Translate raw Finnhub websocket frames into the canonical trade event.

The wire contract for the topic ``stock.trades.v1`` is defined here and
nowhere else. Every field is explicit, typed, and timezone-aware so that the
warehouse never has to guess whether a number is seconds or milliseconds --
the bug that existed in the previous version of this project.

Finnhub trade frame::

    {"type": "trade", "data": [
        {"s": "AAPL", "p": 191.5, "v": 100, "t": 1700000000123, "c": ["1"]}
    ]}
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterator

SCHEMA_VERSION = "1.0.0"
SOURCE = "finnhub.trade"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds")


def ms_to_iso(epoch_ms: int) -> str:
    """Convert Finnhub's epoch milliseconds into an ISO-8601 UTC string."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC).isoformat(
        timespec="milliseconds"
    )


def normalize_trade(raw: dict[str, Any], *, ingested_at: str | None = None) -> dict[str, Any]:
    """Map one Finnhub trade object onto the canonical event.

    Raises:
        ValueError: if a mandatory field is missing or the wrong type. The
            caller routes these to the dead-letter path rather than crashing.
    """
    try:
        symbol = str(raw["s"]).strip().upper()
        price = float(raw["p"])
        trade_ts_ms = int(raw["t"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Malformed Finnhub trade frame: {raw!r}") from exc

    if not symbol:
        raise ValueError(f"Empty symbol in frame: {raw!r}")
    if price <= 0:
        raise ValueError(f"Non-positive price {price} in frame: {raw!r}")
    if trade_ts_ms <= 0:
        raise ValueError(f"Non-positive timestamp {trade_ts_ms} in frame: {raw!r}")

    volume_raw = raw.get("v")
    volume = float(volume_raw) if volume_raw is not None else 0.0

    conditions = raw.get("c") or []
    if not isinstance(conditions, list):
        conditions = [str(conditions)]

    return {
        "schema_version": SCHEMA_VERSION,
        "source": SOURCE,
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "trade_ts_ms": trade_ts_ms,
        "trade_ts": ms_to_iso(trade_ts_ms),
        "conditions": [str(c) for c in conditions],
        "ingested_at": ingested_at or _utc_now_iso(),
    }


def iter_trades(frame: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield the raw trade objects out of a websocket frame.

    Finnhub also sends ``{"type": "ping"}`` keepalives and error frames; both
    are filtered out here so the caller only ever sees trades.
    """
    if not isinstance(frame, dict):
        return
    if frame.get("type") != "trade":
        return
    data = frame.get("data")
    if not isinstance(data, list):
        return
    for item in data:
        if isinstance(item, dict):
            yield item


def event_key(event: dict[str, Any]) -> bytes:
    """Partition key. Keying by symbol guarantees per-symbol ordering."""
    return str(event["symbol"]).encode("utf-8")


def dedupe_id(event: dict[str, Any]) -> str:
    """Natural key of a trade, used downstream to make ingestion idempotent."""
    return f"{event['symbol']}|{event['trade_ts_ms']}|{event['price']}|{event['volume']}"
