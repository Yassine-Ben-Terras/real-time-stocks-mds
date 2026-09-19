"""Contract tests for the topic payload. If these change, the topic version must."""

from __future__ import annotations

import pytest

from producer.normalize import (
    SCHEMA_VERSION,
    dedupe_id,
    event_key,
    iter_trades,
    ms_to_iso,
    normalize_trade,
)

TRADE_FRAME = {
    "type": "trade",
    "data": [
        {"s": "AAPL", "p": 191.52, "v": 100, "t": 1700000000123, "c": ["1", "12"]},
        {"s": "msft", "p": 402.1, "v": 50, "t": 1700000000456, "c": []},
    ],
}


def test_ms_to_iso_treats_input_as_milliseconds():
    # 1700000000123 ms == 2023-11-14T22:13:20.123Z
    assert ms_to_iso(1700000000123).startswith("2023-11-14T22:13:20.123")


def test_ms_to_iso_is_timezone_aware():
    assert ms_to_iso(1700000000123).endswith("+00:00")


def test_normalize_produces_full_contract():
    event = normalize_trade(TRADE_FRAME["data"][0], ingested_at="2023-11-14T22:13:21.000+00:00")
    assert event == {
        "schema_version": SCHEMA_VERSION,
        "source": "finnhub.trade",
        "symbol": "AAPL",
        "price": 191.52,
        "volume": 100.0,
        "trade_ts_ms": 1700000000123,
        "trade_ts": "2023-11-14T22:13:20.123+00:00",
        "conditions": ["1", "12"],
        "ingested_at": "2023-11-14T22:13:21.000+00:00",
    }


def test_symbol_is_uppercased():
    event = normalize_trade(TRADE_FRAME["data"][1])
    assert event["symbol"] == "MSFT"


def test_missing_volume_defaults_to_zero():
    event = normalize_trade({"s": "AAPL", "p": 10.0, "t": 1700000000000})
    assert event["volume"] == 0.0


def test_scalar_conditions_are_coerced_to_a_list():
    event = normalize_trade({"s": "AAPL", "p": 10.0, "t": 1700000000000, "c": "7"})
    assert event["conditions"] == ["7"]


@pytest.mark.parametrize(
    "bad",
    [
        {"p": 10.0, "t": 1700000000000},            # no symbol
        {"s": "AAPL", "t": 1700000000000},          # no price
        {"s": "AAPL", "p": 10.0},                   # no timestamp
        {"s": "AAPL", "p": 0, "t": 1700000000000},  # non-positive price
        {"s": "AAPL", "p": -5, "t": 1700000000000},
        {"s": "AAPL", "p": 10.0, "t": 0},           # non-positive timestamp
        {"s": "", "p": 10.0, "t": 1700000000000},   # empty symbol
        {"s": "AAPL", "p": "abc", "t": 1700000000000},
    ],
)
def test_malformed_frames_raise_value_error(bad):
    with pytest.raises(ValueError):
        normalize_trade(bad)


def test_iter_trades_yields_every_trade():
    assert len(list(iter_trades(TRADE_FRAME))) == 2


@pytest.mark.parametrize(
    "frame",
    [
        {"type": "ping"},
        {"type": "error", "msg": "bad token"},
        {"type": "trade"},
        {"type": "trade", "data": "not-a-list"},
        {},
        "not-a-dict",
    ],
)
def test_iter_trades_ignores_non_trade_frames(frame):
    assert list(iter_trades(frame)) == []


def test_event_key_is_the_symbol_so_ordering_is_per_symbol():
    event = normalize_trade(TRADE_FRAME["data"][0])
    assert event_key(event) == b"AAPL"


def test_dedupe_id_is_stable_for_identical_trades():
    a = normalize_trade(TRADE_FRAME["data"][0], ingested_at="2023-01-01T00:00:00.000+00:00")
    b = normalize_trade(TRADE_FRAME["data"][0], ingested_at="2024-01-01T00:00:00.000+00:00")
    # ingestion time must not affect the natural key
    assert dedupe_id(a) == dedupe_id(b)
