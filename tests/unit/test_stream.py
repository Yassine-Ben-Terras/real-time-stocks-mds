from __future__ import annotations

import threading

from producer.config import Settings
from producer.stream import FinnhubTradeStream, subscribe_payloads


def make_settings(**overrides):
    base = {"finnhub_api_key": "k", "symbols": ["AAPL", "MSFT"]}
    base.update(overrides)
    return Settings(**base)


def test_subscribe_payloads_one_frame_per_symbol():
    payloads = subscribe_payloads(["AAPL", "MSFT"])
    assert payloads == [
        '{"type": "subscribe", "symbol": "AAPL"}',
        '{"type": "subscribe", "symbol": "MSFT"}',
    ]


def test_trade_frames_reach_the_handler():
    seen = []
    stream = FinnhubTradeStream(make_settings(), seen.append)
    stream._handle_raw('{"type":"trade","data":[{"s":"AAPL","p":1,"t":1}]}')
    assert len(seen) == 1


def test_ping_and_error_frames_are_swallowed():
    seen = []
    stream = FinnhubTradeStream(make_settings(), seen.append)
    stream._handle_raw('{"type":"ping"}')
    stream._handle_raw('{"type":"error","msg":"bad token"}')
    assert seen == []


def test_undecodable_payload_does_not_raise():
    seen = []
    stream = FinnhubTradeStream(make_settings(), seen.append)
    stream._handle_raw("<html>gateway error</html>")
    assert seen == []


def test_any_frame_resets_the_staleness_clock():
    stream = FinnhubTradeStream(make_settings(heartbeat_timeout_s=60), lambda _f: None)
    stream._last_message_at -= 300
    assert stream.is_stale is True
    stream._handle_raw('{"type":"ping"}')
    assert stream.is_stale is False


def test_stop_sets_the_shared_event():
    stop = threading.Event()
    stream = FinnhubTradeStream(make_settings(), lambda _f: None, stop_event=stop)
    stream.stop()
    assert stop.is_set()
