from __future__ import annotations

import json

from producer.main import make_frame_handler
from producer.normalize import normalize_trade
from producer.sink import TradeSink


class FakeProducer:
    def __init__(self):
        self.messages = []
        self.polls = 0
        self.flushed = False

    def produce(self, topic, value, key, on_delivery):
        self.messages.append((topic, key, value))

    def poll(self, timeout):
        self.polls += 1
        return 0

    def flush(self, timeout):
        self.flushed = True
        return 0


def test_send_serializes_to_compact_json_and_keys_by_symbol():
    fake = FakeProducer()
    sink = TradeSink(fake, "stock.trades.v1")
    sink.send(normalize_trade({"s": "AAPL", "p": 1.5, "v": 10, "t": 1700000000000}))

    topic, key, value = fake.messages[0]
    assert topic == "stock.trades.v1"
    assert key == b"AAPL"
    assert json.loads(value)["symbol"] == "AAPL"
    assert b", " not in value  # compact separators
    assert sink.sent == 1


def test_send_polls_for_delivery_callbacks_without_blocking():
    fake = FakeProducer()
    sink = TradeSink(fake, "t")
    sink.send(normalize_trade({"s": "AAPL", "p": 1.5, "t": 1700000000000}))
    assert fake.polls == 1


def test_reject_counts_without_raising():
    sink = TradeSink(FakeProducer(), "t")
    sink.reject("bad price", {"s": "AAPL"})
    assert sink.rejected == 1
    assert sink.sent == 0


def test_bad_record_does_not_stop_the_batch():
    """One malformed trade must not cost us the good ones in the same frame."""
    fake = FakeProducer()
    sink = TradeSink(fake, "t")
    handler = make_frame_handler(sink)

    handler(
        {
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 10.0, "t": 1700000000000},
                {"s": "BROKEN", "p": -1, "t": 1700000000000},
                {"s": "MSFT", "p": 20.0, "t": 1700000000001},
            ],
        }
    )

    assert sink.sent == 2
    assert sink.rejected == 1


def test_flush_is_delegated():
    fake = FakeProducer()
    TradeSink(fake, "t").flush()
    assert fake.flushed
