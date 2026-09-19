"""Kafka sink.

Configured for at-least-once delivery with the idempotent producer enabled,
so broker-side retries cannot duplicate a record within a session. Downstream
deduplication (silver layer) closes the remaining gap across restarts.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from producer.config import Settings
from producer.normalize import event_key

log = logging.getLogger(__name__)


class ProducerLike(Protocol):
    """Minimal surface of confluent_kafka.Producer, so tests can fake it."""

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def build_producer(settings: Settings) -> ProducerLike:
    """Create a confluent-kafka producer with production-grade settings."""
    from confluent_kafka import Producer  # imported lazily so tests stay light

    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "stocks-finnhub-producer",
            # Durability: wait for all in-sync replicas.
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": 10,
            "retry.backoff.ms": 200,
            # Throughput: small batches, still sub-100ms latency.
            "linger.ms": settings.kafka_linger_ms,
            "batch.size": settings.kafka_batch_size,
            "compression.type": "zstd",
        }
    )


def _on_delivery(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("delivery_failed error=%s topic=%s", err, getattr(msg, "topic", "?"))


class TradeSink:
    """Serializes canonical events and hands them to Kafka."""

    def __init__(self, producer: ProducerLike, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self.sent = 0
        self.rejected = 0

    def send(self, event: dict[str, Any]) -> None:
        payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
        self._producer.produce(
            self._topic,
            value=payload,
            key=event_key(event),
            on_delivery=_on_delivery,
        )
        self.sent += 1
        # Serve delivery callbacks without blocking the websocket loop.
        self._producer.poll(0)

    def reject(self, reason: str, raw: Any) -> None:
        """Malformed input. Counted and logged rather than crashing the stream."""
        self.rejected += 1
        log.warning("rejected_record reason=%s raw=%.200s", reason, repr(raw))

    def flush(self, timeout: float = 10.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining:
            log.error("flush_incomplete outstanding=%d", remaining)
        return remaining
