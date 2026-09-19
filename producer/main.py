"""Entrypoint: Finnhub websocket -> canonical events -> Kafka."""

from __future__ import annotations

import logging
import signal
import sys
import threading
from types import FrameType
from typing import Any

from producer.config import ConfigError, Settings
from producer.normalize import iter_trades, normalize_trade
from producer.sink import TradeSink, build_producer
from producer.stream import FinnhubTradeStream

log = logging.getLogger("producer")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
        stream=sys.stdout,
    )


def make_frame_handler(sink: TradeSink):
    """Bind a websocket frame to the sink, isolating bad records."""

    def handle(frame: dict[str, Any]) -> None:
        for raw_trade in iter_trades(frame):
            try:
                event = normalize_trade(raw_trade)
            except ValueError as exc:
                sink.reject(str(exc), raw_trade)
                continue
            sink.send(event)

    return handle


def main() -> int:
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level)
    log.info("starting settings=%s", settings.redacted())

    sink = TradeSink(build_producer(settings), settings.kafka_topic)
    stop = threading.Event()
    stream = FinnhubTradeStream(settings, make_frame_handler(sink), stop_event=stop)

    def shutdown(signum: int, _frame: FrameType | None) -> None:
        log.info("shutdown_signal signal=%s sent=%d rejected=%d", signum, sink.sent, sink.rejected)
        stop.set()
        stream.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        stream.run_forever()
    finally:
        sink.flush()
        log.info("stopped sent=%d rejected=%d", sink.sent, sink.rejected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
