"""Finnhub websocket client with subscription management and reconnection."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Callable

from producer.config import Settings

log = logging.getLogger(__name__)

FrameHandler = Callable[[dict[str, Any]], None]


def subscribe_payloads(symbols: list[str]) -> list[str]:
    """The JSON frames that subscribe this client to each symbol."""
    return [json.dumps({"type": "subscribe", "symbol": s}) for s in symbols]


class FinnhubTradeStream:
    """Long-lived websocket consumer.

    Reconnects with capped exponential backoff, and treats a socket that has
    gone quiet for longer than ``heartbeat_timeout_s`` as dead -- Finnhub
    sometimes leaves a half-open connection that never raises.
    """

    def __init__(
        self,
        settings: Settings,
        on_frame: FrameHandler,
        stop_event: threading.Event | None = None,
    ) -> None:
        self._settings = settings
        self._on_frame = on_frame
        self._stop = stop_event or threading.Event()
        self._last_message_at = time.monotonic()

    def stop(self) -> None:
        self._stop.set()

    @property
    def is_stale(self) -> bool:
        idle = time.monotonic() - self._last_message_at
        return idle > self._settings.heartbeat_timeout_s

    def _handle_raw(self, raw: str) -> None:
        self._last_message_at = time.monotonic()
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("undecodable_frame payload=%.200s", raw)
            return
        if frame.get("type") == "error":
            log.error("finnhub_error message=%s", frame.get("msg"))
            return
        if frame.get("type") == "ping":
            return
        self._on_frame(frame)

    def run_forever(self) -> None:
        """Blocking loop. Returns only when ``stop()`` is called."""
        import websocket  # lazy import keeps unit tests dependency-free

        backoff = self._settings.reconnect_backoff_s

        while not self._stop.is_set():
            try:
                log.info("ws_connecting url=%s", self._settings.finnhub_ws_url)
                ws = websocket.create_connection(
                    self._settings.ws_endpoint, timeout=self._settings.heartbeat_timeout_s
                )
                for payload in subscribe_payloads(self._settings.symbols):
                    ws.send(payload)
                log.info("ws_subscribed symbols=%s", ",".join(self._settings.symbols))

                backoff = self._settings.reconnect_backoff_s
                self._last_message_at = time.monotonic()

                while not self._stop.is_set():
                    try:
                        self._handle_raw(ws.recv())
                    except websocket.WebSocketTimeoutException:
                        if self.is_stale:
                            raise ConnectionError("no frames within heartbeat window")
                        continue
                ws.close()
            except Exception as exc:  # noqa: BLE001 - loop must survive anything
                if self._stop.is_set():
                    break
                log.warning("ws_disconnected error=%s retry_in=%ss", exc, backoff)
                self._stop.wait(backoff)
                backoff = min(backoff * 2, self._settings.reconnect_backoff_max_s)
