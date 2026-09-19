"""Runtime configuration, loaded from the environment.

Nothing in this project is allowed to hardcode a credential. Every secret is
read from the environment (docker compose injects it from .env, CI injects it
from GitHub secrets, production injects it from the orchestrator).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


class ConfigError(RuntimeError):
    """Raised when a required setting is missing or malformed."""


def _require(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(
            f"Missing required environment variable {name!r}. "
            f"Copy .env.example to .env and fill it in."
        )
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _csv(name: str, default: str) -> list[str]:
    raw = os.getenv(name, "").strip() or default
    return [item.strip().upper() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True, slots=True)
class Settings:
    """All producer settings in one immutable object."""

    finnhub_api_key: str
    finnhub_ws_url: str = "wss://ws.finnhub.io"
    symbols: list[str] = field(default_factory=lambda: ["AAPL"])

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "stock.trades.v1"
    kafka_linger_ms: int = 50
    kafka_batch_size: int = 65536

    # How often the reconnect loop gives up on a silent socket, in seconds.
    heartbeat_timeout_s: int = 60
    reconnect_backoff_s: int = 2
    reconnect_backoff_max_s: int = 60

    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            finnhub_api_key=_require("FINNHUB_API_KEY"),
            finnhub_ws_url=os.getenv("FINNHUB_WS_URL", "wss://ws.finnhub.io"),
            symbols=_csv("STOCK_SYMBOLS", "AAPL,MSFT,TSLA,GOOGL,AMZN"),
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "stock.trades.v1"),
            kafka_linger_ms=_int("KAFKA_LINGER_MS", 50),
            kafka_batch_size=_int("KAFKA_BATCH_SIZE", 65536),
            heartbeat_timeout_s=_int("HEARTBEAT_TIMEOUT_S", 60),
            reconnect_backoff_s=_int("RECONNECT_BACKOFF_S", 2),
            reconnect_backoff_max_s=_int("RECONNECT_BACKOFF_MAX_S", 60),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def ws_endpoint(self) -> str:
        """The full websocket URL, token included."""
        return f"{self.finnhub_ws_url}?token={self.finnhub_api_key}"

    def redacted(self) -> dict[str, object]:
        """Safe-to-log view of the settings."""
        return {
            "finnhub_ws_url": self.finnhub_ws_url,
            "finnhub_api_key": "***redacted***",
            "symbols": self.symbols,
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers,
            "kafka_topic": self.kafka_topic,
            "log_level": self.log_level,
        }
