from __future__ import annotations

import pytest

from producer.config import ConfigError, Settings


def test_missing_api_key_raises(monkeypatch):
    monkeypatch.delenv("FINNHUB_API_KEY", raising=False)
    with pytest.raises(ConfigError, match="FINNHUB_API_KEY"):
        Settings.from_env()


def test_symbols_are_parsed_and_uppercased(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "abc")
    monkeypatch.setenv("STOCK_SYMBOLS", " aapl , msft,, tsla ")
    assert Settings.from_env().symbols == ["AAPL", "MSFT", "TSLA"]


def test_defaults_are_applied(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "abc")
    monkeypatch.delenv("STOCK_SYMBOLS", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    settings = Settings.from_env()
    assert settings.kafka_topic == "stock.trades.v1"
    assert "AAPL" in settings.symbols


def test_ws_endpoint_includes_token(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "secret-token")
    assert Settings.from_env().ws_endpoint.endswith("?token=secret-token")


def test_redacted_never_leaks_the_api_key(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "secret-token")
    dumped = str(Settings.from_env().redacted())
    assert "secret-token" not in dumped
    assert "redacted" in dumped


def test_non_integer_setting_is_rejected(monkeypatch):
    monkeypatch.setenv("FINNHUB_API_KEY", "abc")
    monkeypatch.setenv("KAFKA_LINGER_MS", "fast")
    with pytest.raises(ConfigError):
        Settings.from_env()
