"""Tests for the Twelve Data ingestion code. No live API calls are made."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from ingestion import extract, provider
from ingestion.api_client import APIClient, ApiHttpError, ApiPayloadError

FIXTURES = Path(__file__).parent / "fixtures"
KEY = "SECRET_TEST_KEY_123"

GOOD_PAYLOAD = {
    "meta": {"symbol": "AAPL", "interval": "1day"},
    "values": [
        {"datetime": "2026-10-23", "open": "349.12", "high": "351.73",
         "low": "345.75", "close": "347.73", "volume": "39496500"},
        {"datetime": "2026-10-22", "open": "348.00", "high": "350.00",
         "low": "346.00", "close": "349.00", "volume": "30000000"},
    ],
    "status": "ok",
}


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, body=None, headers=None, bad_json=False):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self._bad_json = bad_json

    def json(self):
        if self._bad_json:
            raise ValueError("not json")
        return self._body


def make_client(responses):
    """Client whose session returns the given responses (or raises exceptions) in order."""
    client = APIClient("https://api.twelvedata.com", KEY)
    client.session = MagicMock()
    client.session.get.side_effect = responses
    return client


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Never really wait during tests."""
    monkeypatch.setattr("ingestion.api_client.time.sleep", lambda s: None)


def test_success_returns_payload():
    client = make_client([FakeResponse(200, GOOD_PAYLOAD)])
    assert client.get_json("time_series", {"symbol": "AAPL"}) == GOOD_PAYLOAD
    url = client.session.get.call_args.args[0]
    assert url == "https://api.twelvedata.com/time_series"  # no doubled path


def test_api_key_is_sent_as_param():
    client = make_client([FakeResponse(200, GOOD_PAYLOAD)])
    client.get_json("time_series", {"symbol": "AAPL"})
    assert client.session.get.call_args.kwargs["params"]["apikey"] == KEY


@pytest.mark.parametrize("status", [400, 401, 404])
def test_client_errors_are_not_retried(status):
    body = {"code": status, "message": "bad request", "status": "error"}
    client = make_client([FakeResponse(status, body)])
    with pytest.raises(ApiHttpError) as info:
        client.get_json("time_series", {"symbol": "AAPL"})
    assert info.value.status_code == status
    assert client.session.get.call_count == 1


def test_rate_limit_429_is_retried_then_succeeds():
    client = make_client([
        FakeResponse(429, {"code": 429, "message": "too many", "status": "error"},
                     headers={"Retry-After": "1"}),
        FakeResponse(200, GOOD_PAYLOAD),
    ])
    assert client.get_json("time_series", {"symbol": "AAPL"}) == GOOD_PAYLOAD
    assert client.session.get.call_count == 2


def test_server_errors_exhaust_retries():
    client = make_client([FakeResponse(500, {"message": "oops"})] * 3)
    with pytest.raises(ApiHttpError):
        client.get_json("time_series", {"symbol": "AAPL"}, retries=3)
    assert client.session.get.call_count == 3


def test_connection_error_is_retried_then_succeeds():
    client = make_client([requests.ConnectionError("boom"), FakeResponse(200, GOOD_PAYLOAD)])
    assert client.get_json("time_series", {"symbol": "AAPL"}) == GOOD_PAYLOAD
    assert client.session.get.call_count == 2


def test_timeout_is_retried():
    client = make_client([requests.Timeout("slow"), requests.Timeout("slow"),
                          FakeResponse(200, GOOD_PAYLOAD)])
    client.get_json("time_series", {"symbol": "AAPL"}, retries=3)
    assert client.session.get.call_count == 3


def test_error_inside_http_200_is_rejected_without_retry():
    body = {"code": 400, "message": "bad symbol", "status": "error"}
    client = make_client([FakeResponse(200, body)])
    with pytest.raises(ApiPayloadError):
        client.get_json("time_series", {"symbol": "AAPL"})
    assert client.session.get.call_count == 1


def test_invalid_json_is_rejected():
    client = make_client([FakeResponse(200, bad_json=True)])
    with pytest.raises(ApiPayloadError):
        client.get_json("time_series", {"symbol": "AAPL"})


def test_non_object_json_is_rejected():
    client = make_client([FakeResponse(200, ["not", "an", "object"])])
    with pytest.raises(ApiPayloadError):
        client.get_json("time_series", {"symbol": "AAPL"})


def test_api_key_never_appears_in_logs_or_errors(caplog):
    leaky = requests.ConnectionError(f"failed for url: /time_series?apikey={KEY}")
    client = make_client([leaky, leaky, leaky])
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ApiHttpError) as info:
            client.get_json("time_series", {"symbol": "AAPL"}, retries=3)
    assert KEY not in caplog.text
    assert KEY not in str(info.value)


def test_build_params_full_history_has_no_start_date():
    params = provider.build_params("AAPL")
    assert params == {"symbol": "AAPL", "interval": "1day", "outputsize": 5000}


def test_build_params_incremental_has_start_date():
    params = provider.build_params("AAPL", date(2026, 10, 18))
    assert params["start_date"] == "2026-10-18"
    assert params["outputsize"] == 5000


def test_validate_rejects_empty_or_missing_values():
    with pytest.raises(ApiPayloadError):
        provider.validate_time_series({"status": "ok"}, "AAPL")
    with pytest.raises(ApiPayloadError):
        provider.validate_time_series({"values": []}, "AAPL")


def test_validate_rejects_rows_missing_fields():
    payload = {"values": [{"datetime": "2026-10-23", "close": "1"}]}
    with pytest.raises(ApiPayloadError):
        provider.validate_time_series(payload, "AAPL")


def test_fetch_symbol_returns_envelope_without_api_key():
    client = make_client([FakeResponse(200, GOOD_PAYLOAD)])
    result = provider.fetch_symbol("AAPL", client=client)
    assert result["symbol"] == "AAPL"
    assert result["payload"] == GOOD_PAYLOAD
    assert "apikey" not in result["request_params"]
    assert result["http_status"] == 200
    assert result["fetched_at"].endswith("+00:00")  # UTC


def test_extract_incremental_first_run_requests_full_history(monkeypatch):
    monkeypatch.setattr(extract, "get_latest_stored_date", lambda s: None)
    client = make_client([FakeResponse(200, GOOD_PAYLOAD)])
    rows = extract.extract_incremental("AAPL", client=client)
    assert len(rows) == 2
    assert "start_date" not in client.session.get.call_args.kwargs["params"]


def test_extract_incremental_uses_watermark_minus_overlap(monkeypatch):
    monkeypatch.setattr(extract, "get_latest_stored_date", lambda s: date(2026, 10, 20))
    client = make_client([FakeResponse(200, GOOD_PAYLOAD)])
    extract.extract_incremental("AAPL", client=client)
    assert client.session.get.call_args.kwargs["params"]["start_date"] == "2026-10-15"


@pytest.mark.skipif(not (FIXTURES / "good_AAPL.json").exists(), reason="fixture missing")
def test_real_sample_response_passes_validation():
    payload = json.loads((FIXTURES / "good_AAPL.json").read_text())
    rows = provider.validate_time_series(payload, "AAPL")
    assert len(rows) > 0
