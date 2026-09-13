"""Integration test for the resilient API client against the mock API.

Uses the Flask app's test client (no network) by monkeypatching the
client's session ``get``/``post`` to route through ``app.test_client()``.
Exercises pagination and response validation with the deterministic
``stable=1`` path (no random 429/503).
"""

from __future__ import annotations

import pytest

from dataforge.ingestion.api import mock_api
from dataforge.ingestion.api.client import ApiClient


class _FlaskAdapter:
    """Route requests.Session-style calls to the Flask test client."""

    def __init__(self, client):
        self._c = client
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        params = dict(params or {})
        params["stable"] = "1"  # deterministic
        path = url.split("127.0.0.1:5001")[-1] if "127.0.0.1" in url else url
        return _Resp(self._c.get(path, query_string=params, headers=self.headers))

    def post(self, url, json=None, timeout=None):
        path = url.split("127.0.0.1:5001")[-1] if "127.0.0.1" in url else url
        return _Resp(self._c.post(path, json=json, headers=self.headers))


class _Resp:
    def __init__(self, flask_resp):
        self.status_code = flask_resp.status_code
        self._json = flask_resp.get_json()
        self.text = flask_resp.get_data(as_text=True)

    def json(self):
        return self._json


@pytest.fixture()
def api_client(monkeypatch):
    client = ApiClient(base_url="http://127.0.0.1:5001", token="local-dev-token")
    client.page_size = 20
    adapter = _FlaskAdapter(mock_api.app.test_client())
    adapter.headers = {"Authorization": "Bearer local-dev-token"}
    monkeypatch.setattr(client, "session", adapter)
    return client


@pytest.mark.integration
def test_api_pagination_collects_all(api_client):
    rates = api_client.get_all_rates()
    # mock API has 80 rate records (8 currencies x 10).
    assert len(rates) == 80
    assert all("rate" in r and "quote" in r for r in rates)


@pytest.mark.integration
def test_api_incremental_filter(api_client):
    # Lexical filter in the mock API's /rates endpoint.
    rates = api_client.get_all_rates(updated_since="2025-01-02T00:00:00+00:00")
    assert isinstance(rates, list)  # filter path returns a subset without error


@pytest.mark.integration
def test_api_unauthorized_is_non_retryable(monkeypatch):
    client = ApiClient(base_url="http://127.0.0.1:5001", token="wrong-token")
    client.max_retries = 2
    adapter = _FlaskAdapter(mock_api.app.test_client())
    adapter.headers = {"Authorization": "Bearer wrong-token"}
    monkeypatch.setattr(client, "session", adapter)
    from dataforge.common.errors import NonRetryableError

    with pytest.raises(NonRetryableError):
        client.get_rates_page()
