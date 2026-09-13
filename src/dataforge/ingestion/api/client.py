"""Resilient REST API client for ingestion.

Demonstrates the concerns every production API extractor must handle:

- **authentication**   Bearer token from env / Secrets Manager
- **timeout handling** per-request timeout so a hung server can't stall us
- **retry + backoff**  429/503/connection errors are retried with jitter
- **rate limiting**    429 is treated as retryable (honours the pattern)
- **pagination**       follows ``pagination.has_next`` until exhausted
- **incremental**      ``updated_since`` param for delta pulls
- **validation**       response shape is validated before use

Retryable HTTP statuses raise :class:`RetryableError` so the shared
``retry`` decorator backs off; 4xx (except 429) raise
:class:`NonRetryableError` and fail fast.
"""

from __future__ import annotations

import os
from typing import Any

import requests

from dataforge.common.config import Config, load_config
from dataforge.common.errors import NonRetryableError, RetryableError, SchemaValidationError
from dataforge.common.logging_utils import get_logger
from dataforge.common.retry import retry

_log = get_logger("dataforge.ingestion.api")

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class ApiClient:
    """Thin resilient wrapper over ``requests`` for the FX rates API."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        config: Config | None = None,
    ) -> None:
        self.config = config or load_config()
        self.base_url = (base_url or self.config.get("api.base_url")).rstrip("/")
        self.token = token or os.environ.get("DATAFORGE_API_TOKEN", "local-dev-token")
        self.timeout = float(self.config.get("api.timeout_seconds", 10))
        self.max_retries = int(self.config.get("api.max_retries", 5))
        self.backoff = float(self.config.get("api.backoff_base_seconds", 0.5))
        self.page_size = int(self.config.get("api.page_size", 100))
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Single GET with timeout + status-aware error classification."""

        @retry(max_attempts=self.max_retries, base_delay=self.backoff)
        def _do() -> dict[str, Any]:
            try:
                resp = self.session.get(
                    f"{self.base_url}{path}", params=params, timeout=self.timeout
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                raise RetryableError(f"network error: {exc}") from exc

            if resp.status_code in _RETRYABLE_STATUS:
                raise RetryableError(f"retryable status {resp.status_code}")
            if resp.status_code == 401:
                raise NonRetryableError("unauthorized - check API token")
            if resp.status_code >= 400:
                raise NonRetryableError(f"client error {resp.status_code}: {resp.text}")
            return resp.json()

        return _do()

    def get_rates_page(self, page: int = 1, updated_since: str | None = None) -> dict[str, Any]:
        """Fetch a single page of FX rates and validate its shape."""
        params: dict[str, Any] = {"page": page, "page_size": self.page_size}
        if updated_since:
            params["updated_since"] = updated_since
        payload = self._get("/rates", params=params)
        self._validate_rates_payload(payload)
        return payload

    def get_all_rates(self, updated_since: str | None = None) -> list[dict[str, Any]]:
        """Follow pagination and return all rate records (incremental-aware)."""
        page = 1
        collected: list[dict[str, Any]] = []
        while True:
            payload = self.get_rates_page(page=page, updated_since=updated_since)
            collected.extend(payload["data"])
            if not payload["pagination"].get("has_next"):
                break
            page += 1
        _log.info(
            "api ingestion complete",
            extra={"records": len(collected), "pages": page, "since": updated_since},
        )
        return collected

    def post_ingest_log(self, body: dict[str, Any]) -> dict[str, Any]:
        """POST example: send a run summary to the API's write endpoint."""

        @retry(max_attempts=self.max_retries, base_delay=self.backoff)
        def _do() -> dict[str, Any]:
            try:
                resp = self.session.post(
                    f"{self.base_url}/ingest-log", json=body, timeout=self.timeout
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                raise RetryableError(f"network error: {exc}") from exc
            if resp.status_code in _RETRYABLE_STATUS:
                raise RetryableError(f"retryable status {resp.status_code}")
            if resp.status_code >= 400:
                raise NonRetryableError(f"post failed {resp.status_code}: {resp.text}")
            return resp.json()

        return _do()

    @staticmethod
    def _validate_rates_payload(payload: dict[str, Any]) -> None:
        """Validate the API response structure before trusting it."""
        if not isinstance(payload, dict) or "data" not in payload:
            raise SchemaValidationError("API response missing 'data'")
        if "pagination" not in payload:
            raise SchemaValidationError("API response missing 'pagination'")
        for rec in payload["data"]:
            if "rate" not in rec or "quote" not in rec:
                raise SchemaValidationError(f"malformed rate record: {rec}")
