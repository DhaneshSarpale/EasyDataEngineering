"""A local mock REST API used by the API-ingestion demo.

Simulates a foreign-exchange (FX) rates provider with realistic API
concerns so the client can demonstrate handling them:

- **auth**            requires ``Authorization: Bearer <token>``
- **pagination**      ``/rates`` returns pages via ``page`` / ``page_size``
- **rate limiting**   randomly returns HTTP 429 (retryable)
- **transient error** randomly returns HTTP 503 (retryable)
- **incremental**     ``/rates?updated_since=<iso>`` filters by timestamp
- **POST**            ``/ingest-log`` accepts a JSON body (write path)

Run standalone::

    python -m dataforge.ingestion.api.mock_api      # serves on :5001

The randomness can be disabled with ``?stable=1`` for deterministic tests.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request

TOKEN = os.environ.get("DATAFORGE_API_TOKEN", "local-dev-token")
CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD", "CHF"]

app = Flask(__name__)

_BASE = datetime(2025, 1, 1, tzinfo=timezone.utc)
_RATES = [
    {
        "id": i,
        "base": "USD",
        "quote": cur,
        "rate": round(0.5 + i * 0.01, 4),
        "updated_at": (_BASE + timedelta(hours=i)).isoformat(),
    }
    for i, cur in enumerate(CURRENCIES * 10)
]


def _authorized() -> bool:
    header = request.headers.get("Authorization", "")
    return header == f"Bearer {TOKEN}"


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/rates")
def rates():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401

    stable = request.args.get("stable") == "1"
    if not stable:
        roll = random.random()
        if roll < 0.15:
            return jsonify({"error": "rate_limited"}), 429
        if roll < 0.20:
            return jsonify({"error": "service_unavailable"}), 503

    data = _RATES
    updated_since = request.args.get("updated_since")
    if updated_since:
        data = [r for r in data if r["updated_at"] > updated_since]

    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 100))
    start = (page - 1) * page_size
    chunk = data[start : start + page_size]
    has_next = start + page_size < len(data)
    return jsonify(
        {
            "data": chunk,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": len(data),
                "has_next": has_next,
            },
        }
    )


@app.post("/ingest-log")
def ingest_log():
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or "pipeline" not in body:
        return jsonify({"error": "invalid_body"}), 400
    return jsonify({"accepted": True, "received": body}), 201


def main() -> None:
    app.run(host="127.0.0.1", port=5001, debug=False)


if __name__ == "__main__":
    main()
