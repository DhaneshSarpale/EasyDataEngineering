"""Configuration loading.

Environment-specific values (bucket names, regions, thresholds) live in
``config/<env>.yaml`` - never in code. This module loads the right file
based on the ``DATAFORGE_ENV`` environment variable (default ``dev``)
and exposes a small typed accessor with dotted-path lookups.

Secrets are intentionally *not* part of config files. They are resolved
at runtime from environment variables or AWS Secrets Manager. See
``dataforge.common.secrets``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cache, lru_cache
from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    """Return the repository root (three levels up from this file)."""
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Config:
    """Immutable wrapper around the parsed YAML config.

    Use :meth:`get` for dotted-path access with an optional default::

        cfg = load_config()
        cfg.get("s3.raw_bucket")
        cfg.get("streaming.fraud.high_value_threshold", 5000)
    """

    env: str
    raw: dict[str, Any]
    repo_root: Path

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Look up ``a.b.c`` in the nested config, returning ``default`` if absent."""
        node: Any = self.raw
        for part in dotted_key.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def require(self, dotted_key: str) -> Any:
        """Like :meth:`get` but raises if the key is missing."""
        sentinel = object()
        value = self.get(dotted_key, sentinel)
        if value is sentinel:
            raise KeyError(f"Required config key '{dotted_key}' missing in env '{self.env}'")
        return value

    def path(self, dotted_key: str) -> Path:
        """Resolve a config value as a filesystem path relative to the repo root."""
        value = self.require(dotted_key)
        p = Path(value)
        return p if p.is_absolute() else self.repo_root / p


@cache
def load_config(env: str | None = None) -> Config:
    """Load and cache the config for ``env`` (or ``$DATAFORGE_ENV``)."""
    env = env or os.environ.get("DATAFORGE_ENV", "dev")
    root = _repo_root()
    config_file = root / "config" / f"{env}.yaml"
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_file}. "
            f"Expected one of dev.yaml / prod.yaml under config/."
        )
    with config_file.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return Config(env=env, raw=raw, repo_root=root)


@lru_cache(maxsize=1)
def load_data_profiles() -> dict[str, Any]:
    """Load the synthetic-data volume profiles (``config/data_profiles.yaml``)."""
    root = _repo_root()
    profiles_file = root / "config" / "data_profiles.yaml"
    with profiles_file.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}
