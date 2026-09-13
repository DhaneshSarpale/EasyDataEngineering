#!/usr/bin/env python3
"""Convenience wrapper so the README's `python scripts/generate_data.py` works.

Delegates to the packaged CLI ``dataforge.data_generation.generate``.
Ensures ``src/`` is importable when run directly from a checkout without
an editable install.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataforge.data_generation.generate import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
