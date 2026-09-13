"""A small, dependency-free expectation engine (Great-Expectations-style).

Why a custom engine?
    Great Expectations and AWS Glue Data Quality are excellent, but they add
    heavy dependencies / require AWS. This lightweight engine expresses the
    same idea - declarative *expectations* evaluated against a DataFrame -
    so the concepts are demonstrable locally and in CI without any extra
    infrastructure. The Great Expectations and Glue DQ equivalents are
    provided under ``data_quality/great_expectations/`` and
    ``data_quality/glue_dq/``.

Each :class:`Expectation` is either:
    - **row-level**  : produces a boolean Series (True = row passes). Failing
      rows can be quarantined individually.
    - **table-level**: produces a single pass/fail (e.g. uniqueness).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from dataforge.common.config import Config
from dataforge.common.logging_utils import get_logger

_log = get_logger("dataforge.data_quality")


@dataclass
class Expectation:
    """A single named data-quality rule.

    Attributes:
        name: Human-readable rule id (e.g. ``amount_non_negative``).
        column: The column the rule concerns (informational; may be "").
        row_check: Optional callable df -> boolean Series (True = pass).
        table_check: Optional callable df -> bool for whole-table rules.
        severity: ``critical`` rules can fail the pipeline; ``warn`` only log.
    """

    name: str
    column: str = ""
    row_check: Callable[[pd.DataFrame], pd.Series] | None = None
    table_check: Callable[[pd.DataFrame], bool] | None = None
    severity: str = "critical"


@dataclass
class ExpectationResults:
    """Outcome of evaluating a set of expectations against a DataFrame."""

    total_rows: int
    checks: list[dict[str, Any]] = field(default_factory=list)
    _row_masks: dict[str, pd.Series] = field(default_factory=dict, repr=False)

    @property
    def passed(self) -> bool:
        """True if every critical expectation passed."""
        return all(c["passed"] for c in self.checks if c["severity"] == "critical")

    def row_failure_mask(self, df: pd.DataFrame) -> pd.Series:
        """Boolean Series: True where a row failed ANY row-level expectation."""
        mask = pd.Series(False, index=df.index)
        for m in self._row_masks.values():
            mask = mask | (~m)
        return mask

    def reason_for_rows(self, df: pd.DataFrame) -> pd.Series:
        """Series of semicolon-joined failed-rule names per row."""
        reasons = pd.Series("", index=df.index)
        for name, m in self._row_masks.items():
            failed = ~m
            reasons.loc[failed] = (reasons.loc[failed] + f"{name};").str.lstrip()
        return reasons.str.rstrip(";")

    def to_report(self) -> dict[str, Any]:
        """JSON-serialisable summary suitable for writing a DQ report."""
        return {
            "total_rows": self.total_rows,
            "overall_passed": self.passed,
            "checks": self.checks,
        }


def run_expectations(
    df: pd.DataFrame,
    expectations: list[Expectation],
    *,
    config: Config | None = None,
) -> ExpectationResults:
    """Evaluate all ``expectations`` against ``df``."""
    results = ExpectationResults(total_rows=len(df))
    for exp in expectations:
        if exp.row_check is not None:
            mask = exp.row_check(df).fillna(False).astype(bool)
            failed = int((~mask).sum())
            results._row_masks[exp.name] = mask
            results.checks.append(
                {
                    "name": exp.name,
                    "column": exp.column,
                    "type": "row",
                    "severity": exp.severity,
                    "failed_rows": failed,
                    "passed": failed == 0,
                }
            )
        elif exp.table_check is not None:
            ok = bool(exp.table_check(df))
            results.checks.append(
                {
                    "name": exp.name,
                    "column": exp.column,
                    "type": "table",
                    "severity": exp.severity,
                    "failed_rows": 0 if ok else results.total_rows,
                    "passed": ok,
                }
            )
    _log.info(
        "expectations evaluated",
        extra={
            "total_rows": results.total_rows,
            "checks": len(results.checks),
            "overall_passed": results.passed,
        },
    )
    return results
