"""Run the data-quality suite and write a JSON report.

Entry point for ``make run-dq``. Reads the generated transactions +
customers, evaluates their rule sets, writes a combined report to
``data_quality.report_path``, and (in prod, where
``data_quality.fail_pipeline_on_error`` is true) exits non-zero if any
critical expectation failed.
"""

from __future__ import annotations

import json
import sys

from dataforge.common.config import load_config
from dataforge.common.logging_utils import get_logger
from dataforge.data_quality.engine import run_expectations
from dataforge.data_quality.rules import customer_rules, transaction_rules
from dataforge.ingestion.database.extract import extract_full

_log = get_logger("dataforge.data_quality.run")


def main() -> int:
    config = load_config()
    txn = extract_full("transactions", config=config)
    cust = extract_full("customers", config=config)

    txn_results = run_expectations(txn, transaction_rules(config), config=config)
    cust_results = run_expectations(cust, customer_rules(config), config=config)

    report = {
        "transactions": txn_results.to_report(),
        "customers": cust_results.to_report(),
    }
    report_path = config.path("data_quality.report_path")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    print(f"\nData quality report written to {report_path}\n")
    for dataset, res in report.items():
        print(f"[{dataset}] overall_passed={res['overall_passed']}")
        for check in res["checks"]:
            flag = "PASS" if check["passed"] else "FAIL"
            print(
                f"  {flag}  {check['name']:26s} "
                f"severity={check['severity']:8s} failed_rows={check['failed_rows']}"
            )

    fail_on_error = bool(config.get("data_quality.fail_pipeline_on_error", False))
    critical_failed = not (txn_results.passed and cust_results.passed)
    if fail_on_error and critical_failed:
        _log.error("critical data-quality failures; failing pipeline")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
