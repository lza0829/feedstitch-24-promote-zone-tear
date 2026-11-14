"""Bronze → Silver promotion gate (pure reconciliation, no DB access).

The gate compares the bronze rows a run actually left behind against the
ingest report persisted for that run:

* row count must match the report's accepted-for-promotion count;
* the business-key multiset must match the report's key list;
* when the report carries bronze row ids, they must match the recorded rows;
* the report's key-list length must agree with its own count;
* the run must have finished successfully;
* every recorded row must carry a promotable disposition.

Only when every check passes may the caller write rows to ``canonical_items``.
Failures are reported per run and the rows stay in the bronze/pending zone.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

PROMOTABLE_DISPOSITIONS = ("insert", "upsert")


def _report_expected_count(report: dict[str, Any]) -> int | None:
    value = report.get("bronze_candidate_count")
    if value is None:
        value = report.get("upserted_count")
    return int(value) if value is not None else None


def reconcile_run(
    run: dict[str, Any] | None,
    recorded_rows: list[dict[str, Any]],
    pending_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Reconcile one ingest run's bronze rows against its ingest report.

    ``recorded_rows`` contains every bronze row for the run (pending + already
    promoted), so re-running a promotion does not spuriously fail. The subset
    that will actually be written on success is ``pending_rows``.
    """
    issues: list[str] = []
    run_id = recorded_rows[0]["run_id"] if recorded_rows else (run or {}).get("id")
    recorded_ids = {r["id"] for r in recorded_rows}
    pending_ids = {r["id"] for r in pending_rows}

    base = {
        "run_id": run_id,
        "expected_count": None,
        "recorded_count": len(recorded_rows),
        "pending_count": len(pending_rows),
        "already_promoted_count": len(recorded_rows) - len(pending_rows),
        "expected_keys": None,
        "recorded_keys": [r["business_key"] for r in recorded_rows],
        "pending_keys": [r["business_key"] for r in pending_rows],
    }

    if run is None:
        issues.append(f"run {run_id}: ingest report not found")
        return {**base, "passed": False, "issues": issues}

    report = run.get("report") or {}
    status = run.get("status")

    expected_count = _report_expected_count(report)
    expected_keys = report.get("bronze_business_keys")
    recorded_keys = [r["business_key"] for r in recorded_rows]
    base["expected_count"] = expected_count
    base["expected_keys"] = expected_keys if isinstance(expected_keys, list) else None

    if status != "ok":
        issues.append(f"run {run_id}: ingest status is {status!r}, expected 'ok'")
    if expected_count is None:
        issues.append(f"run {run_id}: ingest report has no accepted candidate count")
    elif expected_count != len(recorded_rows):
        issues.append(
            f"run {run_id}: bronze row count {len(recorded_rows)} != "
            f"ingest report count {expected_count}"
        )
    if not isinstance(expected_keys, list):
        issues.append(f"run {run_id}: ingest report has no bronze_business_keys list")
    else:
        if expected_count is not None and len(expected_keys) != expected_count:
            issues.append(
                f"run {run_id}: ingest report key list length {len(expected_keys)} != "
                f"reported count {expected_count}"
            )
        if Counter(recorded_keys) != Counter(expected_keys):
            missing = sorted((Counter(expected_keys) - Counter(recorded_keys)).elements())
            extra = sorted((Counter(recorded_keys) - Counter(expected_keys)).elements())
            detail = []
            if missing:
                detail.append(f"missing={missing}")
            if extra:
                detail.append(f"extra={extra}")
            issues.append(
                f"run {run_id}: business-key set mismatch "
                f"({'; '.join(detail) or 'multiset differs'})"
            )

    expected_ids = report.get("bronze_row_ids")
    if isinstance(expected_ids, list):
        expected_id_set = {int(x) for x in expected_ids}
        if expected_id_set != recorded_ids:
            issues.append(
                f"run {run_id}: bronze row ids differ from ingest report "
                f"(missing={sorted(recorded_ids - expected_id_set)}, "
                f"extra={sorted(expected_id_set - recorded_ids)})"
            )

    bad_dispositions = [
        r for r in recorded_rows if r.get("disposition") not in PROMOTABLE_DISPOSITIONS
    ]
    if bad_dispositions:
        issues.append(
            f"run {run_id}: non-promotable disposition(s) present: "
            + ", ".join(sorted({str(r.get("disposition")) for r in bad_dispositions}))
        )

    if not pending_ids <= recorded_ids:
        issues.append(
            f"run {run_id}: pending ids not in recorded set "
            f"{sorted(pending_ids - recorded_ids)}"
        )

    return {**base, "passed": not issues, "issues": issues}


def reconcile_groups(
    groups: list[tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]],
) -> list[dict[str, Any]]:
    return [reconcile_run(run, recorded, pending) for run, recorded, pending in groups]
