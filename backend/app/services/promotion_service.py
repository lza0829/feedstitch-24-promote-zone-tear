"""Promotion orchestration: reconcile then atomically move bronze → canonical.

Invariants enforced here:

* a row is written to ``canonical_items`` **only** when its run passed the gate;
* a failed or aborted batch leaves every candidate row in the bronze/pending
  zone, annotated with ``failure_reason`` — nothing is silently dropped and
  nothing is reported as a success it did not earn;
* the batch report's candidate/success/failure triple is derived from the same
  row sets that were reconciled against the ingest report, and is cross-checked
  against both zones after the write.
"""

from __future__ import annotations

from typing import Any

from app.db import connect
from app.modules.bronze_silver_promote import reconcile_groups
from app.repositories import bronze as bronze_repo
from app.repositories import canonical as canon_repo
from app.repositories import promotions as promotions_repo
from app.repositories import runs as runs_repo


def _group_for_run(conn, run_id: int) -> tuple[Any, list[dict], list[dict]]:
    run = runs_repo.get_run(conn, run_id)
    recorded = bronze_repo.list_by_run(conn, run_id)
    pending = [r for r in recorded if r["status"] == "pending"]
    return run, recorded, pending


def _rank(row: dict[str, Any]) -> tuple[int, str, int]:
    return (int(row["source_revision"]), row["event_ts"], int(row["id"]))


def _expected_key_state(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Winning row per business key, mirroring the apply order."""
    winners: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = winners.get(row["business_key"])
        if current is None or _rank(row) > _rank(current):
            winners[row["business_key"]] = row
    return winners


def _build_report(
    conn,
    *,
    scope: str,
    checks: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    success_rows: list[dict[str, Any]],
    failure_rows: list[dict[str, Any]],
    message: str,
) -> dict[str, Any]:
    success_ids = {r["id"] for r in success_rows}
    failure_ids = {r["id"] for r in failure_rows}
    return {
        "scope": scope,
        "run_ids": sorted({c["run_id"] for c in checks if c.get("run_id") is not None}),
        # The authoritative triple: every candidate is either promoted or kept.
        "candidate_count": len(candidate_rows),
        "success_count": len(success_rows),
        "failure_count": len(failure_rows),
        # Same-source cross references: ingest report vs. the two zones.
        "ingest_expected_count": sum(int(c["expected_count"] or 0) for c in checks),
        "already_promoted_count": sum(
            int(c.get("already_promoted_count") or 0) for c in checks
        ),
        "candidate_keys": [r["business_key"] for r in candidate_rows],
        "candidate_row_ids": [r["id"] for r in candidate_rows],
        "success_keys": [r["business_key"] for r in success_rows],
        "success_row_ids": sorted(success_ids),
        "failure_keys": [r["business_key"] for r in failure_rows],
        "failure_row_ids": sorted(failure_ids),
        "bronze_pending_count_after": bronze_repo.count(conn, status="pending"),
        "canonical_count_after": canon_repo.count_items(conn),
        "candidates": [
            {
                "bronze_row_id": r["id"],
                "run_id": r["run_id"],
                "business_key": r["business_key"],
                "status": "promoted" if r["id"] in success_ids else "pending",
            }
            for r in candidate_rows
        ],
        "checks": checks,
        "message": message,
    }


def _resolve_candidates(
    conn, candidates: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split candidates into rows to write vs. rows already superseded.

    Canonical is monotonic: a pending candidate older than the canonical state
    for its key must not regress the official zone. It is still resolved (the
    row is promoted) but reported as superseded rather than silently skipped.
    """
    to_write: list[dict[str, Any]] = []
    superseded: list[dict[str, Any]] = []
    for key, row in _expected_key_state(candidates).items():
        canon_row = canon_repo.get_by_key(conn, key)
        if canon_row is not None and _rank(canon_row) > _rank(row):
            superseded.append(row)
        else:
            to_write.append(row)
    return to_write, superseded


def _verify_success(
    conn,
    candidates: list[dict[str, Any]],
    superseded: list[dict[str, Any]],
) -> dict[str, Any]:
    """Cross-check the write against the two zones before declaring success.

    Canonical must hold, per key, the newest row among everything known for
    that key (candidates plus whatever canonical already had).
    """
    canonical = {
        key: canon_repo.get_by_key(conn, key)
        for key in {r["business_key"] for r in candidates}
    }
    known = candidates + [row for row in canonical.values() if row is not None]
    expected = _expected_key_state(known)
    for key, row in expected.items():
        canon_row = canonical.get(key)
        if canon_row is None:
            raise RuntimeError(f"canonical row missing after promotion: {key}")
        if (int(canon_row["source_revision"]), canon_row["event_ts"]) != (
            int(row["source_revision"]),
            row["event_ts"],
        ):
            raise RuntimeError(
                f"canonical state mismatch for {key}: "
                f"got rev {canon_row['source_revision']}, "
                f"expected rev {row['source_revision']}"
            )
    still_pending = {r["id"] for r in bronze_repo.list_pending(conn)} & {
        r["id"] for r in candidates
    }
    if still_pending:
        raise RuntimeError(
            f"rows still pending after promotion: {sorted(still_pending)}"
        )
    return {
        "canonical_keys_after": sorted(expected),
        "canonical_key_count_after": len(expected),
        "pending_candidate_ids_after": [],
        "superseded_row_ids": sorted(r["id"] for r in superseded),
        "superseded_keys": sorted({r["business_key"] for r in superseded}),
    }


def _verify_failure(
    conn,
    candidate_rows: list[dict[str, Any]],
    *,
    canonical_keys_before: set[str],
) -> dict[str, Any]:
    """Confirm every candidate is still pending and no new key leaked in."""
    pending_ids = {r["id"] for r in bronze_repo.list_pending(conn)}
    candidate_ids = {r["id"] for r in candidate_rows}
    missing = candidate_ids - pending_ids
    if missing:
        raise RuntimeError(
            f"failed batch lost candidate rows from bronze/pending: {sorted(missing)}"
        )
    canonical_keys_after = {r["business_key"] for r in canon_repo.list_items(conn)}
    leaked = canonical_keys_after - canonical_keys_before
    if leaked:
        raise RuntimeError(
            f"failed batch wrote keys into canonical: {sorted(leaked)}"
        )
    return {
        "candidate_ids_still_pending": sorted(candidate_ids),
        "canonical_keys_added": [],
    }


def _record_failure(
    conn,
    *,
    scope: str,
    batch_id: int,
    checks: list[dict[str, Any]],
    candidate_rows: list[dict[str, Any]],
    message: str,
    canonical_keys_before: set[str],
) -> dict[str, Any]:
    """Leave every candidate in bronze/pending with a per-row reason."""
    reason_by_run: dict[Any, str] = {}
    for c in checks:
        if c["passed"]:
            reason = "batch aborted: another run failed reconciliation"
        else:
            reason = "; ".join(c["issues"]) or "reconciliation failed"
        reason_by_run[c.get("run_id")] = reason

    by_run: dict[Any, list[int]] = {}
    for row in candidate_rows:
        by_run.setdefault(row["run_id"], []).append(row["id"])
    for rid, row_ids in by_run.items():
        bronze_repo.mark_failed(
            conn,
            row_ids=row_ids,
            promotion_id=batch_id,
            reason=reason_by_run.get(rid, message),
        )

    report = _build_report(
        conn,
        scope=scope,
        checks=checks,
        candidate_rows=candidate_rows,
        success_rows=[],
        failure_rows=candidate_rows,
        message=message,
    )
    report["failure_summary"] = [
        {"run_id": c.get("run_id"), "issues": c["issues"]}
        for c in checks
        if not c["passed"]
    ]
    report["zone_check"] = _verify_failure(
        conn,
        candidate_rows,
        canonical_keys_before=canonical_keys_before,
    )
    promotions_repo.finish_batch(
        conn,
        batch_id,
        status="failed",
        candidate_count=len(candidate_rows),
        success_count=0,
        failure_count=len(candidate_rows),
        report=report,
    )
    conn.commit()
    return promotions_repo.get_batch(conn, batch_id) or {}


def promote(run_id: int | None = None) -> dict[str, Any]:
    """Run the promotion gate for one ingest run or for all pending rows.

    All-or-nothing: if any run in scope fails reconciliation, no canonical row
    is written. Every targeted bronze row stays in the pending zone and carries
    a failure reason.
    """
    conn = connect()
    try:
        scope = "run" if run_id is not None else "pending"

        if run_id is not None:
            if runs_repo.get_run(conn, run_id) is None:
                raise ValueError("run not found")
            groups = [_group_for_run(conn, run_id)]
        else:
            group_run_ids = sorted(
                {r["run_id"] for r in bronze_repo.list_pending(conn)}
            )
            groups = [_group_for_run(conn, rid) for rid in group_run_ids]

        checks = reconcile_groups(groups)
        candidate_rows = [r for _, _, pending in groups for r in pending]
        candidate_ids = [r["id"] for r in candidate_rows]
        all_passed = all(c["passed"] for c in checks)
        canonical_keys_before = {r["business_key"] for r in canon_repo.list_items(conn)}

        batch_id = promotions_repo.create_batch(
            conn,
            scope=scope,
            run_id=run_id,
            candidate_count=len(candidate_rows),
        )

        if not all_passed:
            return _record_failure(
                conn,
                scope=scope,
                batch_id=batch_id,
                checks=checks,
                candidate_rows=candidate_rows,
                message="reconciliation failed; rows kept in bronze/pending zone",
                canonical_keys_before=canonical_keys_before,
            )

        # Gate passed: resolve which candidates actually advance canonical,
        # then apply them in one transaction. Candidates older than the
        # canonical state for their key are still marked promoted (they have
        # been decided), but never regress the official zone.
        to_write, superseded = _resolve_candidates(conn, candidate_rows)
        try:
            apply_order = sorted(
                to_write,
                key=lambda r: (
                    r["business_key"],
                    int(r["source_revision"]),
                    r["event_ts"],
                    r["id"],
                ),
            )
            for row in apply_order:
                canon_repo.upsert_item(conn, row, row["source_id"])
            bronze_repo.mark_promoted(
                conn,
                row_ids=candidate_ids,
                promotion_id=batch_id,
            )

            # Verify the write against both zones instead of trusting the loop
            # counter: every key must hold the winning candidate revision in
            # canonical, and no candidate may still be pending.
            report = _build_report(
                conn,
                scope=scope,
                checks=checks,
                candidate_rows=candidate_rows,
                success_rows=candidate_rows,
                failure_rows=[],
                message=(
                    "promoted to canonical" if candidate_rows
                    else "no pending bronze rows in scope"
                ),
            )
            report["zone_check"] = _verify_success(
                conn, candidate_rows, superseded
            )
            promotions_repo.finish_batch(
                conn,
                batch_id,
                status="passed",
                candidate_count=len(candidate_rows),
                success_count=len(candidate_rows),
                failure_count=0,
                report=report,
            )
            conn.commit()
            return promotions_repo.get_batch(conn, batch_id) or {}
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            error = f"promotion write failed: {exc}"
            checks_with_error = [
                {**c, "passed": False, "issues": c["issues"] + [error]}
                for c in checks
            ]
            # The rolled-back transaction also removed this batch id; recreate
            # it so the failure is durable and auditable.
            batch_id = promotions_repo.create_batch(
                conn,
                scope=scope,
                run_id=run_id,
                candidate_count=len(candidate_rows),
            )
            return _record_failure(
                conn,
                scope=scope,
                batch_id=batch_id,
                checks=checks_with_error,
                candidate_rows=candidate_rows,
                message=error,
                canonical_keys_before=canonical_keys_before,
            )
    finally:
        conn.close()


def list_promotions() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return promotions_repo.list_batches(conn)
    finally:
        conn.close()


def get_promotion(batch_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        return promotions_repo.get_batch(conn, batch_id)
    finally:
        conn.close()
