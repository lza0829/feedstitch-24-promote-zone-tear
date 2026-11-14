"""End-to-end tests for the bronze → silver tear: ingest, gate, and reports.

Every test cross-checks three sources of truth:
  1. the ingest run report (``runs.report_json``),
  2. the promotion batch report (``promotion_batches.report_json``),
  3. the actual row sets in the bronze/pending zone and canonical.
"""

import pytest

from app.db import connect
from app.repositories import bronze as bronze_repo
from app.repositories import canonical as canon_repo
from app.repositories import promotions as promotions_repo
from app.repositories import runs as runs_repo
from app.services import ingest_service, promotion_service


def _zone_state(conn):
    pending = bronze_repo.list_pending(conn)
    promoted = bronze_repo.list_rows(conn, status="promoted")
    canonical = canon_repo.list_items(conn)
    return {
        "pending": pending,
        "promoted": promoted,
        "canonical": canonical,
        "pending_ids": {r["id"] for r in pending},
        "promoted_ids": {r["id"] for r in promoted},
        "canonical_keys": {r["business_key"] for r in canonical},
    }


def _assert_no_duplicate_lifecycle(conn):
    state = _zone_state(conn)
    assert not state["pending_ids"] & state["promoted_ids"], "row in two statuses"


def _ingest(source_id=1, mode="incremental"):
    return ingest_service.run_ingest(source_id, mode)


def test_ingest_never_writes_canonical(fresh_db):
    run = _ingest(1)
    conn = connect()
    try:
        assert run["status"] == "ok"
        report = run["report"]
        assert report["bronze_candidate_count"] == 3
        assert report["accepted_count"] == 3
        # The official zone stays empty until the gate passes.
        assert canon_repo.count_items(conn) == 0
        pending = bronze_repo.list_pending(conn, run_id=run["id"])
        assert len(pending) == 3
        assert [r["business_key"] for r in pending] == report["bronze_business_keys"]
        assert [r["id"] for r in pending] == report["bronze_row_ids"]
        assert report["bronze_pending_count_for_run"] == 3
    finally:
        conn.close()


def test_promotion_success_path_moves_rows_and_report_reconciles(fresh_db):
    run = _ingest(1)
    ingest_report = run["report"]

    batch = promotion_service.promote(run_id=run["id"])

    conn = connect()
    try:
        state = _zone_state(conn)
        assert batch["status"] == "passed"
        assert batch["candidate_count"] == 3
        assert batch["success_count"] == 3
        assert batch["failure_count"] == 0
        report = batch["report"]

        # The gate wrote exactly the ingested accepted keys into canonical.
        assert state["canonical_keys"] == set(ingest_report["bronze_business_keys"])
        assert report["candidate_keys"] == ingest_report["bronze_business_keys"]
        assert report["success_keys"] == ingest_report["bronze_business_keys"]
        assert report["failure_keys"] == []
        assert report["candidate_row_ids"] == ingest_report["bronze_row_ids"]
        assert report["success_row_ids"] == ingest_report["bronze_row_ids"]
        assert report["failure_row_ids"] == []

        # Rows left the pending zone and carry the promoted mark.
        assert state["pending"] == []
        assert state["promoted_ids"] == set(ingest_report["bronze_row_ids"])
        assert report["bronze_pending_count_after"] == 0
        assert report["canonical_count_after"] == len(state["canonical"])
        assert report["already_promoted_count"] == 0
        assert report["ingest_expected_count"] == 3

        # Triple is same-source: candidate = success + failure.
        assert (batch["success_count"] + batch["failure_count"]) == batch["candidate_count"]
        assert len(state["canonical"]) == batch["success_count"]

        _assert_no_duplicate_lifecycle(conn)
    finally:
        conn.close()


def test_promotion_failure_keeps_rows_pending_with_reason(fresh_db):
    run = _ingest(1)
    ingest_report = run["report"]

    # Corrupt the persisted ingest report (simulates a torn ingest): report
    # claims one more candidate than was actually written to bronze.
    conn = connect()
    try:
        tampered = dict(ingest_report)
        tampered["bronze_candidate_count"] = ingest_report["bronze_candidate_count"] + 1
        runs_repo.finish_run(
            conn,
            run["id"],
            status="ok",
            parsed=ingest_report["parsed_count"],
            upserted=ingest_report["upserted_count"],
            conflicts=ingest_report["conflict_count"],
            skipped=ingest_report["skipped_count"],
            report=tampered,
        )
        conn.commit()
    finally:
        conn.close()

    batch = promotion_service.promote(run_id=run["id"])

    conn = connect()
    try:
        state = _zone_state(conn)
        assert batch["status"] == "failed"
        assert batch["candidate_count"] == 3
        assert batch["success_count"] == 0
        assert batch["failure_count"] == 3
        report = batch["report"]

        # No silent loss: all candidates remain in bronze/pending.
        assert len(state["pending"]) == 3
        assert {r["id"] for r in state["pending"]} == set(ingest_report["bronze_row_ids"])
        for row in state["pending"]:
            assert row["failure_reason"], "failure must carry a reason"
            assert "row count" in row["failure_reason"]
        assert state["promoted"] == []
        assert state["canonical"] == [], "failed gate must not pollute canonical"

        assert report["candidate_row_ids"] == ingest_report["bronze_row_ids"]
        assert report["success_row_ids"] == []
        assert report["failure_row_ids"] == ingest_report["bronze_row_ids"]
        assert report["bronze_pending_count_after"] == 3
        assert report["canonical_count_after"] == 0
        assert report["failure_summary"][0]["issues"]

        assert (batch["success_count"] + batch["failure_count"]) == batch["candidate_count"]
        _assert_no_duplicate_lifecycle(conn)
    finally:
        conn.close()


def test_write_failure_rolls_back_and_reports_honestly(fresh_db, monkeypatch):
    run = _ingest(1)
    ingest_report = run["report"]

    original_upsert = canon_repo.upsert_item

    def explode(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(canon_repo, "upsert_item", explode)
    batch = promotion_service.promote(run_id=run["id"])
    monkeypatch.setattr(canon_repo, "upsert_item", original_upsert)

    conn = connect()
    try:
        state = _zone_state(conn)
        assert batch["status"] == "failed"
        assert batch["candidate_count"] == 3
        assert batch["success_count"] == 0
        assert batch["failure_count"] == 3
        assert state["canonical"] == []
        assert {r["id"] for r in state["pending"]} == set(ingest_report["bronze_row_ids"])
        for row in state["pending"]:
            assert "promotion write failed" in (row["failure_reason"] or "")
        # The rolled-back transaction's batch row must not survive, and the
        # report that does survive must be a real failure.
        surviving = [b for b in promotions_repo.list_batches(conn) if b["id"] == batch["id"]]
        assert surviving and surviving[0]["status"] == "failed"
    finally:
        conn.close()


def test_retry_after_success_is_a_clean_noop(fresh_db):
    run = _ingest(1)
    first = promotion_service.promote(run_id=run["id"])
    second = promotion_service.promote(run_id=run["id"])

    conn = connect()
    try:
        assert first["status"] == "passed"
        assert second["status"] == "passed"
        assert second["candidate_count"] == 0
        assert second["success_count"] == 0
        assert second["failure_count"] == 0
        assert second["report"]["already_promoted_count"] == 3
        assert canon_repo.count_items(conn) == 3
        assert bronze_repo.count(conn, status="pending") == 0
    finally:
        conn.close()


def test_pending_scope_promotes_all_sources_without_overlap(fresh_db):
    alpha = _ingest(1)
    beta = _ingest(2)
    gamma = _ingest(3)

    conn = connect()
    try:
        # Nothing in the official zone before promotion.
        assert canon_repo.count_items(conn) == 0
        pending = bronze_repo.list_pending(conn)
        expected_ids = (
            alpha["report"]["bronze_row_ids"]
            + beta["report"]["bronze_row_ids"]
            + gamma["report"]["bronze_row_ids"]
        )
        assert [r["id"] for r in pending] == expected_ids
        expected_keys = (
            alpha["report"]["bronze_business_keys"]
            + beta["report"]["bronze_business_keys"]
            + gamma["report"]["bronze_business_keys"]
        )
    finally:
        conn.close()

    batch = promotion_service.promote()

    conn = connect()
    try:
        state = _zone_state(conn)
        report = batch["report"]
        assert batch["status"] == "passed"
        assert batch["candidate_count"] == len(expected_ids)
        assert batch["success_count"] == len(expected_ids)
        assert batch["failure_count"] == 0
        assert report["candidate_row_ids"] == expected_ids
        assert report["success_row_ids"] == expected_ids
        assert report["failure_row_ids"] == []
        assert set(report["candidate_keys"]) == set(expected_keys)
        # Business keys are unique in canonical even when ingested with dups.
        assert state["canonical_keys"] == set(expected_keys)
        assert len(state["canonical"]) == len(set(expected_keys))
        assert state["pending"] == []
        assert report["bronze_pending_count_after"] == 0
        assert report["canonical_count_after"] == len(state["canonical"])
        _assert_no_duplicate_lifecycle(conn)
    finally:
        conn.close()


def test_one_bad_run_aborts_whole_batch_without_losing_good_rows(fresh_db):
    good = _ingest(1)
    bad = _ingest(3)

    conn = connect()
    try:
        tampered = dict(bad["report"])
        tampered["bronze_candidate_count"] = 99
        runs_repo.finish_run(
            conn,
            bad["id"],
            status="ok",
            parsed=tampered["parsed_count"],
            upserted=tampered["upserted_count"],
            conflicts=tampered["conflict_count"],
            skipped=tampered["skipped_count"],
            report=tampered,
        )
        conn.commit()
    finally:
        conn.close()

    batch = promotion_service.promote()

    conn = connect()
    try:
        state = _zone_state(conn)
        assert batch["status"] == "failed"
        assert batch["success_count"] == 0
        assert batch["failure_count"] == batch["candidate_count"]
        assert state["canonical"] == []
        assert len(state["pending"]) == batch["candidate_count"]
        reasons = {r["failure_reason"] for r in state["pending"]}
        assert any("row count" in (x or "") for x in reasons)
        assert any("another run failed" in (x or "") for x in reasons)
        assert report_ids_cover_all(batch["report"], state["pending"])
        assert batch["report"]["candidate_count"] == (
            batch["report"]["success_count"] + batch["report"]["failure_count"]
        )
    finally:
        conn.close()


def report_ids_cover_all(report, pending_rows):
    return set(report["failure_row_ids"]) == {r["id"] for r in pending_rows}


def test_stale_pending_rows_never_regress_canonical(fresh_db):
    """Promotion is monotonic even when a stale row is promoted after a newer one.

    A pending candidate older than the canonical state for its key is resolved
    (marked promoted) but must not overwrite the newer official row.
    """
    item = {
        "business_key": "SKU-1001",
        "title": "Oak Desk Lamp",
        "price": 129.0,
        "category": "lighting",
        "event_ts": "2026-11-01T10:00:00Z",
        "source_revision": 2,
        "payload": {"business_key": "SKU-1001"},
    }
    newer = {**item, "source_revision": 4, "event_ts": "2026-11-01T12:00:00Z"}

    conn = connect()
    try:
        # Canonical already holds the newer revision from an earlier promotion.
        canon_repo.upsert_item(conn, newer, source_id=1)
        run_id = runs_repo.create_run(conn, source_id=1, mode="incremental")
        row_id = bronze_repo.insert_row(
            conn, run_id=run_id, source_id=1, item=item, disposition="insert"
        )
        runs_repo.finish_run(
            conn,
            run_id,
            status="ok",
            parsed=1,
            upserted=1,
            conflicts=0,
            skipped=0,
            report={
                "bronze_candidate_count": 1,
                "bronze_business_keys": ["SKU-1001"],
                "bronze_row_ids": [row_id],
            },
        )
        conn.commit()
    finally:
        conn.close()

    batch = promotion_service.promote(run_id=run_id)
    assert batch["status"] == "passed"
    assert batch["candidate_count"] == 1
    assert batch["success_count"] == 1

    conn = connect()
    try:
        canon = canon_repo.get_by_key(conn, "SKU-1001")
        assert canon["source_revision"] == 4, "canonical regressed to a stale rev"
        assert "SKU-1001" in batch["report"]["zone_check"]["superseded_keys"]
        assert batch["report"]["zone_check"]["superseded_row_ids"] == [row_id]
        # The stale row is still resolved: not left pending.
        assert bronze_repo.list_pending(conn, run_id=run_id) == []
        assert bronze_repo.count(conn, status="promoted") == 1
    finally:
        conn.close()


def _rows_for_run(run_id):
    conn = connect()
    try:
        return bronze_repo.list_by_run(conn, run_id)
    finally:
        conn.close()


def _rows_for_run(run_id):
    conn = connect()
    try:
        return bronze_repo.list_by_run(conn, run_id)
    finally:
        conn.close()
