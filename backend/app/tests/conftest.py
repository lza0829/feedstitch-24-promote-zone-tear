import json

import pytest

from app import db, seed


@pytest.fixture()
def fresh_db(tmp_path, monkeypatch):
    """Isolated SQLite DB per test, seeded with the standard sources."""
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    seed.init_db()
    yield
    # Connections are short-lived; nothing to tear down.
