"""
Analytics cache for PBS Monitor web server.

Keys are SHA256 hashes of query parameters (freq, bin range, filters, group_by).
Only complete bins are cached — past bins are immutable so no TTL is needed.
Entries that include the current incomplete bin must never be stored here.

Backend selection:
  - PostgreSQL: uses the main DB engine (shared across all web-server workers/users)
  - SQLite: a separate analytics_cache.db file alongside the main DB file
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Column, DateTime, String, Text, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, sessionmaker

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQLite backend (local dev / legacy)
# ---------------------------------------------------------------------------

_SQLITE_CREATE = """
CREATE TABLE IF NOT EXISTS analytics_cache (
    key        TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class _SQLiteCache:
    """Thread-safe SQLite analytics cache (used when main DB is SQLite)."""

    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self._path, check_same_thread=False, timeout=10)

    def _init_db(self) -> None:
        with self._lock:
            con = self._connect()
            try:
                con.execute(_SQLITE_CREATE)
                con.commit()
            finally:
                con.close()

    def get(self, key: str) -> dict | None:
        con = self._connect()
        try:
            row = con.execute(
                "SELECT data FROM analytics_cache WHERE key = ?", (key,)
            ).fetchone()
            return json.loads(row[0]) if row else None
        except Exception as e:
            _LOGGER.warning("Cache get error: %s", e)
            return None
        finally:
            con.close()

    def set(self, key: str, data: dict) -> None:
        payload = json.dumps(data, default=str)
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            con = self._connect()
            try:
                con.execute(
                    "INSERT OR REPLACE INTO analytics_cache (key, data, created_at) VALUES (?, ?, ?)",
                    (key, payload, now),
                )
                con.commit()
            except Exception as e:
                _LOGGER.warning("Cache set error: %s", e)
            finally:
                con.close()


# ---------------------------------------------------------------------------
# PostgreSQL backend (multi-user shared cache)
# ---------------------------------------------------------------------------

_PgBase = declarative_base()


class _AnalyticsCacheRow(_PgBase):
    __tablename__ = "analytics_cache"

    key = Column(String(64), primary_key=True)   # SHA256 hex = 64 chars
    data = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


class _PostgresCache:
    """Shared Postgres analytics cache (used when main DB is PostgreSQL).

    Stores results in an ``analytics_cache`` table within the same schema
    as the rest of the PBS Monitor data, so all web-server workers and users
    share the same cached computations.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
        self._init_db()

    def _init_db(self) -> None:
        # Create the table if it doesn't exist (search_path already set on engine)
        _PgBase.metadata.create_all(self._engine, tables=[_AnalyticsCacheRow.__table__])

    def get(self, key: str) -> dict | None:
        try:
            with self._Session() as session:
                row = session.get(_AnalyticsCacheRow, key)
                return json.loads(row.data) if row else None
        except Exception as e:
            _LOGGER.warning("Cache get error: %s", e)
            return None

    def set(self, key: str, data: dict) -> None:
        payload = json.dumps(data, default=str)
        now = datetime.now(timezone.utc)
        try:
            with self._Session() as session:
                stmt = (
                    pg_insert(_AnalyticsCacheRow)
                    .values(key=key, data=payload, created_at=now)
                    .on_conflict_do_update(
                        index_elements=["key"],
                        set_={"data": payload, "created_at": now},
                    )
                )
                session.execute(stmt)
                session.commit()
        except Exception as e:
            _LOGGER.warning("Cache set error: %s", e)


# ---------------------------------------------------------------------------
# Public API — AnalyticsCache is whichever backend is appropriate
# ---------------------------------------------------------------------------

class AnalyticsCache:
    """Unified analytics cache.  Delegates to SQLite or Postgres backend."""

    def __init__(self, backend: _SQLiteCache | _PostgresCache) -> None:
        self._backend = backend

    @staticmethod
    def make_key(params: dict[str, Any]) -> str:
        """Return SHA256 hex digest of canonical JSON of params."""
        canonical = json.dumps(params, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def get(self, key: str) -> dict | None:
        return self._backend.get(key)

    def set(self, key: str, data: dict) -> None:
        self._backend.set(key, data)


def make_cache(main_db_url: str, engine: Engine | None = None) -> AnalyticsCache:
    """
    Build the appropriate AnalyticsCache for the configured DB backend.

    - PostgreSQL: uses the supplied *engine* (same connection pool + search_path
      as the rest of the app) so the cache is shared across all workers/users.
    - SQLite: a separate ``analytics_cache.db`` file alongside the main DB.
    """
    if main_db_url.startswith("postgresql") and engine is not None:
        return AnalyticsCache(_PostgresCache(engine))

    # SQLite fallback
    if main_db_url.startswith("sqlite:///"):
        main_path = main_db_url[len("sqlite:///"):]
        cache_path = str(Path(main_path).parent / "analytics_cache.db")
    else:
        cache_path = str(Path.home() / ".pbs_monitor_analytics_cache.db")
    return AnalyticsCache(_SQLiteCache(cache_path))
