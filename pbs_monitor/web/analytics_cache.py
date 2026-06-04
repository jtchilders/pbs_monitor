"""
SQLite-backed analytics cache for PBS Monitor web server.

Keys are SHA256 hashes of query parameters (freq, bin range, filters, group_by).
Only complete bins are cached — past bins are immutable so no TTL is needed.
Entries that include the current incomplete bin must never be stored here.
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

_LOGGER = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS analytics_cache (
    key        TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""


class AnalyticsCache:
    """Thread-safe SQLite cache for analytics query results."""

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
                con.execute(_CREATE_TABLE)
                con.commit()
            finally:
                con.close()

    @staticmethod
    def make_key(params: dict[str, Any]) -> str:
        """Return SHA256 hex digest of canonical JSON of params."""
        canonical = json.dumps(params, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()

    def get(self, key: str) -> dict | None:
        """Return cached dict or None on miss."""
        con = self._connect()
        try:
            row = con.execute(
                "SELECT data FROM analytics_cache WHERE key = ?", (key,)
            ).fetchone()
            if row:
                return json.loads(row[0])
            return None
        except Exception as e:
            _LOGGER.warning("Cache get error: %s", e)
            return None
        finally:
            con.close()

    def set(self, key: str, data: dict) -> None:
        """Store data under key. Silently ignores errors."""
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


def make_cache(main_db_url: str) -> AnalyticsCache:
    """
    Build an AnalyticsCache whose DB sits alongside the main SQLite DB,
    or in the user's home directory when using PostgreSQL.
    """
    if main_db_url.startswith("sqlite:///"):
        main_path = main_db_url[len("sqlite:///"):]
        cache_path = str(Path(main_path).parent / "analytics_cache.db")
    else:
        # PostgreSQL or other non-file backend — use a fixed cache location
        cache_path = str(Path.home() / ".pbs_monitor_analytics_cache.db")
    return AnalyticsCache(cache_path)
