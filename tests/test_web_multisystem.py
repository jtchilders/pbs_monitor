"""
Tests for multi-system scoping in pbs_monitor.web.server.

Verifies that the FastAPI endpoints only surface rows that belong to
the system named in ``config.pbs.system``.  Two systems ('polaris'
and 'aurora') are seeded into the same SQLite database; the app is
created with ``config.pbs.system = 'polaris'`` and every endpoint
must return only polaris data.

These are white-box integration tests: they exercise the full
request-response path through FastAPI / SQLAlchemy without spawning
a real uvicorn process.
"""

import os
import textwrap
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from pbs_monitor.config import Config
from pbs_monitor.database import initialize_database
from pbs_monitor.database.connection import DatabaseManager
from pbs_monitor.database.models import (
    DataCollectionLog, DataCollectionStatus,
    Job, JobHistory, JobState,
    Node, NodeSnapshot,
    Queue, QueueSnapshot, QueueState,
    Reservation, ReservationState,
    SystemSnapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(tmp_path, system: str | None, db_url: str | None = None) -> Config:
    """Build an isolated Config."""
    yaml_lines = ["pbs:\n"]
    if system:
        yaml_lines.append(f"  system: {system}\n")
    if db_url:
        yaml_lines.append(f'database:\n  url: "{db_url}"\n')
    else:
        db_path = tmp_path / f"pbs_{system or 'none'}.db"
        yaml_lines.append(f'database:\n  url: "sqlite:///{db_path}"\n')

    cfg_path = tmp_path / f"cfg_{system or 'none'}.yaml"
    cfg_path.write_text("".join(yaml_lines))
    os.chmod(cfg_path, 0o600)
    return Config(config_file=str(cfg_path))


@pytest.fixture
def shared_db(tmp_path) -> tuple[Config, Config, str]:
    """Create a single SQLite DB with rows for both 'polaris' and 'aurora'.

    Returns (polaris_config, aurora_config, db_url).
    """
    db_path = tmp_path / "shared.db"
    db_url = f"sqlite:///{db_path}"

    # Config used only to initialise the schema.
    pol_cfg = _make_config(tmp_path, "polaris", db_url)
    initialize_database(pol_cfg)

    now = datetime.now(timezone.utc)

    dm = DatabaseManager(pol_cfg)
    with dm.get_session() as session:
        # ── Jobs ──
        session.add(Job(
            system="polaris", job_id="111.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
            state=JobState.RUNNING, owner="alice", queue="prod",
            nodes=2, walltime="01:00:00",
            start_time=now - timedelta(minutes=30),
            submit_time=now - timedelta(hours=1),
        ))
        session.add(Job(
            system="polaris", job_id="222.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov",
            state=JobState.QUEUED, owner="bob", queue="debug",
            nodes=1, walltime="00:30:00",
            submit_time=now - timedelta(minutes=10),
        ))
        session.add(Job(
            system="aurora", job_id="999.aurora-pbs-01.hsn.cm.aurora.alcf.anl.gov",
            state=JobState.RUNNING, owner="carol", queue="prod",
            nodes=8, walltime="02:00:00",
            start_time=now - timedelta(hours=1),
            submit_time=now - timedelta(hours=2),
        ))

        # ── Nodes ──
        session.add(Node(system="polaris", name="x3000c0s1b0n0", snapshot_index=0))
        session.add(Node(system="aurora",  name="x4000c0s1b0n0", snapshot_index=0))

        # ── NodeSnapshot ──
        session.add(NodeSnapshot(
            system="polaris", timestamp=now, snapshot_data="A" * 10,
        ))
        session.add(NodeSnapshot(
            system="aurora",  timestamp=now, snapshot_data="B" * 20,
        ))

        # ── SystemSnapshot ──
        session.add(SystemSnapshot(
            system="polaris", timestamp=now,
            running_jobs=1, queued_jobs=1, held_jobs=0,
            total_nodes=100, available_nodes=50,
            system_utilization_percent=50.0,
        ))
        session.add(SystemSnapshot(
            system="aurora",  timestamp=now,
            running_jobs=5, queued_jobs=3, held_jobs=0,
            total_nodes=500, available_nodes=200,
            system_utilization_percent=60.0,
        ))

        # ── DataCollectionLog ──
        session.add(DataCollectionLog(
            system="polaris", collection_type="jobs",
            status=DataCollectionStatus.SUCCESS, timestamp=now,
        ))
        session.add(DataCollectionLog(
            system="aurora", collection_type="jobs",
            status=DataCollectionStatus.SUCCESS,
            timestamp=now - timedelta(seconds=5),
        ))

        # ── Queue / QueueSnapshot ──
        session.add(Queue(system="polaris", name="prod"))
        session.add(Queue(system="aurora",  name="prod"))
        session.add(QueueSnapshot(
            system="polaris", queue_name="prod",
            state=QueueState.ENABLED_STARTED,
            running_jobs=1, queued_jobs=0, timestamp=now,
        ))
        session.add(QueueSnapshot(
            system="aurora",  queue_name="prod",
            state=QueueState.ENABLED_STARTED,
            running_jobs=5, queued_jobs=3, timestamp=now,
        ))

        session.commit()

    aur_cfg = _make_config(tmp_path, "aurora", db_url)
    return pol_cfg, aur_cfg, db_url


@pytest.fixture
def polaris_client(shared_db) -> Generator[TestClient, None, None]:
    """TestClient for an app scoped to polaris."""
    pol_cfg, _, _ = shared_db
    from pbs_monitor.web.server import create_app
    app = create_app(config=pol_cfg)
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


@pytest.fixture
def aurora_client(shared_db) -> Generator[TestClient, None, None]:
    """TestClient for an app scoped to aurora."""
    _, aur_cfg, _ = shared_db
    from pbs_monitor.web.server import create_app
    app = create_app(config=aur_cfg)
    with TestClient(app, raise_server_exceptions=True) as client:
        yield client


# ---------------------------------------------------------------------------
# /api/system — basic smoke-test that the app starts and returns system name
# ---------------------------------------------------------------------------

class TestApiSystem:
    def test_polaris_system_name(self, polaris_client):
        r = polaris_client.get("/api/system")
        assert r.status_code == 200
        data = r.json()
        # system_name must be polaris (from config or heuristic)
        assert "polaris" in data["system_name"].lower()

    def test_aurora_system_name(self, aurora_client):
        r = aurora_client.get("/api/system")
        assert r.status_code == 200
        data = r.json()
        assert "aurora" in data["system_name"].lower()


# ---------------------------------------------------------------------------
# /api/snapshot — system-scoped job lists and aggregates
# ---------------------------------------------------------------------------

class TestApiSnapshot:
    def test_polaris_running_jobs_only(self, polaris_client):
        r = polaris_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        running = data["jobs"]["running"]
        job_ids = [j["full_job_id"] for j in running]
        # The polaris running job must appear.
        assert any("polaris" in jid for jid in job_ids), f"No polaris job in {job_ids}"
        # The aurora running job must NOT appear.
        assert not any("aurora" in jid for jid in job_ids), f"Aurora job leaked: {job_ids}"

    def test_polaris_queued_jobs_only(self, polaris_client):
        r = polaris_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        queued = data["jobs"]["queued"]
        assert len(queued) == 1
        assert "polaris" in queued[0]["full_job_id"]

    def test_aurora_running_jobs_only(self, aurora_client):
        r = aurora_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        running = data["jobs"]["running"]
        job_ids = [j["full_job_id"] for j in running]
        assert any("aurora" in jid for jid in job_ids)
        assert not any("polaris" in jid for jid in job_ids)

    def test_polaris_system_snapshot_values(self, polaris_client):
        """The aggregate numbers should come from the polaris SystemSnapshot."""
        r = polaris_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        sys = data["system"]
        # Polaris snapshot has running_jobs=1, queued_jobs=1
        assert sys["running_jobs"] in (1, len(data["jobs"]["running"]))

    def test_polaris_node_snapshot_length(self, polaris_client):
        """state_string should be the polaris NodeSnapshot (length 10), not aurora's (20)."""
        r = polaris_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        state_string = data.get("state_string", "")
        # Polaris snapshot_data is 10 'A' chars; aurora is 20 'B' chars.
        assert len(state_string) == 10, (
            f"Expected polaris state_string len=10, got {len(state_string)!r}"
        )

    def test_aurora_node_snapshot_length(self, aurora_client):
        r = aurora_client.get("/api/snapshot")
        assert r.status_code == 200
        data = r.json()
        state_string = data.get("state_string", "")
        assert len(state_string) == 20, (
            f"Expected aurora state_string len=20, got {len(state_string)!r}"
        )


# ---------------------------------------------------------------------------
# /api/jobs/{job_id} — cross-system isolation
# ---------------------------------------------------------------------------

class TestApiJobDetail:
    def test_polaris_job_found(self, polaris_client):
        r = polaris_client.get("/api/jobs/111")
        assert r.status_code == 200
        data = r.json()
        assert "polaris" in data["full_job_id"]

    def test_aurora_job_not_found_from_polaris(self, polaris_client):
        """A polaris-scoped app must not find an aurora job."""
        r = polaris_client.get("/api/jobs/999")
        assert r.status_code == 404

    def test_aurora_job_found_from_aurora(self, aurora_client):
        r = aurora_client.get("/api/jobs/999")
        assert r.status_code == 200
        data = r.json()
        assert "aurora" in data["full_job_id"]


# ---------------------------------------------------------------------------
# /api/user/{username}/summary — system scoping
# ---------------------------------------------------------------------------

class TestApiUserSummary:
    def test_polaris_alice_sees_her_job(self, polaris_client):
        r = polaris_client.get("/api/user/alice/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["total_jobs"] >= 1

    def test_polaris_carol_has_no_jobs(self, polaris_client):
        """carol's job is on aurora; polaris-scoped app must not see it."""
        r = polaris_client.get("/api/user/carol/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["total_jobs"] == 0

    def test_aurora_carol_sees_her_job(self, aurora_client):
        r = aurora_client.get("/api/user/carol/summary")
        assert r.status_code == 200
        data = r.json()
        assert data["total_jobs"] >= 1
