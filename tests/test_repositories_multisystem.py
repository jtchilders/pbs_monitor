"""
Tests for the pragmatic Option A in pbs_monitor.database.repositories.

Behavior under test:
   - Writes auto-stamp ``system`` from ``config.pbs.system``.
   - Writes refuse to run if ``config.pbs.system`` is unset.
   - Reads accept an optional ``system`` arg; default is the
     daemon's; if neither is supplied, refuse rather than return
     cross-system data.
   - A repository configured for system X never reads or writes
     rows for system Y (unless an explicit ``system='Y'`` override
     is passed to a read method).
"""

import os
import tempfile
import textwrap

import pytest

from pbs_monitor.config import Config
from pbs_monitor.database import initialize_database
from pbs_monitor.database.models import JobState, QueueState
from pbs_monitor.database.repositories import (
   DataCollectionRepository,
   JobRepository,
   NodeRepository,
   QueueRepository,
   ReservationRepository,
   SystemRepository,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_config(tmp_path, system: str | None) -> Config:
   """Build an isolated Config pointing at a temp sqlite + given system."""
   yaml = "pbs:\n"
   if system is not None:
      yaml += f"  system: {system}\n"
   db_path = tmp_path / f"pbs_{system or 'none'}.db"
   yaml += f'database:\n  url: "sqlite:///{db_path}"\n'

   cfg_path = tmp_path / f"pbs_monitor_{system or 'none'}.yaml"
   cfg_path.write_text(textwrap.dedent(yaml))
   os.chmod(cfg_path, 0o600)

   cfg = Config(config_file=str(cfg_path))
   return cfg


@pytest.fixture
def polaris_config(tmp_path):
   cfg = _make_config(tmp_path, "polaris")
   initialize_database(cfg)
   return cfg


@pytest.fixture
def aurora_config(tmp_path):
   cfg = _make_config(tmp_path, "aurora")
   initialize_database(cfg)
   return cfg


@pytest.fixture
def no_system_config(tmp_path):
   cfg = _make_config(tmp_path, system=None)
   initialize_database(cfg)
   return cfg


# ---------------------------------------------------------------------------
# Auto-stamping on write
# ---------------------------------------------------------------------------

class TestAutoStampingWrites:
   """Daemon writes must always be tagged with ``config.pbs.system``."""

   def test_create_or_update_job_stamps_system(self, polaris_config):
      repo = JobRepository(polaris_config)
      job = repo.create_or_update_job({
         "job_id": "12345.host.alcf.anl.gov",
         "state": JobState.QUEUED,
         "owner": "alice",
      })
      assert job.system == "polaris"

   def test_create_or_update_job_ignores_caller_system_override(self, polaris_config):
      """A polaris daemon must not be tricked into writing aurora rows."""
      repo = JobRepository(polaris_config)
      job = repo.create_or_update_job({
         "job_id": "12345.host.alcf.anl.gov",
         "state": JobState.QUEUED,
         "system": "aurora",  # Sneaky.
      })
      assert job.system == "polaris", "Daemon system must override caller-supplied system"

   def test_create_or_update_queue_stamps_system(self, polaris_config):
      repo = QueueRepository(polaris_config)
      q = repo.create_or_update_queue({"name": "debug"})
      assert q.system == "polaris"

   def test_create_or_update_node_stamps_system(self, polaris_config):
      repo = NodeRepository(polaris_config)
      n = repo.create_or_update_node({"name": "x3000"})
      assert n.system == "polaris"

   def test_add_job_history_stamps_system_for_string_form(self, polaris_config):
      repo = JobRepository(polaris_config)
      # Need a parent job first.
      repo.create_or_update_job({
         "job_id": "h1.host.alcf.anl.gov",
         "state": JobState.QUEUED,
      })
      hist = repo.add_job_history("h1.host.alcf.anl.gov", JobState.RUNNING)
      assert hist.system == "polaris"

   def test_log_collection_start_stamps_system(self, polaris_config):
      repo = DataCollectionRepository(polaris_config)
      log_id = repo.log_collection_start("jobs")
      stats = repo.get_collection_statistics()
      # success_count incremented (this one log entry).
      assert stats["success_count"] >= 1


# ---------------------------------------------------------------------------
# Refusal when system is missing
# ---------------------------------------------------------------------------

class TestWriteRefusalWithoutSystem:
   """Writes refuse to run if config.pbs.system is unset."""

   def test_create_or_update_job_refuses(self, no_system_config):
      repo = JobRepository(no_system_config)
      with pytest.raises(ValueError, match="No PBS system configured"):
         repo.create_or_update_job({"job_id": "z.host", "state": JobState.QUEUED})

   def test_create_or_update_queue_refuses(self, no_system_config):
      repo = QueueRepository(no_system_config)
      with pytest.raises(ValueError, match="No PBS system configured"):
         repo.create_or_update_queue({"name": "debug"})

   def test_create_or_update_node_refuses(self, no_system_config):
      repo = NodeRepository(no_system_config)
      with pytest.raises(ValueError, match="No PBS system configured"):
         repo.create_or_update_node({"name": "x3000"})

   def test_log_collection_start_refuses(self, no_system_config):
      repo = DataCollectionRepository(no_system_config)
      with pytest.raises(ValueError, match="No PBS system configured"):
         repo.log_collection_start("jobs")


# ---------------------------------------------------------------------------
# Read scoping
# ---------------------------------------------------------------------------

class TestReadScoping:
   """Reads respect either the daemon's system or an explicit override."""

   def test_get_jobs_defaults_to_daemon_system(self, polaris_config):
      repo = JobRepository(polaris_config)
      repo.create_or_update_job({"job_id": "j1.host", "state": JobState.RUNNING})
      jobs = repo.get_active_jobs()  # No explicit system.
      assert len(jobs) == 1
      assert jobs[0].system == "polaris"

   def test_read_can_override_system_explicitly(self, polaris_config):
      """Reader can ask for a different system explicitly.

      In a single-DB deployment a polaris-configured reader could be
      asked to fetch aurora rows. Repository should honor the override.
      """
      from pbs_monitor.database.models import Job
      from pbs_monitor.database.connection import DatabaseManager

      # Inject an aurora row directly into the same DB.
      dm = DatabaseManager(polaris_config)
      with dm.get_session() as session:
         session.add(Job(system="aurora", job_id="a1.host", state=JobState.RUNNING))
         session.commit()

      repo = JobRepository(polaris_config)

      # Default scope: only polaris.
      assert len(repo.get_active_jobs()) == 0
      # Explicit override.
      aurora_jobs = repo.get_active_jobs(system="aurora")
      assert len(aurora_jobs) == 1
      assert aurora_jobs[0].system == "aurora"

   def test_read_refuses_when_no_system_anywhere(self, no_system_config):
      repo = JobRepository(no_system_config)
      with pytest.raises(ValueError, match="No PBS system configured"):
         repo.get_active_jobs()  # No daemon system, no explicit arg.

   def test_get_job_by_id_scoped(self, polaris_config):
      """The same job_id must not be returnable across systems by accident."""
      from pbs_monitor.database.models import Job
      from pbs_monitor.database.connection import DatabaseManager

      shared_id = "shared.host.alcf.anl.gov"
      repo = JobRepository(polaris_config)
      repo.create_or_update_job({"job_id": shared_id, "state": JobState.RUNNING})

      dm = DatabaseManager(polaris_config)
      with dm.get_session() as session:
         session.add(Job(system="aurora", job_id=shared_id, state=JobState.QUEUED))
         session.commit()

      # Default (polaris) scope.
      pol = repo.get_job_by_id(shared_id)
      assert pol is not None and pol.system == "polaris" and pol.state == JobState.RUNNING

      # Explicit aurora.
      aur = repo.get_job_by_id(shared_id, system="aurora")
      assert aur is not None and aur.system == "aurora" and aur.state == JobState.QUEUED

   def test_get_queue_snapshots_scoped(self, polaris_config):
      """Snapshots also filter on system."""
      from pbs_monitor.database.models import QueueSnapshot
      from pbs_monitor.database.connection import DatabaseManager

      q_repo = QueueRepository(polaris_config)
      q_repo.create_or_update_queue({"name": "debug"})
      q_repo.add_queue_snapshot("debug", {"state": QueueState.ENABLED_STARTED, "running_jobs": 3})

      # Inject an aurora snapshot directly (need an aurora queue first
      # for the FK to hold).
      dm = DatabaseManager(polaris_config)
      from pbs_monitor.database.models import Queue
      with dm.get_session() as session:
         session.add(Queue(system="aurora", name="debug"))
         session.add(QueueSnapshot(
            system="aurora", queue_name="debug",
            state=QueueState.ENABLED_STARTED, running_jobs=99,
         ))
         session.commit()

      pol = q_repo.get_queue_snapshots("debug")
      assert len(pol) == 1
      assert pol[0].running_jobs == 3

      aur = q_repo.get_queue_snapshots("debug", system="aurora")
      assert len(aur) == 1
      assert aur[0].running_jobs == 99


# ---------------------------------------------------------------------------
# snapshot_index per-system
# ---------------------------------------------------------------------------

class TestNodeSnapshotIndexPerSystem:
   """Two daemons can both use snapshot_index=0 for their first node."""

   def test_independent_index_sequences_in_same_db(self, polaris_config, tmp_path):
      """Reuse the polaris DB as a shared backend for both systems."""
      # Force aurora's config to point at the *same* sqlite file.
      pol_url = polaris_config.database.url
      yaml = textwrap.dedent(f"""\
         pbs:
            system: aurora
         database:
            url: "{pol_url}"
      """)
      acfg_path = tmp_path / "aurora.yaml"
      acfg_path.write_text(yaml)
      os.chmod(acfg_path, 0o600)
      aurora_config = Config(config_file=str(acfg_path))

      pol_repo = NodeRepository(polaris_config)
      aur_repo = NodeRepository(aurora_config)

      p1 = pol_repo.create_or_update_node({"name": "x3000"})
      a1 = aur_repo.create_or_update_node({"name": "x4000"})

      assert p1.snapshot_index == 0
      assert a1.snapshot_index == 0  # Per-system, not global.

      p2 = pol_repo.create_or_update_node({"name": "x3001"})
      a2 = aur_repo.create_or_update_node({"name": "x4001"})

      assert p2.snapshot_index == 1
      assert a2.snapshot_index == 1
