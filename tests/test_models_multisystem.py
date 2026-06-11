"""
Tests for the multi-system schema changes in pbs_monitor.database.models.

These tests exercise the SQLAlchemy models directly against an
in-memory SQLite database, bypassing the repository layer. The repo
layer's own ``system``-awareness is tested elsewhere once it lands
(see test_database.py module-level skip).

Invariants under test:
   1. Every modeled table has a ``system`` column.
   2. Parent tables (jobs/queues/nodes/reservations) have composite
      primary keys ``(system, <natural_id>)``.
   3. Child tables (job_history etc.) carry composite foreign keys
      back to their parents.
   4. The same natural ID can coexist across different ``system``
      values without PK collision.
   5. Duplicate ``(system, natural_id)`` is rejected.
   6. Orphan child rows (system/parent_id pointing nowhere) are
      rejected by the composite FK.
   7. Missing ``system`` is rejected by NOT NULL.
   8. ``nodes.snapshot_index`` is unique per-system, not globally.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, event, inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from pbs_monitor.database.models import (
   Base,
   DataCollectionLog,
   DataCollectionStatus,
   Job,
   JobHistory,
   JobState,
   Node,
   NodeSnapshot,
   Queue,
   QueueSnapshot,
   QueueState,
   Reservation,
   ReservationHistory,
   ReservationState,
   ReservationUtilization,
   SystemSnapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def engine():
   """In-memory SQLite with FK enforcement enabled."""
   eng = create_engine("sqlite:///:memory:")

   @event.listens_for(eng, "connect")
   def _fk_on(dbapi_conn, _):  # noqa: D401 - SA event signature
      dbapi_conn.execute("PRAGMA foreign_keys=ON")

   Base.metadata.create_all(eng)
   return eng


@pytest.fixture
def session(engine):
   Session = sessionmaker(bind=engine)
   s = Session()
   yield s
   s.close()


# Constant FQDN-style job IDs used across tests; matches Polaris/Aurora
# real-world format.
POLARIS_JOB = "7164835.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov"
AURORA_JOB = "7164835.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov"


# ---------------------------------------------------------------------------
# Schema-shape checks (cheap, fast, broad)
# ---------------------------------------------------------------------------

class TestSchemaShape:
   """Inspect the generated DDL to confirm columns/PKs/FKs are right."""

   # Every modeled table should have a system column.
   ALL_TABLES = [
      "jobs", "job_history",
      "queues", "queue_snapshots",
      "nodes", "node_snapshots",
      "system_snapshots",
      "reservations", "reservation_history", "reservation_utilization",
      "data_collection_log",
   ]

   def test_every_table_has_system_column(self, engine):
      insp = inspect(engine)
      missing = []
      for t in self.ALL_TABLES:
         cols = {c["name"] for c in insp.get_columns(t)}
         if "system" not in cols:
            missing.append(t)
      assert not missing, f"Tables missing 'system' column: {missing}"

   @pytest.mark.parametrize("table,expected_pk", [
      ("jobs", ["system", "job_id"]),
      ("queues", ["system", "name"]),
      ("nodes", ["system", "name"]),
      ("reservations", ["system", "reservation_id"]),
   ])
   def test_parent_tables_have_composite_pk(self, engine, table, expected_pk):
      insp = inspect(engine)
      pk = insp.get_pk_constraint(table)
      assert pk["constrained_columns"] == expected_pk, (
         f"{table} PK is {pk['constrained_columns']!r}, expected {expected_pk!r}"
      )

   @pytest.mark.parametrize("table,parent,fk_cols,parent_cols", [
      ("job_history",            "jobs",         ["system", "job_id"],         ["system", "job_id"]),
      ("queue_snapshots",        "queues",       ["system", "queue_name"],     ["system", "name"]),
      ("reservation_history",    "reservations", ["system", "reservation_id"], ["system", "reservation_id"]),
      ("reservation_utilization","reservations", ["system", "reservation_id"], ["system", "reservation_id"]),
   ])
   def test_child_tables_have_composite_fk(
      self, engine, table, parent, fk_cols, parent_cols
   ):
      insp = inspect(engine)
      matching = [
         fk for fk in insp.get_foreign_keys(table)
         if fk["referred_table"] == parent
            and fk["constrained_columns"] == fk_cols
      ]
      assert matching, (
         f"{table} missing composite FK {fk_cols} -> {parent}.{parent_cols}; "
         f"found FKs: {insp.get_foreign_keys(table)}"
      )
      assert matching[0]["referred_columns"] == parent_cols


# ---------------------------------------------------------------------------
# Behavioral invariants (insert / query / constraint enforcement)
# ---------------------------------------------------------------------------

class TestCompositePkBehavior:
   """The PK rules enforce what we want and allow what we want."""

   def test_same_job_id_coexists_across_systems(self, session):
      session.add_all([
         Job(system="polaris", job_id=POLARIS_JOB, state=JobState.QUEUED),
         Job(system="aurora",  job_id=AURORA_JOB,  state=JobState.QUEUED),
      ])
      session.commit()

      rows = session.query(Job).order_by(Job.system).all()
      assert [r.system for r in rows] == ["aurora", "polaris"]

   def test_truly_identical_job_id_across_systems_also_ok(self, session):
      """The hostname suffix differs in practice, but the schema should
      not rely on that. Same exact string in two systems must work."""
      shared = "shared-id.example.alcf.anl.gov"
      session.add_all([
         Job(system="polaris", job_id=shared, state=JobState.QUEUED),
         Job(system="aurora",  job_id=shared, state=JobState.QUEUED),
      ])
      session.commit()
      assert session.query(Job).count() == 2

   def test_duplicate_system_job_id_rejected(self, session):
      session.add(Job(system="polaris", job_id=POLARIS_JOB, state=JobState.QUEUED))
      session.commit()
      session.add(Job(system="polaris", job_id=POLARIS_JOB, state=JobState.RUNNING))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()

   def test_missing_system_rejected(self, session):
      """system is NOT NULL; insert without it must fail."""
      session.add(Job(job_id="zzz.host.alcf.anl.gov", state=JobState.QUEUED))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()


class TestCompositeFkBehavior:
   """Child rows must reference an existing parent in the same system."""

   def test_orphan_job_history_rejected(self, session):
      # No matching jobs row exists yet.
      session.add(JobHistory(
         system="polaris", job_id="nonexistent.host", state=JobState.RUNNING,
      ))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()

   def test_history_for_valid_parent_inserts(self, session):
      session.add(Job(system="polaris", job_id=POLARIS_JOB, state=JobState.QUEUED))
      session.commit()
      session.add(JobHistory(
         system="polaris", job_id=POLARIS_JOB, state=JobState.RUNNING,
      ))
      session.commit()
      assert session.query(JobHistory).count() == 1

   def test_history_cross_system_orphan_rejected(self, session):
      """A polaris job_id with aurora system must NOT resolve to a
      polaris row \u2014 the FK is on the pair, not on job_id alone."""
      session.add(Job(system="polaris", job_id=POLARIS_JOB, state=JobState.QUEUED))
      session.commit()
      session.add(JobHistory(
         system="aurora", job_id=POLARIS_JOB, state=JobState.RUNNING,
      ))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()

   def test_queue_snapshot_fk_requires_matching_system(self, session):
      session.add(Queue(system="polaris", name="debug"))
      session.commit()
      # Same queue name in aurora; should be a different row.
      session.add(QueueSnapshot(
         system="aurora", queue_name="debug", state=QueueState.ENABLED_STARTED,
      ))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()


class TestNodeSnapshotIndexUniqueness:
   """snapshot_index used to be globally unique; now per-system unique."""

   def test_same_snapshot_index_across_systems_allowed(self, session):
      session.add_all([
         Node(system="polaris", name="x3000", snapshot_index=0),
         Node(system="aurora",  name="x4000", snapshot_index=0),
      ])
      session.commit()
      assert session.query(Node).count() == 2

   def test_duplicate_snapshot_index_within_system_rejected(self, session):
      session.add_all([
         Node(system="polaris", name="x3000", snapshot_index=0),
         Node(system="polaris", name="x3001", snapshot_index=0),
      ])
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()


class TestSystemColumnOnSnapshotTables:
   """node_snapshots / system_snapshots / data_collection_log got a
   system column but no FK rework. Verify inserts require it."""

   def test_node_snapshot_requires_system(self, session):
      session.add(NodeSnapshot(snapshot_data="F" * 10, node_count=10))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()

   def test_system_snapshot_requires_system(self, session):
      session.add(SystemSnapshot(total_jobs=42))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()

   def test_data_collection_log_requires_system(self, session):
      session.add(DataCollectionLog(
         collection_type="jobs", status=DataCollectionStatus.SUCCESS,
      ))
      with pytest.raises(IntegrityError):
         session.commit()
      session.rollback()
