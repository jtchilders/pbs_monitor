"""Tests for the v1.4 run_count column: migration, backfill, and its use in
walltime-efficiency normalization.

run_count is PBS's count of run attempts (raw_pbs_data.run_count). A value > 1
means the job was requeued/rerun. The walltime-efficiency analyzer normalizes
its denominator by run_count and excludes jobs that never actually occupied
nodes, so a job PBS requeued to death no longer launders into "100% efficient".
"""

import json

from sqlalchemy import text

from pbs_monitor.config import Config
from pbs_monitor.database.migrations import DatabaseMigration
from pbs_monitor.database.models import Job


class TestMigrationV14:
    def test_fresh_db_reports_v14(self, tmp_path):
        db = tmp_path / "v14.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        from sqlalchemy import inspect
        cols = [c["name"] for c in inspect(m.db_manager.engine).get_columns("jobs")]
        assert "run_count" in cols
        assert m.check_schema_version() == "1.4.0"

    def test_migrate_adds_column_and_idempotent(self, tmp_path):
        db = tmp_path / "v14mig.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        # Explicit re-run must not raise.
        m.migrate_to_v1_4_run_count()
        m.migrate_to_v1_4_run_count()


class TestBackfillRunCount:
    def test_backfill_populates_defaults_and_leaves_norun_null(self, tmp_path):
        db = tmp_path / "rcbf.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()

        with m.db_manager.get_session() as s:
            # explicit run_count in raw data
            s.execute(text(
                "INSERT INTO jobs (job_id, state, raw_pbs_data) "
                "VALUES ('rc21', 'FINISHED', :raw)"
            ), {"raw": json.dumps({"run_count": 21,
                                   "comment": "job held, too many failed attempts"})})
            # ran once, no explicit run_count -> defaults to 1
            s.execute(text(
                "INSERT INTO jobs (job_id, state, raw_pbs_data) "
                "VALUES ('ran1', 'FINISHED', :raw)"
            ), {"raw": json.dumps({"resources_used": {"walltime": "01:00:00"}})})
            # never ran, no run signal -> left NULL
            s.execute(text(
                "INSERT INTO jobs (job_id, state, raw_pbs_data) "
                "VALUES ('norun', 'FINISHED', :raw)"
            ), {"raw": json.dumps({"comment": "job held"})})
            s.commit()

        res = m.backfill_run_count(batch_size=100, dry_run=False)
        assert res["updated"] == 2
        assert res["skipped"] == 1
        assert res["errors"] == 0

        with m.db_manager.get_session() as s:
            rc21 = s.execute(text("SELECT run_count FROM jobs WHERE job_id='rc21'")).scalar()
            ran1 = s.execute(text("SELECT run_count FROM jobs WHERE job_id='ran1'")).scalar()
            norun = s.execute(text("SELECT run_count FROM jobs WHERE job_id='norun'")).scalar()
        assert rc21 == 21
        assert ran1 == 1
        assert norun is None

        # Idempotent.
        res2 = m.backfill_run_count(batch_size=100, dry_run=False)
        assert res2["updated"] == 0


class TestEfficiencyRunCountNormalization:
    def _analyzer(self, cfg):
        from pbs_monitor.database.repositories import RepositoryFactory
        from pbs_monitor.analytics.walltime_efficiency import WalltimeEfficiencyAnalyzer
        return WalltimeEfficiencyAnalyzer(RepositoryFactory(cfg))

    def test_never_ran_job_excluded(self, tmp_path):
        db = tmp_path / "eff1.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        an = self._analyzer(cfg)
        # A never-ran, heavily-requeued, failed job: no occupied, no runtime.
        with m.db_manager.get_session() as s:
            s.execute(text(
                "INSERT INTO jobs (job_id, state, walltime, run_count, "
                "occupied_seconds, actual_runtime_seconds, exit_status, outcome_class) "
                "VALUES ('held', 'FINISHED', '01:30:00', 21, NULL, NULL, -3, 'could_not_run')"
            ))
            s.commit()
            j = s.query(Job).filter(Job.job_id == "held").one()
            assert an._compute_job_efficiency(j) is None

    def test_requeued_job_denominator_normalized(self, tmp_path):
        db = tmp_path / "eff2.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        an = self._analyzer(cfg)
        # Ran 3600s of occupancy, requested 3600s, but requeued 3 times.
        # normalized eff = 3600 / (3600*3) = 33.3%, NOT 100%.
        with m.db_manager.get_session() as s:
            s.execute(text(
                "INSERT INTO jobs (job_id, state, walltime, run_count, "
                "occupied_seconds) "
                "VALUES ('rq', 'FINISHED', '01:00:00', 3, 3600)"
            ))
            s.commit()
            j = s.query(Job).filter(Job.job_id == "rq").one()
            r = an._compute_job_efficiency(j)
            assert r is not None
            assert abs(r["efficiency"] - (100.0 / 3)) < 0.01
            assert r["run_count"] == 3

    def test_single_run_job_unchanged(self, tmp_path):
        db = tmp_path / "eff3.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        an = self._analyzer(cfg)
        with m.db_manager.get_session() as s:
            s.execute(text(
                "INSERT INTO jobs (job_id, state, walltime, run_count, "
                "occupied_seconds) "
                "VALUES ('one', 'FINISHED', '01:00:00', 1, 1800)"
            ))
            s.commit()
            j = s.query(Job).filter(Job.job_id == "one").one()
            r = an._compute_job_efficiency(j)
            assert r is not None
            assert abs(r["efficiency"] - 50.0) < 0.01

    def test_null_run_count_treated_as_single_attempt(self, tmp_path):
        # Rows collected before v1.4 / not yet backfilled have NULL run_count;
        # must not inflate the denominator.
        db = tmp_path / "eff4.db"
        cfg = Config()
        cfg.database.url = f"sqlite:///{db}"
        m = DatabaseMigration(cfg)
        m.create_fresh_database()
        an = self._analyzer(cfg)
        with m.db_manager.get_session() as s:
            s.execute(text(
                "INSERT INTO jobs (job_id, state, walltime, run_count, "
                "occupied_seconds) "
                "VALUES ('nullrc', 'FINISHED', '01:00:00', NULL, 1800)"
            ))
            s.commit()
            j = s.query(Job).filter(Job.job_id == "nullrc").one()
            r = an._compute_job_efficiency(j)
            assert r is not None
            assert abs(r["efficiency"] - 50.0) < 0.01
            assert r["run_count"] == 1
