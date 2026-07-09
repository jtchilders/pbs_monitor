"""Regression test: the data_collector database import chain must not break.

Guards against the circular-import class of bug where importing the analytics
*package* (analytics/__init__) from model_converters pulls in analyzers that
transitively import data_collector, leaving ModelConverters unbound and the
daemon dying with "name 'ModelConverters' is not defined".
"""


def test_data_collector_database_imports_available():
    """DATABASE_AVAILABLE must be True — a False value means the optional
    database import silently failed (e.g. a circular import), which breaks the
    daemon."""
    import pbs_monitor.data_collector as dc
    assert dc.DATABASE_AVAILABLE is True, (
        f"database import chain broken: {dc._DATABASE_IMPORT_ERROR!r}"
    )
    assert hasattr(dc, "ModelConverters")


def test_data_collector_init_with_database():
    """DataCollector(enable_database=True) must initialize without a NameError
    (this is the exact path that failed when the daemon started)."""
    from pbs_monitor.config import Config
    from pbs_monitor.data_collector import DataCollector

    collector = DataCollector(config=Config(), enable_database=True)
    assert collector._model_converters is not None
    assert type(collector._model_converters).__name__ == "ModelConverters"


def test_job_converter_lazy_classifier_import():
    """JobConverter.to_database must reach classify_exit via its lazy import."""
    from pbs_monitor.analytics.outcome_classifier import classify_exit

    assert classify_exit("FINISHED", 0) == "success"
    assert classify_exit("FINISHED", 143, 3500, 3600) == "walltime_killed"
