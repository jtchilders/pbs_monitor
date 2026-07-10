"""Regression tests for reservation ingestion root-cause fixes.

Covers two bugs that produced duplicate / corrupt reservation rows in the DB:

Cause A — pbs_rstat_all_detailed() used to fall back to persisting the
  summary-parsed object (a *base* reservation id like ``M8644167`` with
  unreliable dates) when ``pbs_rstat -f`` failed, creating a SECOND row for the
  same reservation under a different primary key and a corrupted window.  It must
  now skip the reservation for that cycle instead.

Cause B — _parse_timing_field() resolved the summary's relative "Wkdy HH:MM" /
  "Today HH:MM" end stamp against the *collection* date, which collapsed
  (start == end) or inverted the window.  The duration is authoritative, so the
  end is now derived as start + duration.
"""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from pbs_monitor.models.reservation import PBSReservation, ReservationState


# --------------------------------------------------------------------------- #
# Cause B: summary timing parsing never yields a degenerate/inverted window
# --------------------------------------------------------------------------- #

class TestSummaryTimingParsing:
    def test_running_reservation_relative_stamps_do_not_collapse_window(self):
        # Real Aurora line for a running reservation: PBS emits weekday-only
        # start/end stamps.  Before the fix this parsed to start == end == today.
        line = ("M8644167.aurora M8644167      mluczkow RN            "
                "Tue 11:00 / 345600 / Sat 11:00")
        r = PBSReservation.from_summary_line(line)

        assert r.start_time is not None
        assert r.end_time is not None
        # The window must NOT be zero-length or inverted.
        assert r.end_time > r.start_time
        # End is derived from the authoritative duration.
        assert r.duration_seconds == 345600
        assert (r.end_time - r.start_time) == timedelta(seconds=345600)

    def test_end_derived_from_duration_matches_duration(self):
        line = ("M8660121.aurora M8660121      appmm2pb DG     "
                "Mon Jul 13 09:00 / 52200 / Mon Jul 13 23:30")
        r = PBSReservation.from_summary_line(line)
        assert (r.end_time - r.start_time).total_seconds() == r.duration_seconds

    def test_full_date_stamps_still_parse_correctly(self):
        line = ("M8652267.aurora M8652267      bgeltz@* DG     "
                "Tue Jul 14 10:00 / 43200 / Tue Jul 14 22:00")
        r = PBSReservation.from_summary_line(line)
        assert r.start_time.month == 7 and r.start_time.day == 14
        assert r.start_time.hour == 10
        assert (r.end_time - r.start_time).total_seconds() == 43200

    def test_timing_field_prefers_duration_over_parsed_end(self):
        # Directly exercise the helper: an end string that would parse to an
        # earlier time than start must be overridden by start + duration.
        start, dur, end = PBSReservation._parse_timing_field(
            "Tue 11:00 / 345600 / Sat 11:00"
        )
        assert start is not None
        assert dur == 345600
        assert end == start + timedelta(seconds=dur)


# --------------------------------------------------------------------------- #
# Cause A: pbs_rstat_all_detailed skips (never fabricates a base-id duplicate)
# --------------------------------------------------------------------------- #

class _FakeCommands:
    """Minimal stand-in exposing the real pbs_rstat_all_detailed via delegation.

    We build a real PBSCommands-like object by binding the unbound method to a
    lightweight namespace carrying just what the method touches.
    """


def _make_commands(summary_objs, detail_side_effect):
    from pbs_monitor.pbs_commands import PBSCommands
    cmd = PBSCommands.__new__(PBSCommands)
    cmd.logger = MagicMock()
    cmd.pbs_rstat_summary = MagicMock(return_value=summary_objs)
    cmd.pbs_rstat_detailed = MagicMock(side_effect=detail_side_effect)
    return cmd


def _summary_obj(base_id):
    # Mimic from_summary_line output: base id, source=summary marker.
    return PBSReservation(
        reservation_id=base_id,
        state=ReservationState.RUNNING_SHORT,
        raw_attributes={"source": "summary", "full_id": base_id + ".aurora"},
    )


def _detailed_obj(full_id):
    return PBSReservation(
        reservation_id=full_id,
        reservation_name="realname",
        state=ReservationState.RUNNING,
        nodes=260,
    )


def test_detail_success_returns_full_id_record():
    summary = [_summary_obj("M8644167")]
    cmd = _make_commands(
        summary,
        detail_side_effect=lambda rid: _detailed_obj(
            "M8644167.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov"
        ),
    )
    out = cmd.pbs_rstat_all_detailed()
    assert len(out) == 1
    # The canonical full id is used, NOT the base summary id.
    assert out[0].reservation_id.startswith("M8644167.aurora-pbs-0001")
    assert out[0].reservation_id != "M8644167"


def test_detail_failure_skips_rather_than_duplicating():
    summary = [_summary_obj("M8644167")]
    cmd = _make_commands(
        summary,
        detail_side_effect=Exception("pbs_rstat -f timed out"),
    )
    out = cmd.pbs_rstat_all_detailed()
    # Must NOT fall back to the base-id summary object → no duplicate row.
    assert out == []
    # Both the initial attempt and the retry were made (2 calls).
    assert cmd.pbs_rstat_detailed.call_count == 2


def test_detail_transient_failure_then_retry_succeeds():
    summary = [_summary_obj("M8644167")]
    full_id = "M8644167.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov"
    calls = {"n": 0}

    def flaky(rid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise Exception("transient")
        return _detailed_obj(full_id)

    cmd = _make_commands(summary, detail_side_effect=flaky)
    out = cmd.pbs_rstat_all_detailed()
    assert len(out) == 1
    assert out[0].reservation_id == full_id


def test_mixed_batch_keeps_good_skips_bad():
    summary = [_summary_obj("GOOD"), _summary_obj("BAD")]

    def side_effect(rid):
        if rid == "GOOD":
            return _detailed_obj("GOOD.aurora-pbs-0001")
        raise Exception("no detail for BAD")

    cmd = _make_commands(summary, detail_side_effect=side_effect)
    out = cmd.pbs_rstat_all_detailed()
    ids = [r.reservation_id for r in out]
    assert ids == ["GOOD.aurora-pbs-0001"]
    # BAD is absent — no base-id duplicate persisted.
    assert "BAD" not in ids
