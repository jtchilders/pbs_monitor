"""
PBS Monitor Web Dashboard — FastAPI backend

Serves the live dashboard frontend and provides REST API endpoints
for system info, node state snapshots, running jobs, and queue status.
Reads from the same SQLite database the PBS Monitor daemon writes to.
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, event, func, or_, and_, text
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, List, Optional
import asyncio
import hashlib
import re
import socket
import time as _time

import json as _json

from pbs_monitor.database.models import (
    Job, JobState, Node, NodeSnapshot, SystemSnapshot,
    DataCollectionLog, Reservation, ReservationUtilization,
)

# State character → human-readable label
STATE_CHAR_LABELS = {
    'A': 'free', 'B': 'offline', 'C': 'down', 'D': 'busy',
    'E': 'job-exclusive', 'F': 'job-sharing', 'G': 'reserve',
    'H': 'resv-exclusive', 'I': 'down,offline',
    'J': 'state-unknown,down', 'K': 'state-unknown,down,offline',
    'L': 'job-exclusive,resv-exclusive', 'M': 'offline,resv-exclusive',
    'N': 'unknown',
}

# Static files directory (relative to this package)
STATIC_DIR = Path(__file__).parent / "static"


def _parse_walltime(wt: str | None) -> int | None:
    """Parse HH:MM:SS walltime string to total seconds."""
    if not wt:
        return None
    try:
        parts = wt.split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return None


def _short_job_id(job_id: str) -> str:
    """Strip PBS server suffix: '7159563.polaris-pbs-01...' → '7159563'."""
    return job_id.split('.')[0] if job_id else job_id


def _parse_execution_nodes(exec_node: str | None) -> list[str]:
    """
    Parse PBS execution_node field to a list of node names.
    Format: 'x3001c0s1b0n0/0*64+x3001c0s1b1n0/0*64+...'
    """
    if not exec_node:
        return []
    names = []
    for chunk in exec_node.split('+'):
        name = chunk.split('/')[0].strip()
        if name:
            names.append(name)
    return names


# Default server resource defaults for score calculation
_SERVER_DEFAULTS = {
    "base_score": 0,
    "score_boost": 0,
    "enable_wfp": 0,
    "wfp_factor": 100000,
    "enable_backfill": 0,
    "backfill_max": 50,
    "backfill_factor": 84600,
    "enable_fifo": 1,
    "fifo_factor": 1800,
    "total_cpus": 1,
}


def _parse_time_str(ts: str | None) -> int:
    """Parse PBS time string 'HH:MM:SS' or 'DDDD:HH:MM' to seconds."""
    if not ts:
        return 0
    try:
        parts = ts.split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 3600 + int(parts[1]) * 60
        return 0
    except (ValueError, IndexError):
        return 0


def _coerce_int(val, default: int = 0) -> int:
    """Coerce a value to int."""
    if isinstance(val, (int, float)):
        return int(val)
    try:
        s = str(val)
        return int(float(s)) if '.' in s else int(s)
    except (ValueError, TypeError):
        return default


def _compute_job_score(raw: dict, formula: str | None = None) -> float | None:
    """Compute job score from raw PBS data using the job_sort_formula.

    Uses the same approach as pbs_monitor.replay.state_tracker.ScoreCalculator:
    build a variables dict from Resource_List + eligible_time, then eval the
    formula.  Falls back to eligible_time in seconds if no formula is set.
    """
    rl = raw.get("Resource_List", {})

    eligible_seconds = _parse_time_str(raw.get("eligible_time"))
    walltime_seconds = _parse_time_str(
        rl.get("walltime", raw.get("walltime", "01:00:00"))
    )

    if formula is None:
        # Fallback: eligible_time alone
        return float(eligible_seconds)

    variables = {
        "eligible_time": eligible_seconds,
        "walltime": walltime_seconds,
        "nodect": _coerce_int(rl.get("nodect", raw.get("nodect", 1)), 1),
        "base_score": _coerce_int(rl.get("base_score", _SERVER_DEFAULTS["base_score"])),
        "score_boost": _coerce_int(rl.get("score_boost", _SERVER_DEFAULTS["score_boost"])),
        "enable_wfp": _coerce_int(rl.get("enable_wfp", _SERVER_DEFAULTS["enable_wfp"])),
        "wfp_factor": _coerce_int(rl.get("wfp_factor", _SERVER_DEFAULTS["wfp_factor"])),
        "enable_backfill": _coerce_int(rl.get("enable_backfill", _SERVER_DEFAULTS["enable_backfill"])),
        "backfill_max": _coerce_int(rl.get("backfill_max", _SERVER_DEFAULTS["backfill_max"])),
        "backfill_factor": _coerce_int(rl.get("backfill_factor", _SERVER_DEFAULTS["backfill_factor"])),
        "enable_fifo": _coerce_int(rl.get("enable_fifo", _SERVER_DEFAULTS["enable_fifo"])),
        "fifo_factor": _coerce_int(rl.get("fifo_factor", _SERVER_DEFAULTS["fifo_factor"])),
        "project_priority": _coerce_int(rl.get("project_priority", 1), 1),
        "total_cpus": _coerce_int(rl.get("total_cpus", _SERVER_DEFAULTS["total_cpus"]), 1),
        "min": min,
        "max": max,
    }

    try:
        score = eval(formula, {"__builtins__": {}}, variables)
        return float(score)
    except Exception:
        # Fallback to eligible_time
        return float(eligible_seconds) if eligible_seconds else None


def _extract_job_score(job, formula: str | None = None) -> float | None:
    """Extract score for a Job ORM object."""
    if not job.raw_pbs_data:
        return None
    try:
        raw = _json.loads(job.raw_pbs_data) if isinstance(job.raw_pbs_data, str) else job.raw_pbs_data
    except (ValueError, TypeError):
        return None
    return _compute_job_score(raw, formula)


def _detect_system_name(db: Session) -> str:
    """Infer the system name from job IDs in the database."""
    sample = db.query(Job.job_id).filter(Job.job_id.isnot(None)).limit(10).all()
    for (jid,) in sample:
        # e.g. "7159563.polaris-pbs-01.hsn.cm.polaris.alcf.anl.gov"
        m = re.search(r'\.(\w+)-pbs', jid)
        if m:
            return m.group(1)
    return "unknown"


def _build_topology(db: Session) -> dict:
    """
    Build rack topology from Cray node naming conventions.
    Returns {rack_names: [...], nodes_per_rack: [...]}
    """
    nodes = (
        db.query(Node.name)
        .filter(Node.name.like('x%'))
        .order_by(Node.snapshot_index)
        .all()
    )
    rack_map: dict[str, list[str]] = {}
    for (name,) in nodes:
        rack_id = name[:5]  # e.g. 'x3001'
        rack_map.setdefault(rack_id, []).append(name)

    rack_names = sorted(rack_map.keys())
    nodes_per_rack = [len(rack_map[r]) for r in rack_names]
    return {"rack_names": rack_names, "nodes_per_rack": nodes_per_rack}


def _build_node_index(db: Session) -> tuple[list[str], list[int]]:
    """Ordered compute node names and their snapshot_data indices."""
    rows = (
        db.query(Node.name, Node.snapshot_index)
        .filter(Node.name.like('x%'))
        .order_by(Node.snapshot_index)
        .all()
    )
    names = [r.name for r in rows]
    indices = [r.snapshot_index for r in rows]
    return names, indices


# ---------------------------------------------------------------------------
# App factory — called by the CLI `web` command
# ---------------------------------------------------------------------------


def create_app(config=None) -> FastAPI:
    """Create and configure the FastAPI application."""
    # Resolve database URL
    if config is None:
        from pbs_monitor.config import Config
        config = Config()

    db_url = config.database.url
    connect_args: dict[str, Any] = {}
    if db_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        connect_args["timeout"] = 60
        # Open in URI/read-only mode (mode=ro):
        #   - SQLite never attempts a write, BEGIN IMMEDIATE, or PENDING/RESERVED
        #     lock, so the web server cannot contribute to lock contention against
        #     the daemon on Lustre/Flare (root cause of the 2026-05-29 incident).
        #   - mode=ro still takes a brief SHARED lock per read transaction, which
        #     is compatible with the daemon's PENDING/RESERVED locks; readers and
        #     the daemon block each other only at the daemon's EXCLUSIVE commit
        #     moment, which is expected and transient.
        #   - immutable=1 is intentionally NOT set: it disables change detection
        #     and page-cache invalidation, which would cause the web server to
        #     serve stale or internally inconsistent data while the daemon writes.
        raw_path = db_url.replace("sqlite:///", "")
        engine = create_engine(
            f"sqlite:///file:{raw_path}?mode=ro&uri=true",
            connect_args=connect_args,
        )
    else:
        engine = create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        # Apply schema search_path for PostgreSQL multi-system deployments
        schema = getattr(config.database, 'schema', 'public') or 'public'
        if schema != 'public':
            @event.listens_for(engine, 'connect')
            def set_search_path(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute(f'SET search_path TO "{schema}", public')
                cursor.close()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    app = FastAPI(title="PBS Monitor Dashboard")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- dependency ----
    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            # Mask KeyboardInterrupt / GeneratorExit during close so that a
            # Ctrl-C landing inside pysqlite's connection teardown cannot leave
            # a stale lock or hot journal on Lustre (the root cause of the
            # 2026-05-29 lock-storm incident).
            try:
                db.close()
            except BaseException:
                # Interrupt landed during teardown. Mark this session's
                # connection as broken so SQLAlchemy's pool discards it
                # rather than handing it out again.
                try:
                    db.invalidate()
                except Exception:
                    pass
                raise

    # ---- cached PBS score config (loaded lazily on first request) ----
    _score_config: dict[str, Any] = {}  # {"formula": str|None, "loaded": bool}

    def _get_job_formula() -> str | None:
        """Lazy-load the PBS job sort formula and server defaults.

        Called on the first API request, not at Uvicorn startup.
        Result is cached for the lifetime of the process.
        """
        if _score_config.get("loaded"):
            return _score_config.get("formula")

        _score_config["loaded"] = True
        import logging
        log = logging.getLogger(__name__)
        try:
            from pbs_monitor.pbs_commands import PBSCommands
            pbs = PBSCommands(timeout=10)
            server_data = pbs.qstat_server()
            formula = pbs.get_job_sort_formula(server_data=server_data)
            # Update server defaults from live PBS data
            server_info = server_data.get("Server", {})
            for _name, details in server_info.items():
                pbs_defaults = details.get("resources_default", {})
                if pbs_defaults:
                    _SERVER_DEFAULTS.update({
                        k: _coerce_int(v) for k, v in pbs_defaults.items()
                        if k in _SERVER_DEFAULTS
                    })
                break
            if formula:
                log.info(f"Loaded PBS job sort formula: {formula}")
            _score_config["formula"] = formula
            return formula
        except Exception as e:
            log.info(f"PBS commands unavailable, scores will use eligible_time fallback: {e}")
            _score_config["formula"] = None
            return None

    # ---- cached system info ----
    _system_cache: dict[str, Any] = {}

    def _populate_system_cache(db: Session) -> dict[str, Any]:
        """Build and cache system info."""
        if _system_cache:
            return _system_cache

        system_name = _detect_system_name(db)
        total_nodes = db.query(func.count(Node.name)).filter(Node.name.like('x%')).scalar() or 0
        topology = _build_topology(db)
        node_names, snapshot_indices = _build_node_index(db)

        last_log = (
            db.query(DataCollectionLog.timestamp)
            .order_by(DataCollectionLog.timestamp.desc())
            .first()
        )

        # CPUs per node by system — used for job misconfiguration detection
        CPUS_PER_NODE_BY_SYSTEM = {
            "polaris": 32,   # 2x AMD EPYC Rome 16-core
            "aurora":  208,  # 2x Intel Xeon Max 9470 (52-core HT)
            "sophia":  128,  # 2x AMD EPYC Milan 64-core
        }
        cpus_per_node = next(
            (v for k, v in CPUS_PER_NODE_BY_SYSTEM.items() if k in system_name.lower()),
            None,
        )

        info = {
            "system_name": system_name,
            "server_host": socket.gethostname(),
            "total_nodes": total_nodes,
            "topology": topology,
            "node_index": node_names,
            "snapshot_indices": snapshot_indices,
            "last_collection": last_log[0].isoformat() if last_log else None,
            "job_sort_formula": _get_job_formula(),
            "cpus_per_node": cpus_per_node,
        }
        _system_cache.update(info)
        return info

    @app.get("/api/system")
    def api_system(db: Session = Depends(get_db)):
        return _populate_system_cache(db)

    @app.get("/api/snapshot")
    def api_snapshot(db: Session = Depends(get_db)):
        now = datetime.now(timezone.utc)

        # Ensure system cache is populated (needed for snapshot_indices)
        if not _system_cache:
            _populate_system_cache(db)

        # --- freshest data timestamp ---
        latest_collection = (
            db.query(func.max(DataCollectionLog.timestamp)).scalar()
        )

        # --- system aggregate ---
        sys_snap = (
            db.query(SystemSnapshot)
            .order_by(SystemSnapshot.timestamp.desc())
            .first()
        )

        # --- node state string ---
        node_snap = (
            db.query(NodeSnapshot)
            .order_by(NodeSnapshot.timestamp.desc())
            .first()
        )
        state_string = node_snap.snapshot_data if node_snap else ""

        # State counts — only for compute nodes (use their snapshot indices)
        compute_indices = _system_cache.get("snapshot_indices", [])
        state_counts: dict[str, int] = {}
        for si in compute_indices:
            if si < len(state_string):
                ch = state_string[si]
                label = STATE_CHAR_LABELS.get(ch, "unknown")
                state_counts[label] = state_counts.get(label, 0) + 1

        # --- node name → snapshot_index lookup (compute nodes only) ---
        node_map: dict[str, int] = {
            n.name: n.snapshot_index
            for n in db.query(Node.name, Node.snapshot_index)
            .filter(Node.name.like('x%'))
            .all()
        }

        # --- running jobs ---
        running_rows = db.query(Job).filter(Job.state == JobState.RUNNING).all()
        running_jobs = []
        for job in running_rows:
            elapsed = 0
            remaining = 0
            wall_secs = _parse_walltime(job.walltime)
            if job.start_time:
                start_utc = job.start_time
                if start_utc.tzinfo is None:
                    start_utc = start_utc.replace(tzinfo=timezone.utc)
                elapsed = int((now - start_utc).total_seconds())
                if wall_secs is not None:
                    remaining = max(0, wall_secs - elapsed)

            exec_names = _parse_execution_nodes(job.execution_node)
            node_indices = [node_map[n] for n in exec_names if n in node_map]

            queue_time = job.queue_time_seconds
            if queue_time is None and job.start_time and job.submit_time:
                st = job.start_time
                su = job.submit_time
                if st.tzinfo is None:
                    st = st.replace(tzinfo=timezone.utc)
                if su.tzinfo is None:
                    su = su.replace(tzinfo=timezone.utc)
                queue_time = int((st - su).total_seconds())

            running_jobs.append({
                "job_id": _short_job_id(job.job_id),
                "full_job_id": job.job_id,
                "name": job.job_name or "",
                "owner": job.owner or "",
                "project": job.project or "",
                "allocation_type": job.allocation_type or "",
                "queue": job.queue or "",
                "nodes": job.nodes or len(exec_names) or 1,
                "walltime": job.walltime or "",
                "elapsed_seconds": elapsed,
                "remaining_seconds": remaining,
                "queue_time_seconds": queue_time or 0,
                "node_indices": node_indices,
                "score": _extract_job_score(job, _get_job_formula()),
            })

        # --- queued jobs (full detail for table) ---
        queued_rows = db.query(Job).filter(Job.state == JobState.QUEUED).all()
        queued_jobs = []
        for job in queued_rows:
            queue_time = 0
            if job.submit_time:
                su = job.submit_time
                if su.tzinfo is None:
                    su = su.replace(tzinfo=timezone.utc)
                queue_time = int((now - su).total_seconds())

            # Extract score from raw PBS data
            score = _extract_job_score(job, _get_job_formula())

            # Extract comment and ncpus from raw PBS data for job classification
            raw = job.raw_pbs_data or {}
            rl = raw.get("Resource_List", {})
            ncpus_req = rl.get("ncpus")
            comment = raw.get("comment", "")

            queued_jobs.append({
                "job_id": _short_job_id(job.job_id),
                "full_job_id": job.job_id,
                "name": job.job_name or "",
                "owner": job.owner or "",
                "project": job.project or "",
                "allocation_type": job.allocation_type or "",
                "queue": job.queue or "",
                "nodes": job.nodes or 1,
                "walltime": job.walltime or "",
                "queue_time_seconds": queue_time,
                "score": score,
                "ncpus_requested": ncpus_req,
                "comment": comment,
            })

        # --- held jobs count ---
        held_count = db.query(func.count(Job.job_id)).filter(Job.state == JobState.HELD).scalar() or 0

        # --- queue node-hours for queue status bars ---
        def _job_node_hours(job) -> float:
            """Compute node-hours for a job: nodes × walltime_hours."""
            nodes = job.nodes or 1
            wt_sec = _parse_walltime(job.walltime) or 3600
            return nodes * wt_sec / 3600.0

        # Accumulate node-hours per queue per state
        nh_running: dict[str, float] = {}
        nh_queued: dict[str, float] = {}
        nh_held: dict[str, float] = {}

        for job in running_rows:
            q = job.queue or ""
            nh_running[q] = nh_running.get(q, 0) + _job_node_hours(job)

        for job in queued_rows:
            q = job.queue or ""
            nh_queued[q] = nh_queued.get(q, 0) + _job_node_hours(job)

        held_rows = db.query(Job).filter(Job.state == JobState.HELD).all()
        held_jobs = []
        for job in held_rows:
            q = job.queue or ""
            nh_held[q] = nh_held.get(q, 0) + _job_node_hours(job)
            queue_time = 0
            if job.submit_time:
                su = job.submit_time
                if su.tzinfo is None:
                    su = su.replace(tzinfo=timezone.utc)
                queue_time = int((now - su).total_seconds())
            held_jobs.append({
                "job_id": _short_job_id(job.job_id),
                "full_job_id": job.job_id,
                "name": job.job_name or "",
                "owner": job.owner or "",
                "project": job.project or "",
                "allocation_type": job.allocation_type or "",
                "queue": job.queue or "",
                "nodes": job.nodes or 1,
                "walltime": job.walltime or "",
                "queue_time_seconds": queue_time,
                "score": _extract_job_score(job, _get_job_formula()),
            })

        all_queue_names = set(nh_running) | set(nh_queued) | set(nh_held)

        queues = []
        for qname in all_queue_names:
            if not qname:
                continue
            r = round(nh_running.get(qname, 0), 1)
            q = round(nh_queued.get(qname, 0), 1)
            h = round(nh_held.get(qname, 0), 1)
            total = r + q + h
            if total == 0:
                continue
            queues.append({
                "name": qname,
                "running": r,
                "queued": q,
                "held": h,
                "total": round(total, 1),
            })

        # Use the freshest timestamp available
        best_ts = latest_collection or (
            sys_snap.timestamp if sys_snap else
            node_snap.timestamp if node_snap else None
        )

        return {
            "timestamp": best_ts.isoformat() if best_ts else None,
            "system": {
                "running_jobs": sys_snap.running_jobs if sys_snap else len(running_jobs),
                "queued_jobs": sys_snap.queued_jobs if sys_snap else len(queued_rows),
                "held_jobs": sys_snap.held_jobs if sys_snap else held_count,
                "utilization_percent": round(sys_snap.system_utilization_percent or 0, 1) if sys_snap else 0,
                "total_nodes": sys_snap.total_nodes if sys_snap else 0,
                "available_nodes": sys_snap.available_nodes if sys_snap else 0,
            },
            "state_string": state_string,
            "state_counts": state_counts,
            "jobs": {
                "running": running_jobs,
                "queued": queued_jobs,
                "held": held_jobs,
            },
            "queues": queues,
        }

    @app.get("/api/jobs/{job_id}")
    def api_job_detail(job_id: str, db: Session = Depends(get_db)):
        # Try exact match first, then with short id
        job = db.query(Job).filter(Job.job_id == job_id).first()
        if not job:
            job = db.query(Job).filter(Job.job_id.like(f"{job_id}.%")).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        # Compute elapsed / remaining for running jobs
        now = datetime.now(timezone.utc)
        elapsed_seconds = None
        remaining_seconds = None
        wall_secs = _parse_walltime(job.walltime)
        if job.start_time and job.state and job.state.value == "R":
            start_utc = job.start_time
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
            elapsed_seconds = int((now - start_utc).total_seconds())
            if wall_secs is not None:
                remaining_seconds = max(0, wall_secs - elapsed_seconds)

        # Extract useful fields from raw PBS data
        raw = {}
        if job.raw_pbs_data:
            try:
                raw = _json.loads(job.raw_pbs_data) if isinstance(job.raw_pbs_data, str) else (job.raw_pbs_data or {})
            except (ValueError, TypeError):
                raw = {}

        rl = raw.get("Resource_List", {})
        resources_used = raw.get("resources_used", {})

        # Parse node list into something readable
        exec_names = _parse_execution_nodes(job.execution_node)
        unique_nodes = list(dict.fromkeys(exec_names))  # deduplicated, order preserved

        return {
            # Identity
            "job_id": _short_job_id(job.job_id),
            "full_job_id": job.job_id,
            "job_name": job.job_name,
            "state": job.state.value if job.state else None,

            # Ownership
            "owner": job.owner,
            "project": job.project,
            "allocation_type": job.allocation_type,
            "queue": job.queue,

            # Resources requested
            "nodes": job.nodes,
            "total_cores": job.total_cores,
            "walltime": job.walltime,
            "memory_requested": rl.get("mem") or rl.get("pmem"),
            "ncpus_requested": rl.get("ncpus"),
            "mpiprocs": rl.get("mpiprocs"),
            "ompthreads": rl.get("ompthreads"),
            "select": rl.get("select"),
            "place": rl.get("place"),

            # Resources used (populated when job completes or is running)
            "cpu_used": resources_used.get("cpupercent"),
            "mem_used": resources_used.get("mem"),
            "vmem_used": resources_used.get("vmem"),
            "walltime_used": resources_used.get("walltime"),
            "ncpus_used": resources_used.get("ncpus"),

            # Timing
            "submit_time": job.submit_time.isoformat() if job.submit_time else None,
            "start_time": job.start_time.isoformat() if job.start_time else None,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "elapsed_seconds": elapsed_seconds,
            "remaining_seconds": remaining_seconds,
            "walltime_seconds": wall_secs,
            "actual_runtime_seconds": job.actual_runtime_seconds,
            "queue_time_seconds": job.queue_time_seconds,

            # Placement
            "execution_nodes": unique_nodes,
            "execution_node_count": len(unique_nodes),

            # Score
            "score": _extract_job_score(job, _get_job_formula()),

            # PBS internals (useful for debugging / power users)
            "priority": raw.get("Priority"),
            "eligible_time": raw.get("eligible_time"),
            "comment": raw.get("comment"),
            "exit_status": raw.get("Exit_status"),
            "array_index": raw.get("array_index"),
            "job_array_id": raw.get("array_id"),

            # Full raw PBS attribute dict (already parsed above). Returned so the
            # detail modal can render a collapsible "Raw PBS Attributes" section
            # for the many fields that don't get a dedicated column/label. This
            # rides the existing single-row per-click fetch, so it adds no cost
            # to any list/analytics endpoint (those deliberately avoid
            # raw_pbs_data via with_entities).
            "raw_pbs_data": raw or None,
        }

    # ---- context page routes (must be before static mounts) ----

    @app.get("/page/user/{username}")
    async def serve_user_page(username: str):
        return FileResponse(STATIC_DIR / "user.html")

    @app.get("/page/project/{project}")
    async def serve_project_page(project: str):
        return FileResponse(STATIC_DIR / "project.html")

    @app.get("/page/reservation/{reservation_id}")
    async def serve_reservation_page(reservation_id: str):
        return FileResponse(STATIC_DIR / "reservation.html")

    # ---- user/project API endpoints ----

    def _date_range(range_days: int) -> datetime:
        """Return UTC datetime for range_days ago."""
        return datetime.now(timezone.utc) - timedelta(days=range_days)

    def _all_time_stats(db: Session, column, value: str) -> dict:
        """Entity-wide job totals ignoring any time window.

        Cheap aggregate (COUNT + MIN/MAX submit_time) filtered only by the
        owner/project column, so the detail page can tell the difference
        between an unknown entity and one whose activity predates the window.
        """
        row = db.query(
            func.count(Job.job_id),
            func.min(Job.submit_time),
            func.max(Job.submit_time),
        ).filter(column == value).one()
        total, first_ts, last_ts = row

        def _iso(dt):
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()

        return {
            "total_jobs": int(total or 0),
            "first_activity": _iso(first_ts),
            "last_activity": _iso(last_ts),
        }

    def _fill_date_series(counts: dict, start: datetime, range_days: int) -> list[dict]:
        """Fill a date→count/value dict with zeros for missing days."""
        result = []
        for i in range(range_days):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            result.append({"date": d, "count": counts.get(d, 0)})
        return result

    def _fill_nh_series(counts: dict, start: datetime, range_days: int) -> list[dict]:
        result = []
        for i in range(range_days):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            result.append({"date": d, "node_hours": round(counts.get(d, 0.0), 2)})
        return result

    def _build_summary(jobs: list, name: str, kind: str, range_days: int,
                       all_time: dict | None = None) -> dict:
        """Build summary stats from a list of Job ORM objects.

        ``all_time`` (optional) carries entity-wide totals that ignore the
        time window — used by the frontend to render a helpful empty state
        when the selected window has no jobs but the entity has history.
        """
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=range_days)

        state_counts: dict[str, int] = {}
        total_node_hours = 0.0
        runtime_sum = 0
        runtime_count = 0
        queue_sum = 0
        queue_count = 0
        jobs_by_day: dict[str, int] = {}
        nh_by_day: dict[str, float] = {}

        for job in jobs:
            # state counts — use .name for human-readable key (HELD not H)
            st = job.state.name if job.state else "UNKNOWN"
            state_counts[st] = state_counts.get(st, 0) + 1

            # node-hours: use actual runtime if available, else walltime
            nodes = job.nodes or 1
            runtime_sec = job.actual_runtime_seconds
            if not runtime_sec and job.start_time and job.end_time:
                st_t = job.start_time.replace(tzinfo=timezone.utc) if job.start_time.tzinfo is None else job.start_time
                et_t = job.end_time.replace(tzinfo=timezone.utc) if job.end_time.tzinfo is None else job.end_time
                runtime_sec = int((et_t - st_t).total_seconds())
            if not runtime_sec:
                wt_sec = _parse_walltime(job.walltime) or 0
                runtime_sec = wt_sec
            nh = nodes * runtime_sec / 3600.0
            total_node_hours += nh

            # runtime stats (only jobs with actual runtime)
            if job.actual_runtime_seconds:
                runtime_sum += job.actual_runtime_seconds
                runtime_count += 1

            # queue time stats
            if job.queue_time_seconds:
                queue_sum += job.queue_time_seconds
                queue_count += 1

            # per-day buckets — group by submit_time date
            if job.submit_time:
                su = job.submit_time.replace(tzinfo=timezone.utc) if job.submit_time.tzinfo is None else job.submit_time
                d = su.strftime("%Y-%m-%d")
                jobs_by_day[d] = jobs_by_day.get(d, 0) + 1
                nh_by_day[d] = nh_by_day.get(d, 0.0) + nh

        return {
            "name": name,
            "kind": kind,
            "range_days": range_days,
            "total_jobs": len(jobs),
            "total_node_hours": round(total_node_hours, 1),
            "avg_queue_time_seconds": int(queue_sum / queue_count) if queue_count else None,
            "avg_runtime_seconds": int(runtime_sum / runtime_count) if runtime_count else None,
            "state_counts": state_counts,
            "jobs_per_day": _fill_date_series(jobs_by_day, start, range_days),
            "node_hours_per_day": _fill_nh_series(nh_by_day, start, range_days),
            # Entity-wide totals ignoring the time window. Lets the frontend
            # distinguish "no such user/project" from "no jobs in this window"
            # and offer a jump to the full history.
            "all_time": all_time or {"total_jobs": 0, "first_activity": None,
                                     "last_activity": None},
        }

    def _serialize_job(job, now: datetime) -> dict:
        """Serialize a Job ORM object to a dict for context page job lists."""
        nodes = job.nodes or 1
        runtime_sec = job.actual_runtime_seconds
        if not runtime_sec and job.start_time and job.end_time:
            st_t = job.start_time.replace(tzinfo=timezone.utc) if job.start_time.tzinfo is None else job.start_time
            et_t = job.end_time.replace(tzinfo=timezone.utc) if job.end_time.tzinfo is None else job.end_time
            runtime_sec = int((et_t - st_t).total_seconds())
        wt_sec = _parse_walltime(job.walltime) or 0
        node_hours = round(nodes * (runtime_sec or wt_sec) / 3600.0, 2)

        # Queue time: prefer stored value, then start-submit delta, then now-submit fallback
        queue_time = job.queue_time_seconds
        if queue_time is None and job.start_time and job.submit_time:
            st_t = job.start_time.replace(tzinfo=timezone.utc) if job.start_time.tzinfo is None else job.start_time
            su_t = job.submit_time.replace(tzinfo=timezone.utc) if job.submit_time.tzinfo is None else job.submit_time
            queue_time = int((st_t - su_t).total_seconds())
        if queue_time is None and job.submit_time:
            su_t = job.submit_time.replace(tzinfo=timezone.utc) if job.submit_time.tzinfo is None else job.submit_time
            queue_time = int((now - su_t).total_seconds())

        return {
            "job_id": _short_job_id(job.job_id),
            "full_job_id": job.job_id,
            "name": job.job_name or "",
            "state": job.state.name if job.state else "",  # .name = 'HELD', .value = 'H'
            "owner": job.owner or "",
            "project": job.project or "",
            "allocation_type": job.allocation_type or "",
            "queue": job.queue or "",
            "nodes": nodes,
            "walltime": job.walltime or "",
            "submit_time": job.submit_time.isoformat() if job.submit_time else None,
            "start_time": job.start_time.isoformat() if job.start_time else None,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "actual_runtime_seconds": runtime_sec,
            "queue_time_seconds": queue_time or 0,
            "node_hours": node_hours,
            "score": _extract_job_score(job, _get_job_formula()),
        }

    def _query_jobs(db: Session, filter_col, filter_val: str, range_days: int, state_filter: str):
        """Shared job query for user/project endpoints."""
        since = _date_range(range_days)
        q = db.query(Job).filter(filter_col == filter_val).filter(Job.submit_time >= since)
        if state_filter and state_filter.upper() != "ALL":
            # Map frontend state string to JobState enum value
            state_map = {
                "RUNNING": JobState.RUNNING,
                "QUEUED": JobState.QUEUED,
                "FINISHED": JobState.FINISHED,
                "HELD": JobState.HELD,
                "UNKNOWN_END": JobState.UNKNOWN_END,
            }
            js = state_map.get(state_filter.upper())
            if js:
                q = q.filter(Job.state == js)
        return q.order_by(Job.submit_time.desc()).all()

    @app.get("/api/user/{username}/summary")
    def api_user_summary(
        username: str,
        range: int = Query(7, ge=1, le=3650),
        db: Session = Depends(get_db),
    ):
        jobs = db.query(Job).filter(Job.owner == username).filter(
            Job.submit_time >= _date_range(range)
        ).all()
        all_time = _all_time_stats(db, Job.owner, username)
        return _build_summary(jobs, username, "user", range, all_time)

    @app.get("/api/user/{username}/jobs")
    def api_user_jobs(
        username: str,
        range: int = Query(7, ge=1, le=3650),
        state: str = Query("ALL"),
        db: Session = Depends(get_db),
    ):
        now = datetime.now(timezone.utc)
        jobs = _query_jobs(db, Job.owner, username, range, state)
        return {"total": len(jobs), "jobs": [_serialize_job(j, now) for j in jobs]}

    @app.get("/api/project/{project}/summary")
    def api_project_summary(
        project: str,
        range: int = Query(7, ge=1, le=3650),
        db: Session = Depends(get_db),
    ):
        jobs = db.query(Job).filter(Job.project == project).filter(
            Job.submit_time >= _date_range(range)
        ).all()
        all_time = _all_time_stats(db, Job.project, project)
        return _build_summary(jobs, project, "project", range, all_time)

    @app.get("/api/project/{project}/jobs")
    def api_project_jobs(
        project: str,
        range: int = Query(7, ge=1, le=3650),
        state: str = Query("ALL"),
        db: Session = Depends(get_db),
    ):
        now = datetime.now(timezone.utc)
        jobs = _query_jobs(db, Job.project, project, range, state)
        return {"total": len(jobs), "jobs": [_serialize_job(j, now) for j in jobs]}

    # ---- reservation detail API endpoints ----

    def _naive(dt):
        """Strip tzinfo for naive-datetime DBs (SQLite); no-op if already naive."""
        if dt is not None and hasattr(dt, "tzinfo") and dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt

    def _resv_jobs_query(db: Session, r):
        """Jobs that ran in a reservation's queue overlapping its time window.

        Mirrors the linkage used by the reservations list + analytics timeline:
        a job belongs to a reservation if it ran in the reservation's dedicated
        queue and its run window overlaps the reservation window.
        """
        if not r.queue:
            return []
        start_t = _naive(r.start_time)
        end_t = _naive(r.end_time)
        q = db.query(Job).filter(Job.queue == r.queue)
        # Overlap: job started before reservation ended, and (no end or ended
        # after reservation started). Only meaningful when the window is known.
        if start_t is not None:
            q = q.filter(
                or_(Job.end_time == None, Job.end_time >= start_t)  # noqa: E711
            )
        if end_t is not None:
            q = q.filter(
                or_(Job.start_time == None, Job.start_time <= end_t)  # noqa: E711
            )
        return q.order_by(Job.start_time.asc().nullslast()).all()

    def _compute_reservation_utilization(db: Session, r) -> dict:
        """Compute node-hour utilization for a reservation using live binning+capping.

        This is the single canonical implementation shared by both the detail
        endpoint (api_reservation_summary) and the table endpoint (get_reservations).
        It does NOT open its own DB session — it reuses the caller's ``db`` session.

        Returns a dict with keys:
            node_hours_reserved  – float or None
            node_hours_used      – float (0.0 if window invalid)
            utilization_pct      – float or None
            total_jobs           – int (all jobs matched by _resv_jobs_query)
            usage_series         – list[dict] (bin-level breakdown; empty when
                                   window is degenerate or nodes==0)

        Algorithm (canonical reference):
        - Full start→end window (NOT capped at now) for both reserved and used.
        - Jobs fetched via _resv_jobs_query (queue match + run-window overlap).
        - Window binned into sensible intervals (15-min … weekly).
        - Each job's overlap clipped to the reservation window and distributed
          across bins proportionally.
        - Each bin's used value is CAPPED at that bin's max (min(used, max_per_bin)).
        - utilization_pct = min(total_used, reserved) / reserved * 100
        """
        start_t = _naive(r.start_time)
        end_t = _naive(r.end_time)

        # Reservation-level derived numbers
        duration_seconds = r.duration_seconds
        if not duration_seconds and start_t and end_t and end_t > start_t:
            duration_seconds = int((end_t - start_t).total_seconds())
        nodes = r.nodes or 0
        node_hours_reserved = None
        if nodes and duration_seconds:
            node_hours_reserved = round(nodes * duration_seconds / 3600.0, 1)

        # Fetch jobs once; used for both binning and total_jobs count
        jobs_list = _resv_jobs_query(db, r)

        # ---- usage-over-time series (binning + per-bin cap) ----
        series: list = []
        total_used_node_hours = 0.0

        if start_t and end_t and end_t > start_t and nodes > 0:
            total_hours = (end_t - start_t).total_seconds() / 3600.0
            # Aim for ~24-48 bins; snap bin size to a sensible unit.
            if total_hours <= 6:
                bin_hours = 0.25          # 15 min
            elif total_hours <= 24:
                bin_hours = 1.0           # hourly
            elif total_hours <= 24 * 7:
                bin_hours = 6.0           # 6-hourly
            elif total_hours <= 24 * 30:
                bin_hours = 24.0          # daily
            else:
                bin_hours = 24.0 * 7      # weekly
            n_bins = max(1, int((total_hours + bin_hours - 1e-9) // bin_hours))
            # Cap to avoid pathological payloads
            if n_bins > 400:
                bin_hours = total_hours / 400.0
                n_bins = 400
            bin_seconds = bin_hours * 3600.0

            bin_edges = [start_t + timedelta(seconds=bin_seconds * i) for i in range(n_bins + 1)]
            bin_edges[-1] = end_t  # last edge clamps to reservation end
            used_per_bin = [0.0] * n_bins
            max_per_bin = []
            for i in range(n_bins):
                b0 = bin_edges[i]
                b1 = bin_edges[i + 1]
                bh = (b1 - b0).total_seconds() / 3600.0
                max_per_bin.append(nodes * bh)

            # Distribute each job's overlap-with-reservation across the bins
            for job in jobs_list:
                j_nodes = job.nodes or 0
                if j_nodes <= 0:
                    continue
                j_start = _naive(job.start_time)
                if j_start is None:
                    continue
                j_end = _naive(job.end_time) or end_t
                # Clip to reservation window
                ov_start = max(j_start, start_t)
                ov_end = min(j_end, end_t)
                if ov_end <= ov_start:
                    continue
                # Which bins does [ov_start, ov_end) touch?
                rel_start = (ov_start - start_t).total_seconds()
                rel_end = (ov_end - start_t).total_seconds()
                first_bin = max(0, int(rel_start // bin_seconds))
                last_bin = min(n_bins - 1, int((rel_end - 1e-9) // bin_seconds))
                for bi in range(first_bin, last_bin + 1):
                    bin_lo = bi * bin_seconds
                    bin_hi = min((bi + 1) * bin_seconds, rel_end if bi == last_bin else (bi + 1) * bin_seconds)
                    seg_lo = max(rel_start, bin_lo)
                    seg_hi = min(rel_end, (bi + 1) * bin_seconds)
                    if seg_hi > seg_lo:
                        used_per_bin[bi] += j_nodes * (seg_hi - seg_lo) / 3600.0

            for i in range(n_bins):
                used = round(used_per_bin[i], 2)
                # A job can't use more node-hours than the reservation offers in a bin
                capped = min(used, round(max_per_bin[i], 2))
                total_used_node_hours += capped
                series.append({
                    "bin_start": bin_edges[i].isoformat(),
                    "bin_end": bin_edges[i + 1].isoformat(),
                    "used_node_hours": capped,
                    "max_node_hours": round(max_per_bin[i], 2),
                })

        total_used_node_hours = round(total_used_node_hours, 1)
        utilization_pct = None
        if node_hours_reserved and node_hours_reserved > 0:
            utilization_pct = round(
                min(total_used_node_hours, node_hours_reserved) / node_hours_reserved * 100, 1
            )

        return {
            "node_hours_reserved": node_hours_reserved,
            "node_hours_used": total_used_node_hours,
            "utilization_pct": utilization_pct,
            "total_jobs": len(jobs_list),
            "usage_series": series,
            # Expose jobs_list for callers that need per-job data (e.g. state_counts)
            "_jobs_list": jobs_list,
        }

    @app.get("/api/reservation/{reservation_id}/summary")
    def api_reservation_summary(
        reservation_id: str,
        db: Session = Depends(get_db),
    ):
        r = db.query(Reservation).filter(
            Reservation.reservation_id == reservation_id
        ).first()
        if r is None:
            raise HTTPException(status_code=404, detail="Reservation not found")

        start_t = _naive(r.start_time)
        end_t = _naive(r.end_time)

        # Reservation-level derived numbers (needed for the response beyond utilization)
        duration_seconds = r.duration_seconds
        if not duration_seconds and start_t and end_t and end_t > start_t:
            duration_seconds = int((end_t - start_t).total_seconds())

        # State display normalisation (reuse the same mapping as the list endpoint)
        state_key = r.state.name if hasattr(r.state, "name") else str(r.state)

        # ---- utilization via shared canonical helper ----
        util = _compute_reservation_utilization(db, r)
        jobs_list = util["_jobs_list"]

        try:
            auth_users = _json.loads(r.authorized_users or "[]")
            auth_groups = _json.loads(r.authorized_groups or "[]")
        except Exception:
            auth_users, auth_groups = [], []
        auth_users = [u.split("@")[0] for u in auth_users]

        creation_t = _naive(r.creation_time)
        # State breakdown of jobs in the reservation
        state_counts: dict[str, int] = {}
        for j in jobs_list:
            st = j.state.name if j.state else "UNKNOWN"
            state_counts[st] = state_counts.get(st, 0) + 1

        return {
            "reservation_id": r.reservation_id,
            "reservation_name": r.reservation_name or "",
            "owner": r.owner or "",
            "state": state_key,
            "queue": r.queue or "",
            "nodes": r.nodes,
            "ncpus": r.ncpus,
            "ngpus": r.ngpus,
            "walltime": r.walltime,
            "duration_seconds": duration_seconds,
            "start_time": start_t.isoformat() if start_t else None,
            "end_time": end_t.isoformat() if end_t else None,
            "creation_time": creation_t.isoformat() if creation_t else None,
            "partition": r.partition,
            "server": r.server,
            "authorized_users": auth_users,
            "authorized_groups": auth_groups,
            "node_hours_reserved": util["node_hours_reserved"],
            "node_hours_used": util["node_hours_used"],
            "utilization_pct": util["utilization_pct"],
            "total_jobs": util["total_jobs"],
            "state_counts": state_counts,
            "usage_series": util["usage_series"],
        }

    @app.get("/api/reservation/{reservation_id}/jobs")
    def api_reservation_jobs(
        reservation_id: str,
        state: str = Query("ALL"),
        db: Session = Depends(get_db),
    ):
        r = db.query(Reservation).filter(
            Reservation.reservation_id == reservation_id
        ).first()
        if r is None:
            raise HTTPException(status_code=404, detail="Reservation not found")
        now = datetime.now(timezone.utc)
        jobs = _resv_jobs_query(db, r)
        if state and state.upper() != "ALL":
            state_map = {
                "RUNNING": JobState.RUNNING,
                "QUEUED": JobState.QUEUED,
                "FINISHED": JobState.FINISHED,
                "HELD": JobState.HELD,
                "UNKNOWN_END": JobState.UNKNOWN_END,
            }
            js = state_map.get(state.upper())
            if js:
                jobs = [j for j in jobs if j.state == js]
        return {"total": len(jobs), "jobs": [_serialize_job(j, now) for j in jobs]}

    # ---- reservations endpoint ----
    @app.get("/api/reservations")
    async def get_reservations(db: Session = Depends(get_db)):
        import json as _json
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=14)

        # Fetch reservations via ORM (dialect-safe; avoids raw SQL string functions
        # like strpos/substr which differ between SQLite and Postgres)
        reservations = db.query(Reservation).filter(
            or_(
                # currently active: started, not yet ended
                and_(Reservation.start_time <= now,
                     or_(Reservation.end_time == None, Reservation.end_time >= now)),
                # upcoming
                Reservation.start_time > now,
                # recently ended (within 14 days)
                Reservation.end_time >= cutoff,
            )
        ).order_by(Reservation.start_time.desc()).all()

        # Normalise state → a clean display label + CSS key
        _STATE_DISPLAY = {
            'RUNNING':          'RUNNING',
            'RUNNING_SHORT':    'RUNNING',
            'RN':               'RUNNING',
            'CONFIRMED':        'CONFIRMED',
            'CONFIRMED_SHORT':  'CONFIRMED',
            'CO':               'CONFIRMED',
            'DEGRADED':         'DEGRADED',
            'DG':               'DEGRADED',
            'FINISHED':         'COMPLETED',
            'RESV_RUNNING':     'RUNNING',
            'RESV_CONFIRMED':   'CONFIRMED',
            'RESV_FINISHED':    'COMPLETED',
            'RESV_DELETED':     'CANCELLED',
            'RESV_DEGRADED':    'DEGRADED',
            'EXPIRED':          'EXPIRED',
            'DELETED':          'CANCELLED',
            'BD':               'CANCELLED',
            'UN':               'UNKNOWN',
            'UNKNOWN':          'UNKNOWN',
        }

        result = []
        for r in reservations:
            state_key = r.state.name if hasattr(r.state, 'name') else str(r.state)
            display_state = _STATE_DISPLAY.get(state_key, state_key)

            # Compute utilization using the same canonical live binning+capping logic
            # as the detail endpoint — ensures table and detail page always agree.
            # (The previous reservation_utilization cache table approach capped at
            # "now" for running reservations and lacked per-bin capping, producing
            # different numbers than the detail page for the same reservation.)
            util = _compute_reservation_utilization(db, r)
            node_hours_reserved = util["node_hours_reserved"]
            node_hours_used     = util["node_hours_used"]
            utilization_pct     = util["utilization_pct"]
            jobs_submitted      = util["total_jobs"]

            try:
                auth_users  = _json.loads(r.authorized_users  or '[]')
                auth_groups = _json.loads(r.authorized_groups or '[]')
            except Exception:
                auth_users, auth_groups = [], []

            # Strip hostname suffixes from users
            auth_users = [u.split('@')[0] for u in auth_users]

            result.append({
                "reservation_id":      r.reservation_id,
                "reservation_name":    r.reservation_name or '',
                "owner":               r.owner or '',
                "state":               state_key,
                "display_state":       display_state,
                "nodes":               r.nodes,
                "ncpus":               r.ncpus,
                "ngpus":               r.ngpus,
                "walltime":            r.walltime,
                "node_hours_reserved": node_hours_reserved,
                "node_hours_used":     node_hours_used,
                "utilization_pct":     utilization_pct,
                "jobs_submitted":      jobs_submitted,
                "start_time":          r.start_time,
                "end_time":            r.end_time,
                "authorized_users":    auth_users,
                "authorized_groups":   auth_groups,
            })
        return {"reservations": result}

    # ---- analytics helpers ----

    from pbs_monitor.web.analytics_cache import make_cache
    _analytics_cache = make_cache(db_url, engine=engine)

    # total-nodes module-level cache (refreshed every 5 min)
    _tnc: dict[str, Any] = {"value": None, "ts": 0.0}

    def _get_total_nodes(db: Session) -> int:
        if _time.time() - _tnc["ts"] < 300 and _tnc["value"] is not None:
            return _tnc["value"]
        count = db.query(func.count(Node.name)).filter(Node.name.like('x%')).scalar() or 0
        _tnc["value"] = count
        _tnc["ts"] = _time.time()
        return count

    def _auto_freq(days: int) -> str:
        if days <= 7:  return 'h'
        if days < 90:  return 'd'
        return 'w'

    def _floor_bin(dt: datetime, freq: str) -> datetime:
        dt = dt.replace(tzinfo=None)  # strip tz for arithmetic
        if freq == 'h':
            return dt.replace(minute=0, second=0, microsecond=0)
        if freq == 'd':
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # week — floor to Monday
        return (dt - timedelta(days=dt.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    def _next_bin(t: datetime, freq: str) -> datetime:
        if freq == 'h': return t + timedelta(hours=1)
        if freq == 'd': return t + timedelta(days=1)
        return t + timedelta(weeks=1)

    def _bin_hours(freq: str) -> float:
        return {'h': 1.0, 'd': 24.0, 'w': 168.0}[freq]

    def _parse_walltime_hours(wt: str) -> float:
        """Parse PBS walltime string HH:MM:SS or DD:HH:MM:SS → float hours."""
        if not wt:
            return 0.0
        try:
            parts = [int(x) for x in str(wt).strip().split(':')]
            if len(parts) == 3:
                h, m, s = parts
                return h + m / 60 + s / 3600
            if len(parts) == 4:
                d, h, m, s = parts
                return d * 24 + h + m / 60 + s / 3600
        except Exception:
            pass
        return 0.0

    def _apply_job_filters(
        query,
        queue: List[str],
        queue_exclude: List[str],
        owner: List[str],
        owner_exclude: List[str],
        project: List[str],
        project_exclude: List[str],
        allocation_type: List[str],
        allocation_type_exclude: List[str],
    ):
        if queue:              query = query.filter(Job.queue.in_(queue))
        if queue_exclude:      query = query.filter(~Job.queue.in_(queue_exclude))
        if owner:              query = query.filter(Job.owner.in_(owner))
        if owner_exclude:      query = query.filter(~Job.owner.in_(owner_exclude))
        if project:            query = query.filter(Job.project.in_(project))
        if project_exclude:    query = query.filter(~Job.project.in_(project_exclude))
        if allocation_type:    query = query.filter(Job.allocation_type.in_(allocation_type))
        if allocation_type_exclude: query = query.filter(~Job.allocation_type.in_(allocation_type_exclude))
        return query

    # ---- analytics endpoints ----

    @app.get("/api/analytics/filters")
    async def api_analytics_filters(
        days: int = 30,
        db: Session = Depends(get_db),
    ):
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        def _fetch():
            base = db.query(Job).filter(
                or_(Job.start_time >= cutoff, Job.submit_time >= cutoff)
            )
            queues  = sorted({r.queue for r in base.with_entities(Job.queue).distinct() if r.queue})
            owners  = sorted({r.owner for r in base.with_entities(Job.owner).distinct() if r.owner})
            projs   = sorted({r.project for r in base.with_entities(Job.project).distinct() if r.project})
            allocs  = sorted({r.allocation_type for r in base.with_entities(Job.allocation_type).distinct() if r.allocation_type})
            return {"queues": queues, "owners": owners, "projects": projs, "allocation_types": allocs}

        return await asyncio.get_event_loop().run_in_executor(None, _fetch)

    @app.get("/api/analytics/wait-current")
    async def api_analytics_wait_current(
        db: Session = Depends(get_db),
        include_held: bool = False,
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """Return the current wait-time distribution as per-state binned counts.

        Bins are fixed 10-bucket ranges from <1hr to >1mo.
        NOTE: bin edges here MUST stay identical to WAIT_BINS in
        pbs_monitor/web/static/js/app.js — update both files together.

        Args:
            include_held: When True, count HELD jobs in addition to QUEUED jobs
                          and return their per-bin breakdown in ``held_counts``.
                          When False (default), count only QUEUED jobs and
                          return all-zero ``held_counts``.

        Returns a JSON object with:
            bins         : list[str]  — bin labels (length N)
            queued_counts: list[int]  — per-bin QUEUED job counts (length N)
            held_counts  : list[int]  — per-bin HELD job counts (length N);
                                        all zeros when include_held is False
            counts       : list[int]  — element-wise sum of queued_counts +
                                        held_counts (backward-compat field used
                                        by the frontend empty-check)
        """
        # KEEP IN SYNC with WAIT_BINS constant in app.js
        BINS = [
            ('<1hr',   0,    1),
            ('1-6hr',  1,    6),
            ('6-12hr', 6,   12),
            ('12-24hr',12,  24),
            ('1-2d',   24,  48),
            ('2-7d',   48, 168),
            ('7-14d', 168, 336),
            ('2-3wk', 336, 504),
            ('3-5wk', 504, 840),
            ('>1mo',  840, float('inf')),
        ]

        def _bin_index(wait_h: float) -> int:
            """Return the bin index for a wait time given in hours, or -1."""
            for i, (_, lo, hi) in enumerate(BINS):
                if lo <= wait_h < hi:
                    return i
            return -1

        def _fetch():
            now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive for arithmetic against DB timestamps

            queued_counts: List[int] = [0] * len(BINS)
            held_counts: List[int]   = [0] * len(BINS)

            # Always query QUEUED jobs.
            q_queued = db.query(Job).filter(
                Job.state == JobState.QUEUED,
                Job.submit_time.isnot(None),
            )
            q_queued = _apply_job_filters(
                q_queued, queue, queue_exclude, owner, owner_exclude,
                project, project_exclude, allocation_type, allocation_type_exclude,
            )
            for job in q_queued.all():
                st = job.submit_time
                if st is None:
                    continue
                if st.tzinfo is not None:
                    st = st.replace(tzinfo=None)
                idx = _bin_index((now - st).total_seconds() / 3600)
                if idx >= 0:
                    queued_counts[idx] += 1

            # Only query HELD jobs when explicitly requested.
            if include_held:
                q_held = db.query(Job).filter(
                    Job.state == JobState.HELD,
                    Job.submit_time.isnot(None),
                )
                q_held = _apply_job_filters(
                    q_held, queue, queue_exclude, owner, owner_exclude,
                    project, project_exclude, allocation_type, allocation_type_exclude,
                )
                for job in q_held.all():
                    st = job.submit_time
                    if st is None:
                        continue
                    if st.tzinfo is not None:
                        st = st.replace(tzinfo=None)
                    idx = _bin_index((now - st).total_seconds() / 3600)
                    if idx >= 0:
                        held_counts[idx] += 1

            counts = [q + h for q, h in zip(queued_counts, held_counts)]
            return {
                "bins": [b[0] for b in BINS],
                "queued_counts": queued_counts,
                "held_counts": held_counts,
                "counts": counts,  # backward-compat: element-wise sum for frontend empty-check
            }

        return await asyncio.get_event_loop().run_in_executor(None, _fetch)

    @app.get("/api/analytics/utilization")
    async def api_analytics_utilization(
        days: int = 30,
        freq: Optional[str] = None,
        group_by: str = 'queue',
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        if group_by not in ('queue', 'allocation_type'):
            group_by = 'queue'
        eff_freq = freq if freq in ('h', 'd', 'w') else _auto_freq(days)
        now = datetime.now(timezone.utc)
        window_start  = _floor_bin(now - timedelta(days=days), eff_freq)
        last_complete = _floor_bin(now, eff_freq)

        cache_key = _analytics_cache.make_key({
            "endpoint": "utilization",
            "freq": eff_freq,
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "group_by": group_by,
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        total_nodes = _get_total_nodes(db)

        def _compute():
            q = db.query(Job).filter(
                Job.end_time > window_start,
                Job.start_time < last_complete,
                Job.end_time.isnot(None),
                Job.start_time.isnot(None),
                Job.nodes > 0,
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            jobs = q.all()

            # Build bin list
            bins = []
            t = window_start
            while t < last_complete:
                bins.append(t)
                t = _next_bin(t, eff_freq)

            # group → bin_index → used_node_hours
            groups: dict[str, list[float]] = {}
            cap = total_nodes * _bin_hours(eff_freq)

            for job in jobs:
                grp = getattr(job, group_by, None) or 'unknown'
                if grp not in groups:
                    groups[grp] = [0.0] * len(bins)
                js = job.start_time
                je = job.end_time
                if js and js.tzinfo: js = js.replace(tzinfo=None)
                if je and je.tzinfo: je = je.replace(tzinfo=None)
                # Effective occupancy end = start + measured node-occupancy time
                # (occupied_seconds, from PBS resources_used.walltime). This
                # excludes HELD/QUEUED gaps that inflate the raw start..end span
                # for requeued/preempted jobs.
                #
                # occupied_seconds IS NULL means the job never occupied nodes
                # (never ran — no resources_used / exec record), so it must
                # contribute ZERO. Do NOT fall back to end_time here: that would
                # re-introduce the multi-week span of never-ran jobs and is the
                # exact bug this fix removes. (Deploy order guarantees the column
                # is backfilled before this code serves traffic.)
                occ = getattr(job, 'occupied_seconds', None)
                if occ is None or js is None:
                    continue
                occ_end = js + timedelta(seconds=occ)
                je_eff = occ_end if je is None else min(je, occ_end)
                n = job.nodes or 1
                for i, t in enumerate(bins):
                    nt = _next_bin(t, eff_freq)
                    seg_s = max(js, t)
                    seg_e = min(je_eff, nt)
                    hours = max(0.0, (seg_e - seg_s).total_seconds() / 3600)
                    if hours > 0:
                        groups[grp][i] += n * hours

            sorted_groups = sorted(groups.keys())
            bin_labels = [t.isoformat() for t in bins]
            series = {}
            for grp in sorted_groups:
                vals = groups[grp]
                series[grp] = [round(v / cap * 100, 2) if cap > 0 else 0.0 for v in vals]

            return {
                "freq": eff_freq,
                "group_by": group_by,
                "groups": sorted_groups,
                "bins": bin_labels,
                "series": series,
                "total_nodes": total_nodes,
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    @app.get("/api/analytics/queue-depth")
    async def api_analytics_queue_depth(
        days: int = 30,
        freq: Optional[str] = None,
        group_by: str = 'queue',
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        if group_by not in ('queue', 'allocation_type'):
            group_by = 'queue'
        eff_freq = freq if freq in ('h', 'd', 'w') else _auto_freq(days)
        now = datetime.now(timezone.utc)
        window_start  = _floor_bin(now - timedelta(days=days), eff_freq)
        last_complete = _floor_bin(now, eff_freq)

        cache_key = _analytics_cache.make_key({
            "endpoint": "queue-depth",
            "freq": eff_freq,
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "group_by": group_by,
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        total_nodes = _get_total_nodes(db)

        def _compute():
            # Include two classes of jobs:
            # 1. Jobs that started within the window (historical backlog)
            # 2. Jobs currently queued/held/waiting (still in queue now)
            # Exclude cancelled/finished jobs that never got a start_time
            # — those would be treated as queued from submission to now,
            # massively inflating the backlog.
            _active_states = (
                JobState.QUEUED, JobState.HELD, JobState.WAITING,
                JobState.TRANSITIONING,
            )
            q = db.query(Job).filter(
                Job.submit_time.isnot(None),
                Job.walltime.isnot(None),
                Job.nodes > 0,
                or_(
                    # Historical: jobs that actually started in the window
                    and_(Job.start_time.isnot(None),
                         Job.start_time >= window_start),
                    # Current: jobs still sitting in the queue
                    and_(Job.start_time.is_(None),
                         Job.state.in_(_active_states)),
                ),
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            jobs = q.all()

            bins = []
            t = window_start
            while t < last_complete:
                bins.append(t)
                t = _next_bin(t, eff_freq)

            groups: dict[str, list[float]] = {}

            now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive to match bins
            for job in jobs:
                grp = getattr(job, group_by, None) or 'unknown'
                if grp not in groups:
                    groups[grp] = [0.0] * len(bins)
                wt_h = _parse_walltime_hours(job.walltime)
                if wt_h <= 0:
                    continue
                n = job.nodes or 1
                nh = n * wt_h
                sub = job.submit_time
                sta = job.start_time
                if sub and sub.tzinfo: sub = sub.replace(tzinfo=None)
                if sta and sta.tzinfo: sta = sta.replace(tzinfo=None)
                # For jobs that haven't started yet, treat start as
                # "now" so they count as queued up to the present
                # (matches CLI usage-insights behaviour).
                effective_sta = sta if sta is not None else now
                for i, t in enumerate(bins):
                    nt = _next_bin(t, eff_freq)
                    # Job was queued during bin [t, nt) if:
                    #   - submitted before the bin ended, AND
                    #   - still queued at or after the bin start
                    #     (started at/after t, or hasn't started)
                    queued_during = (
                        sub < nt and
                        effective_sta >= t
                    )
                    if queued_during:
                        groups[grp][i] += nh

            # Normalize node-hours → system-hours (divide by total_nodes)
            denom = total_nodes if total_nodes > 0 else 1
            sorted_groups = sorted(groups.keys())
            bin_labels = [t.isoformat() for t in bins]
            series = {
                grp: [round(v / denom, 4) for v in groups[grp]]
                for grp in sorted_groups
            }

            return {
                "freq": eff_freq,
                "group_by": group_by,
                "groups": sorted_groups,
                "bins": bin_labels,
                "series": series,
                "total_nodes": total_nodes,
                "unit": "system-hours",
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    # NOTE: GET /api/analytics/wait-vs-score was removed in task S
    # (analytics-reorg-scaffold).  The scatter plot was cut per plan §5.8.
    # If you need to restore it, see git history on branch feature/analytics-reorg-scaffold.

    # ── Task A: Job Outcomes endpoints (plan §5.1, §5.2) ──────────────────────

    # Signal name table for exit codes 128+n (POSIX signals 1–63)
    _SIGNAL_NAMES: dict[int, str] = {
        1: 'SIGHUP',   2: 'SIGINT',   3: 'SIGQUIT',  4: 'SIGILL',
        5: 'SIGTRAP',  6: 'SIGABRT',  7: 'SIGBUS',   8: 'SIGFPE',
        9: 'SIGKILL', 10: 'SIGUSR1', 11: 'SIGSEGV', 12: 'SIGUSR2',
        13: 'SIGPIPE', 14: 'SIGALRM', 15: 'SIGTERM', 16: 'SIGURG',
        17: 'SIGCHLD', 18: 'SIGCONT', 19: 'SIGSTOP', 20: 'SIGTSTP',
        21: 'SIGTTIN', 22: 'SIGTTOU', 24: 'SIGXCPU', 25: 'SIGXFSZ',
        26: 'SIGVTALRM',27: 'SIGPROF', 28: 'SIGWINCH',29: 'SIGIO',
        30: 'SIGPWR',  31: 'SIGSYS',
    }

    def _exit_code_label(code: int) -> str:
        """Return a human-readable label for a raw PBS exit_status code."""
        if code == 0:
            return 'success'
        if code == 271:
            return 'requeued (PBS 271)'
        if code == -29:
            # Confirmed against Aurora data: -29 == job exceeded its walltime
            # and was killed (100% of -29 jobs ran >=95% of requested walltime).
            return 'walltime exceeded (PBS -29)'
        if code < 0:
            return f'PBS special ({code})'
        if 128 < code < 192:
            sig = code - 128
            sig_name = _SIGNAL_NAMES.get(sig, f'SIG{sig}')
            return f'killed by {sig_name} (exit {code})'
        return f'exit code {code}'

    @app.get("/api/analytics/job-outcomes")
    async def api_analytics_job_outcomes(
        days: int = 30,
        freq: Optional[str] = None,
        group_by: str = 'queue',
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """Stacked job-count (and rate) by outcome_class over time.

        Bins FINISHED jobs by end_time into the standard bin grid.
        Per bin, counts jobs per outcome_class (the T0-backfilled column).
        NULL outcome_class rows are counted as 'unknown'.

        Returns:
            freq: effective bin frequency
            bins: ISO-format bin start times
            classes: sorted list of outcome classes present
            series: {class_name: [count_per_bin, ...]}
            series_rate: {class_name: [pct_of_bin_total, ...]}  (0.0 when empty bin)
            totals: [total_jobs_per_bin, ...]
            total: grand total job count
        """
        eff_freq = freq if freq in ('h', 'd', 'w') else _auto_freq(days)
        now = datetime.now(timezone.utc)
        window_start  = _floor_bin(now - timedelta(days=days), eff_freq)
        last_complete = _floor_bin(now, eff_freq)

        cache_key = _analytics_cache.make_key({
            "endpoint": "job-outcomes",
            "freq": eff_freq,
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        def _compute():
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.end_time >= window_start,
                Job.end_time < last_complete,
                Job.end_time.isnot(None),
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            # Pull only the two columns needed — avoid loading full Job ORM
            # objects (incl. raw_pbs_data JSON) for hundreds of thousands of rows.
            rows = q.with_entities(Job.outcome_class, Job.end_time).all()

            # Build bin list
            bins: list[datetime] = []
            t = window_start
            while t < last_complete:
                bins.append(t)
                t = _next_bin(t, eff_freq)

            n_bins = len(bins)
            # Map bin start → index for O(1) lookup
            bin_index: dict[datetime, int] = {b: i for i, b in enumerate(bins)}

            # outcome_class → per-bin counts
            counts: dict[str, list[int]] = {}
            totals: list[int] = [0] * n_bins

            for cls_val, je in rows:
                cls = cls_val if cls_val else 'unknown'
                if je and je.tzinfo:
                    je = je.replace(tzinfo=None)
                if je is None:
                    continue
                # Find the bin this job falls into
                bin_start = _floor_bin(je, eff_freq)
                idx = bin_index.get(bin_start)
                if idx is None:
                    continue
                if cls not in counts:
                    counts[cls] = [0] * n_bins
                counts[cls][idx] += 1
                totals[idx] += 1

            sorted_classes = sorted(counts.keys())
            bin_labels = [b.isoformat() for b in bins]

            # Compute rate (% of bin total)
            series_rate: dict[str, list[float]] = {}
            for cls in sorted_classes:
                series_rate[cls] = [
                    round(counts[cls][i] / totals[i] * 100, 2) if totals[i] > 0 else 0.0
                    for i in range(n_bins)
                ]

            return {
                "freq": eff_freq,
                "bins": bin_labels,
                "classes": sorted_classes,
                "series": {cls: counts[cls] for cls in sorted_classes},
                "series_rate": series_rate,
                "totals": totals,
                "total": sum(totals),
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    @app.get("/api/analytics/exit-taxonomy")
    async def api_analytics_exit_taxonomy(
        days: int = 30,
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """Distribution of jobs per outcome_class and per raw exit_status code.

        No time binning — counts over the entire window.
        NULL outcome_class counted as 'unknown'.
        Returns top 20 exit codes + 'other' bucket.

        Returns:
            classes: {class_name: count}
            codes: [{code, count, label, outcome_class}, ...]  top-20 + other
            total: total finished jobs in window
        """
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(days=days)

        cache_key = _analytics_cache.make_key({
            "endpoint": "exit-taxonomy",
            "days": days,
            "window_start": window_start.isoformat(),
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        def _compute():
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.end_time >= window_start,
                Job.end_time.isnot(None),
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)

            # Fetch only the columns we need
            rows = q.with_entities(Job.outcome_class, Job.exit_status).all()

            class_counts: dict[str, int] = {}
            code_counts: dict[int | str, int] = {}

            for row in rows:
                cls = row.outcome_class if row.outcome_class else 'unknown'
                class_counts[cls] = class_counts.get(cls, 0) + 1

                code = row.exit_status
                if code is None:
                    code_key: int | str = 'null'
                else:
                    code_key = int(code)
                code_counts[code_key] = code_counts.get(code_key, 0) + 1

            # Sort codes by count desc, take top 20 + aggregate rest
            sorted_codes = sorted(code_counts.items(), key=lambda x: -x[1])
            top_codes = sorted_codes[:20]
            other_count = sum(v for _, v in sorted_codes[20:])

            codes_out = []
            for code, cnt in top_codes:
                if code == 'null':
                    label = 'no exit code (NULL)'
                    oc = 'unknown'
                else:
                    code_int = int(code)
                    label = _exit_code_label(code_int)
                    # Infer outcome_class for this code (must mirror
                    # analytics.outcome_classifier.classify_exit exactly).
                    if code_int == 0:
                        oc = 'success'
                    elif code_int == 271:
                        oc = 'requeued'
                    elif code_int == -29:
                        # -29 = walltime exceeded (checked before generic <0).
                        oc = 'walltime_killed'
                    elif code_int < 0:
                        oc = 'could_not_run'
                    elif 128 < code_int < 192:
                        oc = 'signal_killed'
                    else:
                        oc = 'error'
                codes_out.append({"code": code, "count": cnt, "label": label, "outcome_class": oc})

            if other_count > 0:
                codes_out.append({
                    "code": "other",
                    "count": other_count,
                    "label": f"other ({len(sorted_codes) - 20} codes)",
                    "outcome_class": "other",
                })

            return {
                "classes": dict(sorted(class_counts.items(), key=lambda x: -x[1])),
                "codes": codes_out,
                "total": sum(class_counts.values()),
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    # ---- Task B: Walltime Accuracy endpoints ----

    @app.get("/api/analytics/walltime-histogram")
    async def api_analytics_walltime_histogram(
        days: int = 30,
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """2D histogram: requested walltime (x, log buckets) vs used fraction (y buckets).

        Returns server-side-binned counts — never per-job rows (plan §4.3).
        Summary stats: median_used_fraction, pct_under_25, pct_over_95, n, excluded_unparseable.
        """
        now = datetime.now(timezone.utc)
        window_start  = now - timedelta(days=days)
        last_complete = _floor_bin(now, _auto_freq(days))

        cache_key = _analytics_cache.make_key({
            "endpoint": "walltime-histogram",
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        # X-axis: requested walltime buckets (log scale, seconds)
        x_edges_sec = [0, 15*60, 30*60, 60*60, 2*3600, 6*3600, 12*3600, 24*3600, float('inf')]
        x_labels = ["≤15m", "15-30m", "30-60m", "1-2h", "2-6h", "6-12h", "12-24h", ">24h"]

        # Y-axis: used-fraction buckets (%)
        y_edges_pct = [0, 10, 25, 50, 75, 95, 100, float('inf')]
        y_labels = ["0-10%", "10-25%", "25-50%", "50-75%", "75-95%", "95-100%", ">100%"]

        def _parse_wt(wt_str):
            """Parse HH:MM:SS walltime to seconds. Returns None on failure."""
            if not wt_str:
                return None
            try:
                parts = wt_str.strip().split(":")
                if len(parts) == 3:
                    h, m, s = parts
                    return int(h) * 3600 + int(m) * 60 + int(s)
            except (ValueError, AttributeError):
                pass
            return None

        def _compute():
            # Query FINISHED jobs with parseable walltime and actual runtime
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.end_time >= window_start,
                Job.end_time < last_complete,
                Job.walltime.isnot(None),
                Job.actual_runtime_seconds.isnot(None),
                Job.actual_runtime_seconds > 0,
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            # Only pull the two columns we need — loading full Job ORM objects
            # (incl. the multi-KB raw_pbs_data JSON) for ~450k rows blows past
            # the request timeout.  with_entities keeps this a lean 2-column scan.
            rows = q.with_entities(Job.walltime, Job.actual_runtime_seconds).all()

            nx = len(x_labels)
            ny = len(y_labels)
            # 2D grid: cells[xi][yi] = count
            grid = [[0] * ny for _ in range(nx)]

            fractions = []
            excluded_unparseable = 0

            for wt_val, actual in rows:
                req_sec = _parse_wt(wt_val)
                if req_sec is None or req_sec <= 0:
                    excluded_unparseable += 1
                    continue
                frac = actual / req_sec  # may exceed 1.0
                frac_pct = frac * 100.0
                fractions.append(frac)

                # Bin x: requested walltime
                xi = 0
                for i in range(len(x_edges_sec) - 1):
                    if x_edges_sec[i] < req_sec <= x_edges_sec[i + 1]:
                        xi = i
                        break
                else:
                    xi = nx - 1  # fallback >24h

                # Bin y: used fraction %
                yi = 0
                for j in range(len(y_edges_pct) - 1):
                    if y_edges_pct[j] <= frac_pct < y_edges_pct[j + 1]:
                        yi = j
                        break
                else:
                    yi = ny - 1  # >100%

                grid[xi][yi] += 1

            n = len(fractions)
            if n > 0:
                fractions.sort()
                mid = n // 2
                median_frac = fractions[mid] if n % 2 else (fractions[mid - 1] + fractions[mid]) / 2
                pct_under_25 = sum(1 for f in fractions if f < 0.25) / n
                pct_over_95  = sum(1 for f in fractions if f >= 0.95) / n
            else:
                median_frac = 0.0
                pct_under_25 = 0.0
                pct_over_95  = 0.0

            # Flatten grid to sparse cells (skip zero-count cells)
            cells = []
            for xi in range(nx):
                for yi in range(ny):
                    c = grid[xi][yi]
                    if c > 0:
                        cells.append({"x": xi, "y": yi, "count": c})

            return {
                "x_labels": x_labels,
                "y_labels": y_labels,
                "cells": cells,
                "median_used_fraction": round(median_frac, 4),
                "pct_under_25": round(pct_under_25, 4),
                "pct_over_95": round(pct_over_95, 4),
                "n": n,
                "excluded_unparseable": excluded_unparseable,
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    @app.get("/api/analytics/walltime-efficiency")
    async def api_analytics_walltime_efficiency(
        days: int = 30,
        group_by: str = "user",
        top_n: int = 20,
        min_jobs: int = 3,
        db: Session = Depends(get_db),
    ):
        """Efficiency scorecard: top/bottom N by mean efficiency, grouped by user or project.

        Implements WalltimeEfficiencyAnalyzer logic directly against the shared db session.
        Returns ranked table rows (plan §5.3, reuse WalltimeEfficiencyAnalyzer patterns).

        DUPLICATED-LOGIC WARNING — the rerun-aware efficiency policy here
        (numerator = occupied_seconds w/ actual_runtime_seconds fallback;
        denominator = requested_walltime * run_count, NULL run_count -> 1;
        exclude never-ran; cap at 100%) is ALSO implemented in:
          - analytics/walltime_efficiency.py :: _compute_job_efficiency (CLI)
          - web/server.py :: api_leaderboard (leaderboard efficiency ranking)
        Kept separate because input shapes differ (this uses with_entities
        tuples). If you change the efficiency definition, change all three or
        the views will silently disagree.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=days)

        cache_key = _analytics_cache.make_key({
            "endpoint": "walltime-efficiency",
            "days": days,
            "group_by": group_by,
            "top_n": top_n,
            "min_jobs": min_jobs,
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        def _parse_wt_sec(wt_str):
            """Parse HH:MM:SS or DD:HH:MM:SS to seconds, return 0 on failure."""
            if not wt_str:
                return 0
            try:
                parts = wt_str.strip().split(":")
                if len(parts) == 3:
                    h, m, s = parts
                    return int(h) * 3600 + int(m) * 60 + int(s)
                elif len(parts) == 4:
                    d, h, m, s = parts
                    return int(d) * 86400 + int(h) * 3600 + int(m) * 60 + int(s)
            except (ValueError, AttributeError):
                pass
            return 0

        def _compute():
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.end_time >= cutoff,
                Job.walltime.isnot(None),
            )
            group_col_attr = Job.owner if group_by != "project" else Job.project
            q = q.with_entities(
                Job.walltime, Job.actual_runtime_seconds, Job.occupied_seconds,
                Job.run_count, group_col_attr
            )
            rows_raw = q.all()

            # Aggregate by group
            stats: dict[str, dict] = {}
            for wt_val, actual_val, occ_val, run_count_val, grp_val in rows_raw:
                req_sec = _parse_wt_sec(wt_val)
                if req_sec <= 0:
                    continue
                # Rerun-safe node-occupancy: prefer occupied_seconds (excludes
                # requeue gaps), fall back to actual_runtime_seconds for rows not
                # yet backfilled. A job that never occupied nodes (both absent/0)
                # is EXCLUDED -- it has no meaningful run/request efficiency and
                # must not be laundered into 100% by the cap.
                occ = occ_val if (occ_val is not None and occ_val > 0) else (actual_val or 0)
                if occ <= 0:
                    continue
                # Rerun-aware denominator: walltime * run_count (NULL -> 1).
                run_count = run_count_val or 1
                if run_count < 1:
                    run_count = 1
                eff = min((occ / (req_sec * run_count)) * 100.0, 100.0)
                name = grp_val or "unknown"
                if name not in stats:
                    stats[name] = {"effs": [], "jobs": 0}
                stats[name]["effs"].append(eff)
                stats[name]["jobs"] += 1

            # Build ranked rows
            rows = []
            for name, s in stats.items():
                if s["jobs"] < min_jobs:
                    continue
                effs = s["effs"]
                n = len(effs)
                mean_e = sum(effs) / n
                min_e  = min(effs)
                max_e  = max(effs)
                if n > 1:
                    variance = sum((e - mean_e) ** 2 for e in effs) / (n - 1)
                    std_e = variance ** 0.5
                else:
                    std_e = 0.0
                rows.append({
                    "name": name,
                    "jobs": n,
                    "mean_efficiency": f"{mean_e:.1f}%",
                    "std_dev": f"{std_e:.1f}%",
                    "min_efficiency": f"{min_e:.1f}%",
                    "max_efficiency": f"{max_e:.1f}%",
                    "_eff_float": mean_e,
                })

            rows.sort(key=lambda r: r["_eff_float"], reverse=True)
            # Remove internal sort key before returning
            for r in rows:
                r.pop("_eff_float", None)

            n_total = len(rows)
            if len(rows) > top_n * 2:
                rows = rows[:top_n] + rows[-top_n:]
            elif len(rows) > top_n:
                rows = rows[:top_n]

            return {"group_by": group_by, "rows": rows, "n_total": n_total}

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    # ---- static files ----
    # Serve index.html at root, everything else from /static
    @app.get("/")
    async def serve_index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/analytics")
    async def serve_analytics():
        return FileResponse(STATIC_DIR / "analytics.html")

    @app.get("/leaderboard")
    async def serve_leaderboard():
        return FileResponse(STATIC_DIR / "leaderboard.html")

    @app.get("/api/leaderboard")
    async def api_leaderboard(
        window: int = Query(default=7, description="Time window in days (1, 7, or 30)"),
        group_by: str = Query(default="user", description="Group by 'user' or 'project'"),
        db: Session = Depends(get_db),
    ):
        """Leaderboard: top/bottom 10 by node-hours and efficiency.

        Node-hours: RUNNING (elapsed so far) + FINISHED jobs where occupancy > 30min.
        Efficiency: FINISHED jobs only, occupancy > 30min. Rerun-aware and weighted
        by node-hours:
            efficiency = sum(occupied * nodes) / sum(walltime_seconds * run_count * nodes)
        where `occupied` is PBS-measured node-occupancy time (occupied_seconds),
        which excludes HELD/QUEUED gaps for requeued/preempted jobs, and the
        denominator is multiplied by run_count so a job requeued N times is
        measured against the N reservations it consumed. This prevents a job PBS
        re-ran many times from reading far above 100%.

        DUPLICATED-LOGIC WARNING — the rerun-aware efficiency policy here
        (numerator = occupied_seconds w/ actual_runtime_seconds fallback;
        denominator = requested_walltime * run_count, NULL run_count -> 1;
        exclude never-ran; cap at 100%) is ALSO implemented in:
          - analytics/walltime_efficiency.py :: _compute_job_efficiency (CLI)
          - web/server.py :: api_analytics_walltime_efficiency (scorecard tab)
        Kept separate because this one is a node-hour-WEIGHTED sum, not a
        per-job mean. If you change the efficiency definition, change all
        three or the views will silently disagree.
        """
        MIN_RUNTIME_SEC = 1800  # 30 minutes
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=window)

        group_col = Job.owner if group_by == "user" else Job.project

        def _occupancy_seconds(job) -> int:
            """PBS-measured node-occupancy seconds, rerun-safe.

            Prefer occupied_seconds (resources_used.walltime, excludes requeue
            gaps). Fall back to actual_runtime_seconds (elapsed start..end span)
            only when occupied_seconds is unpopulated (pre-v1.3 rows / not yet
            backfilled). Never-ran jobs have neither -> 0.
            """
            occ = getattr(job, "occupied_seconds", None)
            if occ is not None and occ > 0:
                return occ
            return job.actual_runtime_seconds or 0

        def _fetch():
            # ── node-hours: RUNNING (elapsed) + FINISHED (occupancy > 30min) ──
            nh_map: dict[str, float] = {}

            # Running jobs – elapsed node-hours so far
            running = (
                db.query(Job)
                .filter(
                    Job.state == JobState.RUNNING,
                    Job.start_time.isnot(None),
                    Job.start_time >= cutoff,
                )
                .all()
            )
            for job in running:
                st = job.start_time
                if st.tzinfo is None:
                    st = st.replace(tzinfo=timezone.utc)
                elapsed = (now - st).total_seconds()
                if elapsed < MIN_RUNTIME_SEC:
                    continue
                key = (job.owner if group_by == "user" else job.project) or "unknown"
                nodes = job.nodes or 1
                nh_map[key] = nh_map.get(key, 0.0) + elapsed * nodes / 3600.0

            # Finished jobs – measured node-occupancy, occupancy > 30min.
            # Filter on end_time in window; the occupancy threshold is applied in
            # Python against occupied_seconds (with actual_runtime fallback) so a
            # requeued job's inflated start..end span no longer sneaks past a
            # SQL actual_runtime_seconds filter.
            finished = (
                db.query(Job)
                .filter(
                    Job.state == JobState.FINISHED,
                    Job.end_time >= cutoff,
                )
                .all()
            )

            # ── efficiency: finished jobs only ──
            # Rerun-aware weighted efficiency:
            #   sum(occupied * nodes) / sum(walltime * run_count * nodes)
            eff_actual: dict[str, float] = {}     # sum of occupied * nodes
            eff_requested: dict[str, float] = {}  # sum of walltime * run_count * nodes

            for job in finished:
                occ = _occupancy_seconds(job)
                if occ <= MIN_RUNTIME_SEC:
                    # Never ran, or ran < 30 min: excluded from both node-hours
                    # and efficiency (a never-ran requeued job occupied nothing).
                    continue
                key = (job.owner if group_by == "user" else job.project) or "unknown"
                nodes = job.nodes or 1
                nh_map[key] = nh_map.get(key, 0.0) + occ * nodes / 3600.0

                # Efficiency denominator: walltime_seconds * run_count. run_count
                # may be NULL on rows not yet backfilled -> treat as 1 so missing
                # data never inflates the denominator.
                wall = _parse_walltime(job.walltime)
                if wall and wall > 0:
                    run_count = getattr(job, "run_count", None) or 1
                    if run_count < 1:
                        run_count = 1
                    eff_actual[key]    = eff_actual.get(key, 0.0)    + occ * nodes
                    eff_requested[key] = eff_requested.get(key, 0.0) + wall * run_count * nodes

            # Build unified records
            all_keys = set(nh_map) | set(eff_actual)
            records = []
            for key in all_keys:
                nh = nh_map.get(key, 0.0)
                req = eff_requested.get(key, 0.0)
                act = eff_actual.get(key, 0.0)
                # Cap at 100%: with the rerun-normalized denominator this only
                # trims genuine small overruns (a job that ran a hair past its
                # single wall), not the multi-attempt inflation.
                eff = round(min(act / req * 100, 100.0), 1) if req > 0 else None
                records.append({
                    "name": key,
                    "node_hours": round(nh, 1),
                    "efficiency": eff,  # percent, or null if no finished jobs
                })

            # Sort helpers
            by_nh   = sorted(records, key=lambda r: r["node_hours"],            reverse=True)
            eff_only = [r for r in records if r["efficiency"] is not None]
            by_eff  = sorted(eff_only,  key=lambda r: r["efficiency"],           reverse=True)

            return {
                "window_days": window,
                "group_by": group_by,
                "node_hours": {
                    "top":    by_nh[:10],
                    "bottom": by_nh[-10:][::-1] if len(by_nh) > 10 else [],
                },
                "efficiency": {
                    "top":    by_eff[:10],
                    "bottom": by_eff[-10:][::-1] if len(by_eff) > 10 else [],
                },
            }

        return await asyncio.get_event_loop().run_in_executor(None, _fetch)

    # ── Task E: Collector Health endpoint ───────────────────────────────────
    @app.get("/api/analytics/collector-health")
    async def api_analytics_collector_health(
        days: int = 30,
        summary: int = 0,
        db: Session = Depends(get_db),
    ):
        """Collection cadence, gap detection, and failure reporting.

        ?summary=1  → cheap banner payload: {gap_count, max_gap_min,
                       last_success_age_min, failed_count}
        full mode   → {cadence:[{t, gap_min}], gaps:[...],
                       failures:[{timestamp, collection_type, error_message}],
                       median_gap_min}

        Gap detection uses system_snapshots timestamps (one row per successful
        collection cycle).  A gap is flagged when it exceeds *both* 60 min AND
        2× the median inter-snapshot interval, so brief scheduled pauses don't
        generate noise.
        """

        def _compute():
            from statistics import median as _median

            now_utc = datetime.now(timezone.utc)
            window_start_naive = (now_utc - timedelta(days=days)).replace(tzinfo=None)
            now_naive = now_utc.replace(tzinfo=None)

            # ── 1. Snapshot cadence (system_snapshots timestamps in window) ──
            rows = db.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT timestamp FROM system_snapshots "
                    "WHERE timestamp >= :ws AND timestamp <= :we "
                    "ORDER BY timestamp"
                ),
                {"ws": window_start_naive, "we": now_naive},
            ).fetchall()

            timestamps_raw = [r[0] for r in rows]

            # Parse timestamps. SQLite returns naive datetimes / strings; Postgres
            # returns tz-aware datetimes. Normalize everything to naive UTC so the
            # arithmetic below never mixes offset-aware and offset-naive values.
            def _parse(ts):
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                if ts is not None and ts.tzinfo is not None:
                    ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
                return ts

            timestamps = [_parse(t) for t in timestamps_raw]

            # ── 2. Compute inter-snapshot gaps ───────────────────────────────
            cadence = []  # [{t (ISO str), gap_min}]
            gap_minutes = []  # raw gap values

            for i in range(1, len(timestamps)):
                gap_min = (timestamps[i] - timestamps[i - 1]).total_seconds() / 60.0
                gap_minutes.append(gap_min)
                cadence.append({
                    "t": timestamps[i].isoformat(),
                    "gap_min": round(gap_min, 2),
                })

            median_gap_min = round(_median(gap_minutes), 2) if gap_minutes else None

            # ── 3. Flag significant gaps ──────────────────────────────────────
            # A gap is "significant" if it is both > 60 min AND > 2× median.
            threshold = max(60.0, 2.0 * median_gap_min) if median_gap_min else 60.0
            gaps = [entry for entry in cadence if entry["gap_min"] > threshold]

            # ── 4. Failed collections in window ──────────────────────────────
            fail_rows = db.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT timestamp, collection_type, error_message "
                    "FROM data_collection_log "
                    "WHERE UPPER(CAST(status AS VARCHAR)) != 'SUCCESS' "
                    "  AND timestamp >= :ws "
                    "ORDER BY timestamp DESC"
                ),
                {"ws": window_start_naive},
            ).fetchall()

            failures = [
                {
                    "timestamp": str(r[0]),
                    "collection_type": r[1],
                    "error_message": r[2],
                }
                for r in fail_rows
            ]

            # ── 5. Last successful collection age ─────────────────────────────
            last_ok_row = db.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT timestamp FROM data_collection_log "
                    "WHERE UPPER(CAST(status AS VARCHAR)) = 'SUCCESS' "
                    "ORDER BY timestamp DESC LIMIT 1"
                )
            ).fetchone()

            last_success_age_min = None
            if last_ok_row:
                last_ok_ts = _parse(last_ok_row[0])
                last_success_age_min = round(
                    (now_naive - last_ok_ts).total_seconds() / 60.0, 1
                )

            # ── 6. Build response ─────────────────────────────────────────────
            gap_count  = len(gaps)
            max_gap_min = round(max(g["gap_min"] for g in gaps), 1) if gaps else 0

            if summary:
                return {
                    "gap_count":           gap_count,
                    "max_gap_min":         max_gap_min,
                    "last_success_age_min": last_success_age_min,
                    "failed_count":        len(failures),
                }

            return {
                "cadence":              cadence,
                "gaps":                 gaps,
                "failures":             failures,
                "median_gap_min":       median_gap_min,
                "gap_count":            gap_count,
                "max_gap_min":          max_gap_min,
                "last_success_age_min": last_success_age_min,
                "failed_count":         len(failures),
            }

        # Summary mode is cheap (no heavy aggregation) — skip cache, run inline.
        # Full mode runs inside executor to avoid blocking the event loop.
        if summary:
            return _compute()
        return await asyncio.get_event_loop().run_in_executor(None, _compute)

    # ---- Task C: Wait Times endpoints ----

    @app.get("/api/analytics/wait-ecdf")
    async def api_analytics_wait_ecdf(
        days: int = 30,
        group_by: str = 'queue',
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """ECDF of queue_time_seconds per group for FINISHED jobs in window.

        Server-side downsampled to ≤200 points per group (quantile sampling).
        Returns {groups, curves:{group:[[wait_hours, cum_fraction], ...]}}
        Never returns raw per-job arrays (plan §4.3).
        """
        if group_by not in ('queue', 'allocation_type', 'project'):
            group_by = 'queue'

        now = datetime.now(timezone.utc)
        window_start  = _floor_bin(now - timedelta(days=days), _auto_freq(days))
        last_complete = _floor_bin(now, _auto_freq(days))

        cache_key = _analytics_cache.make_key({
            "endpoint": "wait-ecdf",
            "days": days,
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "group_by": group_by,
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        DOWNSAMPLE_POINTS = 200  # max points per group curve

        def _compute():
            # Select only the two columns needed — avoid loading full ORM objects
            group_col = getattr(Job, group_by)
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.start_time >= window_start,
                Job.start_time < last_complete,
                Job.start_time.isnot(None),
                Job.queue_time_seconds.isnot(None),
                Job.queue_time_seconds >= 0,
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            rows = q.with_entities(Job.queue_time_seconds, group_col).all()

            # Group wait times by group value
            from collections import defaultdict
            group_waits: dict = defaultdict(list)
            for wait_sec, grp_val in rows:
                grp = grp_val or 'unknown'
                group_waits[grp].append(float(wait_sec))

            groups_sorted = sorted(group_waits.keys())

            # Build ECDF per group, downsampled to DOWNSAMPLE_POINTS
            curves: dict = {}
            for grp in groups_sorted:
                waits = sorted(group_waits[grp])
                n = len(waits)
                if n == 0:
                    continue
                # Sample quantile indices for up to DOWNSAMPLE_POINTS points
                if n <= DOWNSAMPLE_POINTS:
                    indices = list(range(n))
                else:
                    step = n / DOWNSAMPLE_POINTS
                    indices = [int(i * step) for i in range(DOWNSAMPLE_POINTS)]
                    if indices[-1] != n - 1:
                        indices.append(n - 1)

                curve = []
                for idx in indices:
                    wait_hours = round(waits[idx] / 3600.0, 4)
                    cum_fraction = round((idx + 1) / n, 4)
                    curve.append([wait_hours, cum_fraction])
                curves[grp] = curve

            return {
                "group_by": group_by,
                "groups": groups_sorted,
                "curves": curves,
                "n_jobs": sum(len(v) for v in group_waits.values()),
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    @app.get("/api/analytics/wait-percentiles")
    async def api_analytics_wait_percentiles(
        days: int = 30,
        freq: Optional[str] = None,
        group_by: str = 'queue',
        db: Session = Depends(get_db),
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
        """Per-bin p50/p90/p99 of queue_time_seconds for jobs that STARTED in that bin.

        Returns {freq, bins, groups, series:{group:{p50:[...],p90:[...],p99:[...]}}}
        Used for the 'queue degrading' early-warning view (plan §5.5).
        """
        if group_by not in ('queue', 'allocation_type', 'project'):
            group_by = 'queue'

        eff_freq = freq if freq in ('h', 'd', 'w') else _auto_freq(days)
        now = datetime.now(timezone.utc)
        window_start  = _floor_bin(now - timedelta(days=days), eff_freq)
        last_complete = _floor_bin(now, eff_freq)

        cache_key = _analytics_cache.make_key({
            "endpoint": "wait-percentiles",
            "freq": eff_freq,
            "window_start": window_start.isoformat(),
            "last_complete": last_complete.isoformat(),
            "group_by": group_by,
            "queue": sorted(queue), "queue_exclude": sorted(queue_exclude),
            "owner": sorted(owner), "owner_exclude": sorted(owner_exclude),
            "project": sorted(project), "project_exclude": sorted(project_exclude),
            "allocation_type": sorted(allocation_type),
            "allocation_type_exclude": sorted(allocation_type_exclude),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        def _compute():
            group_col = getattr(Job, group_by)
            q = db.query(Job).filter(
                Job.state == JobState.FINISHED,
                Job.start_time >= window_start,
                Job.start_time < last_complete,
                Job.start_time.isnot(None),
                Job.queue_time_seconds.isnot(None),
                Job.queue_time_seconds >= 0,
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            rows = q.with_entities(Job.queue_time_seconds, Job.start_time, group_col).all()

            # Build bin list
            bins: list = []
            t = window_start
            while t < last_complete:
                bins.append(t)
                t = _next_bin(t, eff_freq)
            n_bins = len(bins)
            bin_index: dict = {b: i for i, b in enumerate(bins)}

            # Collect wait_sec lists per (group, bin_index)
            from collections import defaultdict
            bucket: dict = defaultdict(lambda: defaultdict(list))

            for wait_sec, js, grp_val in rows:
                grp = grp_val or 'unknown'
                if js and js.tzinfo:
                    js = js.replace(tzinfo=None)
                if js is None:
                    continue
                # Find bin for start_time
                bi = None
                for i in range(n_bins - 1, -1, -1):
                    if js >= bins[i]:
                        bi = i
                        break
                if bi is None:
                    continue
                bucket[grp][bi].append(float(wait_sec))

            groups_sorted = sorted(bucket.keys())
            bin_labels = [b.isoformat() for b in bins]

            def _percentile(vals: list, p: float) -> float:
                """Return the p-th percentile (0–100) of sorted list."""
                if not vals:
                    return 0.0
                sv = sorted(vals)
                n = len(sv)
                idx = (p / 100) * (n - 1)
                lo = int(idx)
                hi = min(lo + 1, n - 1)
                frac = idx - lo
                return round(sv[lo] * (1 - frac) + sv[hi] * frac, 1)

            series: dict = {}
            for grp in groups_sorted:
                p50 = []
                p90 = []
                p99 = []
                for bi in range(n_bins):
                    vals = bucket[grp].get(bi, [])
                    p50.append(_percentile(vals, 50))
                    p90.append(_percentile(vals, 90))
                    p99.append(_percentile(vals, 99))
                series[grp] = {"p50": p50, "p90": p90, "p99": p99}

            return {
                "freq": eff_freq,
                "group_by": group_by,
                "bins": bin_labels,
                "groups": groups_sorted,
                "series": series,
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    # ---- Task D: Reservations endpoint ----

    @app.get("/api/analytics/reservation-utilization-timeline")
    async def api_analytics_reservation_utilization_timeline(
        days: int = 365,
        top_n: int = 20,
        db: Session = Depends(get_db),
    ):
        """Per-reservation reserved-vs-used node-hours + wasted-hours ranking.

        Reuses Reservation ORM directly against the injected db session
        (ReservationUtilizationAnalyzer opens its own sessions via Config
        and cannot share the FastAPI-injected db — see pitfalls).

        Returns compact payload with top_n reservations by wasted_node_hours
        plus summary stats. Cache by (days, top_n).
        """
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(days=days)

        cache_key = _analytics_cache.make_key({
            "endpoint": "reservation-utilization-timeline",
            "days": days,
            "top_n": top_n,
            "window_start": window_start.isoformat(),
        })
        cached = _analytics_cache.get(cache_key)
        if cached:
            return cached

        def _compute():
            # Fetch reservations that overlap the window
            # Strip tz from window_start for naive-datetime DBs (SQLite)
            ws_naive = window_start.replace(tzinfo=None)
            reservations = db.query(Reservation).filter(
                Reservation.nodes.isnot(None),
                Reservation.nodes > 0,
                Reservation.start_time.isnot(None),
                Reservation.end_time.isnot(None),
                Reservation.end_time >= ws_naive,
            ).with_entities(
                Reservation.reservation_id,
                Reservation.reservation_name,
                Reservation.owner,
                Reservation.nodes,
                Reservation.start_time,
                Reservation.end_time,
                Reservation.queue,
            ).all()

            results = []
            for res_id, res_name, owner, nodes, start_t, end_t, queue_name in reservations:
                # Normalise timestamps (SQLite stores naive, Postgres stores tz-aware)
                if start_t and hasattr(start_t, 'tzinfo') and start_t.tzinfo:
                    start_t = start_t.replace(tzinfo=None)
                if end_t and hasattr(end_t, 'tzinfo') and end_t.tzinfo:
                    end_t = end_t.replace(tzinfo=None)

                if not start_t or not end_t or end_t <= start_t:
                    continue

                duration_hours = (end_t - start_t).total_seconds() / 3600.0
                reserved_node_hours = round(nodes * duration_hours, 2)

                # Find jobs that ran in this reservation's queue during its window
                job_rows = db.query(Job).filter(
                    Job.queue == queue_name,
                    Job.start_time.isnot(None),
                    Job.start_time <= end_t,
                    Job.nodes.isnot(None),
                    Job.nodes > 0,
                ).with_entities(
                    Job.nodes, Job.start_time, Job.end_time
                ).all()

                used_node_hours = 0.0
                for j_nodes, j_start, j_end in job_rows:
                    if j_start is None:
                        continue
                    if j_start and hasattr(j_start, 'tzinfo') and j_start.tzinfo:
                        j_start = j_start.replace(tzinfo=None)
                    if j_end and hasattr(j_end, 'tzinfo') and j_end.tzinfo:
                        j_end = j_end.replace(tzinfo=None)
                    real_end = j_end if j_end else end_t
                    ov_start = max(j_start, start_t)
                    ov_end = min(real_end, end_t)
                    if ov_end > ov_start:
                        overlap_h = (ov_end - ov_start).total_seconds() / 3600.0
                        used_node_hours += j_nodes * overlap_h

                used_node_hours = round(used_node_hours, 2)
                # Cap at reserved (jobs can't use more than reserved)
                used_node_hours = min(used_node_hours, reserved_node_hours)
                wasted_node_hours = round(reserved_node_hours - used_node_hours, 2)
                utilization_pct = round(
                    (used_node_hours / reserved_node_hours * 100) if reserved_node_hours > 0 else 0.0, 1
                )

                results.append({
                    "reservation_id": res_id,
                    "name": res_name or res_id,
                    "owner": owner or "unknown",
                    "nodes": nodes,
                    "reserved_node_hours": reserved_node_hours,
                    "used_node_hours": used_node_hours,
                    "wasted_node_hours": wasted_node_hours,
                    "utilization_pct": utilization_pct,
                    "start": start_t.isoformat() if start_t else None,
                    "end": end_t.isoformat() if end_t else None,
                })

            # Sort by wasted_node_hours descending for the ranking
            results.sort(key=lambda r: r["wasted_node_hours"], reverse=True)

            total = len(results)
            total_reserved = round(sum(r["reserved_node_hours"] for r in results), 2)
            total_used = round(sum(r["used_node_hours"] for r in results), 2)
            total_wasted = round(total_reserved - total_used, 2)
            avg_util = round(
                sum(r["utilization_pct"] for r in results) / total if total > 0 else 0.0, 1
            )
            under_25_count = sum(1 for r in results if r["utilization_pct"] < 25)

            # Return top_n + summary; discard tail to keep payload small
            top_results = results[:top_n]

            return {
                "days": days,
                "total_reservations": total,
                "total_reserved_node_hours": total_reserved,
                "total_used_node_hours": total_used,
                "total_wasted_node_hours": total_wasted,
                "avg_utilization_pct": avg_util,
                "under_25_pct_count": under_25_count,
                "reservations": top_results,
            }

        result = await asyncio.get_event_loop().run_in_executor(None, _compute)
        _analytics_cache.set(cache_key, result)
        return result

    app.mount("/css", StaticFiles(directory=str(STATIC_DIR / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(STATIC_DIR / "js")), name="js")

    # ---- cache pre-warm on startup ----
    @app.on_event("startup")
    async def _prewarm_cache() -> None:
        """Fire common analytics queries in the background at startup so the
        first user page-load hits the cache instead of waiting 30-60s."""
        import logging
        log = logging.getLogger(__name__)

        async def _warm(days_val: int, freq_val: str, group: str) -> None:
            now = datetime.now(timezone.utc)
            eff_freq = freq_val
            window_start  = _floor_bin(now - timedelta(days=days_val), eff_freq)
            last_complete = _floor_bin(now, eff_freq)

            for endpoint in ("utilization", "queue-depth"):
                key = _analytics_cache.make_key({
                    "endpoint": endpoint,
                    "freq": eff_freq,
                    "window_start": window_start.isoformat(),
                    "last_complete": last_complete.isoformat(),
                    "group_by": group,
                    "queue": [], "queue_exclude": [],
                    "owner": [], "owner_exclude": [],
                    "project": [], "project_exclude": [],
                    "allocation_type": [], "allocation_type_exclude": [],
                })
                if _analytics_cache.get(key):
                    log.info("cache prewarm: %s days=%d freq=%s group=%s — already cached",
                             endpoint, days_val, eff_freq, group)
                    continue

                log.info("cache prewarm: starting %s days=%d freq=%s group=%s",
                         endpoint, days_val, eff_freq, group)
                try:
                    db = SessionLocal()
                    try:
                        if endpoint == "utilization":
                            await api_analytics_utilization(
                                days=days_val, freq=eff_freq, group_by=group, db=db,
                                queue=[], queue_exclude=[], owner=[], owner_exclude=[],
                                project=[], project_exclude=[],
                                allocation_type=[], allocation_type_exclude=[],
                            )
                        else:
                            await api_analytics_queue_depth(
                                days=days_val, freq=eff_freq, group_by=group, db=db,
                                queue=[], queue_exclude=[], owner=[], owner_exclude=[],
                                project=[], project_exclude=[],
                                allocation_type=[], allocation_type_exclude=[],
                            )
                    finally:
                        # Same interrupt-safe teardown as get_db() above.
                        try:
                            db.close()
                        except BaseException:
                            try:
                                db.invalidate()
                            except Exception:
                                pass
                            raise
                    log.info("cache prewarm: done %s days=%d freq=%s group=%s",
                             endpoint, days_val, eff_freq, group)
                except Exception as exc:
                    log.warning("cache prewarm error: %s", exc)

        async def _run_all() -> None:
            # Delay so the server finishes startup and handles the initial
            # page load before kicking off heavy background queries.
            await asyncio.sleep(30)
            # Warm the most common views one at a time to avoid lock contention
            for days_val, freq_val in [(30, 'd'), (7, 'h'), (90, 'd')]:
                for group in ('queue', 'allocation_type'):
                    await _warm(days_val, freq_val, group)
                    await asyncio.sleep(2)  # brief gap between queries

        asyncio.create_task(_run_all())

    return app


# Allow `python -m pbs_monitor.web.server` for quick testing
if __name__ == "__main__":
    import uvicorn
    app = create_app()
    uvicorn.run(app, host="127.0.0.1", port=8080)
