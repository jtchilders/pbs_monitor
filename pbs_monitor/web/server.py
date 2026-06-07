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
from sqlalchemy import create_engine, event, func, or_, and_
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
        }

    # ---- context page routes (must be before static mounts) ----

    @app.get("/page/user/{username}")
    async def serve_user_page(username: str):
        return FileResponse(STATIC_DIR / "user.html")

    @app.get("/page/project/{project}")
    async def serve_project_page(project: str):
        return FileResponse(STATIC_DIR / "project.html")

    # ---- user/project API endpoints ----

    def _date_range(range_days: int) -> datetime:
        """Return UTC datetime for range_days ago."""
        return datetime.now(timezone.utc) - timedelta(days=range_days)

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

    def _build_summary(jobs: list, name: str, kind: str, range_days: int) -> dict:
        """Build summary stats from a list of Job ORM objects."""
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
        range: int = Query(7, ge=1, le=90),
        db: Session = Depends(get_db),
    ):
        jobs = db.query(Job).filter(Job.owner == username).filter(
            Job.submit_time >= _date_range(range)
        ).all()
        return _build_summary(jobs, username, "user", range)

    @app.get("/api/user/{username}/jobs")
    def api_user_jobs(
        username: str,
        range: int = Query(7, ge=1, le=90),
        state: str = Query("ALL"),
        db: Session = Depends(get_db),
    ):
        now = datetime.now(timezone.utc)
        jobs = _query_jobs(db, Job.owner, username, range, state)
        return {"total": len(jobs), "jobs": [_serialize_job(j, now) for j in jobs]}

    @app.get("/api/project/{project}/summary")
    def api_project_summary(
        project: str,
        range: int = Query(7, ge=1, le=90),
        db: Session = Depends(get_db),
    ):
        jobs = db.query(Job).filter(Job.project == project).filter(
            Job.submit_time >= _date_range(range)
        ).all()
        return _build_summary(jobs, project, "project", range)

    @app.get("/api/project/{project}/jobs")
    def api_project_jobs(
        project: str,
        range: int = Query(7, ge=1, le=90),
        state: str = Query("ALL"),
        db: Session = Depends(get_db),
    ):
        now = datetime.now(timezone.utc)
        jobs = _query_jobs(db, Job.project, project, range, state)
        return {"total": len(jobs), "jobs": [_serialize_job(j, now) for j in jobs]}

    # ---- reservations endpoint ----
    @app.get("/api/reservations")
    async def get_reservations(db: Session = Depends(get_db)):
        import json as _json
        from sqlalchemy import func as safunc
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

        # Build a lookup of the most recent utilization analysis per reservation.
        # This uses the reservation_utilization cache table populated by
        # `pbs-monitor database refresh-cache -t utilization`.
        resv_ids = [r.reservation_id for r in reservations]
        util_lookup: dict = {}  # reservation_id -> ReservationUtilization row
        if resv_ids:
            # Subquery: latest analysis_timestamp per reservation_id
            latest_sq = (
                db.query(
                    ReservationUtilization.reservation_id,
                    safunc.max(ReservationUtilization.analysis_timestamp).label('max_ts'),
                )
                .filter(ReservationUtilization.reservation_id.in_(resv_ids))
                .group_by(ReservationUtilization.reservation_id)
                .subquery()
            )
            latest_rows = (
                db.query(ReservationUtilization)
                .join(
                    latest_sq,
                    and_(
                        ReservationUtilization.reservation_id == latest_sq.c.reservation_id,
                        ReservationUtilization.analysis_timestamp == latest_sq.c.max_ts,
                    ),
                )
                .all()
            )
            util_lookup = {u.reservation_id: u for u in latest_rows}

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

            # Pull metrics from the utilization cache when available.
            # Fall back to simple reservation-level math when no cache entry exists
            # (e.g. before the first refresh-cache run).
            util = util_lookup.get(r.reservation_id)
            if util:
                node_hours_reserved = round(util.total_node_hours_reserved or 0, 1)
                node_hours_used     = round(util.total_node_hours_used or 0, 1)
                utilization_pct     = round(util.utilization_percentage or 0, 1)
                jobs_submitted      = util.jobs_submitted or 0
            else:
                # Fallback: compute from reservation metadata + job table
                jobs_submitted = 0
                node_hours_used = 0.0
                if r.queue:
                    agg = db.query(
                        safunc.count(Job.job_id),
                        safunc.sum(
                            safunc.coalesce(
                                Job.nodes * Job.actual_runtime_seconds / 3600.0,
                                0.0
                            )
                        )
                    ).filter(Job.queue == r.queue).one()
                    jobs_submitted  = agg[0] or 0
                    node_hours_used = round(float(agg[1] or 0), 1)

                node_hours_reserved = None
                utilization_pct = None
                if r.nodes and r.duration_seconds:
                    node_hours_reserved = round(r.nodes * r.duration_seconds / 3600, 1)
                    if node_hours_reserved > 0:
                        utilization_pct = round(node_hours_used / node_hours_reserved * 100, 1)

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
        queue: List[str] = Query(default=[]),
        queue_exclude: List[str] = Query(default=[]),
        owner: List[str] = Query(default=[]),
        owner_exclude: List[str] = Query(default=[]),
        project: List[str] = Query(default=[]),
        project_exclude: List[str] = Query(default=[]),
        allocation_type: List[str] = Query(default=[]),
        allocation_type_exclude: List[str] = Query(default=[]),
    ):
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

        def _fetch():
            now = datetime.now(timezone.utc).replace(tzinfo=None)  # naive for arithmetic against DB timestamps
            q = db.query(Job).filter(
                Job.state == JobState.QUEUED,
                Job.submit_time.isnot(None),
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            jobs = q.all()
            counts = [0] * len(BINS)
            for job in jobs:
                st = job.submit_time
                if st is None:
                    continue
                if st.tzinfo is not None:
                    st = st.replace(tzinfo=None)
                wait_h = (now - st).total_seconds() / 3600
                for i, (_, lo, hi) in enumerate(BINS):
                    if lo <= wait_h < hi:
                        counts[i] += 1
                        break
            return {"bins": [b[0] for b in BINS], "counts": counts}

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
                n = job.nodes or 1
                for i, t in enumerate(bins):
                    nt = _next_bin(t, eff_freq)
                    seg_s = max(js, t)
                    seg_e = min(je, nt)
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

            now = datetime.now(timezone.utc)
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

    @app.get("/api/analytics/wait-vs-score")
    async def api_analytics_wait_vs_score(
        days: int = 30,
        x_axis: str = 'queue_time',
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
        if x_axis not in ('queue_time', 'elapsed_time'):
            x_axis = 'queue_time'
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        def _fetch():
            from pbs_monitor.database.models import JobHistory
            # For each job that started in window, get the score from JobHistory
            # at the transition to RUNNING state (closest record to start_time)
            q = db.query(Job).filter(
                Job.start_time >= cutoff,
                Job.start_time.isnot(None),
                Job.submit_time.isnot(None),
            )
            q = _apply_job_filters(q, queue, queue_exclude, owner, owner_exclude,
                                   project, project_exclude, allocation_type, allocation_type_exclude)
            jobs = q.all()

            # Build a lookup: job_id → score from JobHistory near start_time
            job_ids = [j.job_id for j in jobs]
            score_map: dict[str, float] = {}
            if job_ids:
                # Get all history records for these jobs with a score
                history_rows = (
                    db.query(JobHistory)
                    .filter(
                        JobHistory.job_id.in_(job_ids),
                        JobHistory.score.isnot(None),
                        JobHistory.state == JobState.RUNNING,
                    )
                    .all()
                )
                # Take the first RUNNING record per job (chronologically earliest)
                for h in history_rows:
                    if h.job_id not in score_map:
                        score_map[h.job_id] = h.score

            points = []
            for job in jobs:
                score = score_map.get(job.job_id)
                if score is None:
                    continue
                js = job.start_time
                sub = job.submit_time
                if js and js.tzinfo: js = js.replace(tzinfo=None)
                if sub and sub.tzinfo: sub = sub.replace(tzinfo=None)
                queue_time_h = (js - sub).total_seconds() / 3600 if js and sub else None
                if queue_time_h is None or queue_time_h < 0:
                    continue

                # elapsed_time: use queue_time_seconds vs actual queue_time
                # The job has queue_time_seconds stored; for elapsed_time we use it directly
                if x_axis == 'elapsed_time' and job.queue_time_seconds is not None:
                    x_val = job.queue_time_seconds / 3600
                else:
                    x_val = queue_time_h

                points.append({
                    "x":      round(x_val, 4),
                    "y":      round(score, 4),
                    "queue":  job.queue or 'unknown',
                    "job_id": job.job_id,
                    "owner":  job.owner or '',
                })

            if len(points) < 10:
                return {"x_axis": x_axis, "points": [],
                        "note": f"Only {len(points)} scored jobs found in window — insufficient for scatter plot."}
            return {"x_axis": x_axis, "points": points}

        return await asyncio.get_event_loop().run_in_executor(None, _fetch)

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

        Node-hours: RUNNING (elapsed so far) + FINISHED jobs where runtime > 30min.
        Efficiency: FINISHED jobs only, runtime > 30min. Weighted by node-hours:
            efficiency = sum(actual_runtime * nodes) / sum(walltime_seconds * nodes)
        """
        MIN_RUNTIME_SEC = 1800  # 30 minutes
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=window)

        group_col = Job.owner if group_by == "user" else Job.project

        def _fetch():
            # ── node-hours: RUNNING (elapsed) + FINISHED (actual runtime > 30min) ──
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

            # Finished jobs – actual_runtime_seconds, runtime > 30min
            finished = (
                db.query(Job)
                .filter(
                    Job.state == JobState.FINISHED,
                    Job.end_time >= cutoff,
                    Job.actual_runtime_seconds > MIN_RUNTIME_SEC,
                )
                .all()
            )

            # ── efficiency: finished jobs only ──
            # Weighted: sum(actual * nodes) / sum(walltime * nodes)
            eff_actual: dict[str, float] = {}   # sum of actual_runtime * nodes
            eff_requested: dict[str, float] = {}  # sum of walltime_seconds * nodes

            for job in finished:
                key = (job.owner if group_by == "user" else job.project) or "unknown"
                nodes = job.nodes or 1
                actual = job.actual_runtime_seconds or 0
                nh_map[key] = nh_map.get(key, 0.0) + actual * nodes / 3600.0

                # Efficiency denominator: walltime_seconds
                wall = _parse_walltime(job.walltime)
                if wall and wall > 0:
                    eff_actual[key]     = eff_actual.get(key, 0.0)     + actual * nodes
                    eff_requested[key]  = eff_requested.get(key, 0.0)  + wall   * nodes

            # Build unified records
            all_keys = set(nh_map) | set(eff_actual)
            records = []
            for key in all_keys:
                nh = nh_map.get(key, 0.0)
                req = eff_requested.get(key, 0.0)
                act = eff_actual.get(key, 0.0)
                eff = round(act / req * 100, 1) if req > 0 else None
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
