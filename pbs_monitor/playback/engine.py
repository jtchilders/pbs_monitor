"""Core playback engine for iterating historical scheduler state."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Generator, Iterable, Iterator, List, Optional, Tuple

from ..config import Config
from ..data_collector import DataCollector
from ..pbs_commands import PBSCommands
from ..database.repositories import RepositoryFactory, PlaybackRepository
from ..database.models import Job


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PlaybackJobView:
   """Representation of a job augmented with playback-specific fields."""

   job_id: str
   owner: Optional[str]
   project: Optional[str]
   allocation: Optional[str]
   queue: Optional[str]
   state: Optional[str]
   nodes: Optional[int]
   start_time: Optional[datetime]
   end_time: Optional[datetime]
   score_at_runtime: Optional[float]
   walltime_actual_seconds: Optional[int]


@dataclass(frozen=True)
class PlaybackSnapshot:
   """Snapshot of scheduler state for a single timestep."""

   tick: datetime
   tick_end: datetime
   jobs: List[PlaybackJobView]
   occupied_nodes: int
   total_nodes: Optional[int]
   occupancy_percent: Optional[float]


class PlaybackEngine:
   """Encapsulates playback iteration, job enrichment, and occupancy stats."""

   def __init__(
      self,
      config: Optional[Config] = None,
      repository_factory: Optional[RepositoryFactory] = None,
      data_collector: Optional[DataCollector] = None,
      pbs_commands: Optional[PBSCommands] = None,
   ) -> None:
      self.config = config or Config()
      self.repo_factory = repository_factory or RepositoryFactory(self.config)
      self.data_collector = data_collector
      self.pbs_commands = pbs_commands or PBSCommands(timeout=self.config.pbs.command_timeout)

      self._score_cache: Dict[str, Optional[float]] = {}
      self._total_nodes: Optional[int] = None
      self._server_data: Optional[Dict[str, object]] = None
      self._server_defaults: Optional[Dict[str, object]] = None

      self.logger = logging.getLogger(__name__)

   # ------------------------------------------------------------------
   # Timestep helpers
   # ------------------------------------------------------------------
   def parse_time_step(self, time_step: Optional[str | timedelta]) -> timedelta:
      """Parse timestep input to a :class:`timedelta`.

      Supports ``timedelta`` objects directly, ``HH:MM:SS`` strings, and
      ``DD:HH:MM`` strings. Detection between the two string layouts is based on
      whether the hours component exceeds 23.
      """

      if isinstance(time_step, timedelta):
         if time_step.total_seconds() <= 0:
            raise ValueError("time_step must be greater than zero")
         return time_step

      if not time_step:
         raise ValueError("time_step value required")

      parts = time_step.strip().split(':')
      if len(parts) != 3:
         raise ValueError("time_step must be in HH:MM:SS or DD:HH:MM format")

      try:
         first, second, third = (int(part) for part in parts)
      except ValueError as exc:
         raise ValueError("time_step components must be integers") from exc

      if second < 0 or third < 0 or first < 0:
         raise ValueError("time_step components must be non-negative")

      if second >= 60 or third >= 60:
         raise ValueError("Minutes/seconds components must be < 60")

      # Interpret as DD:HH:MM when hours overflow a single day.
      if first >= 24:
         delta = timedelta(days=first, hours=second, minutes=third)
      else:
         delta = timedelta(hours=first, minutes=second, seconds=third)

      if delta.total_seconds() <= 0:
         raise ValueError("time_step must represent a positive duration")

      return delta

   def generate_ticks(self, start: datetime, end: datetime, step: timedelta) -> Iterator[datetime]:
      """Yield playback ticks inclusive of start and exclusive of end."""

      if end <= start:
         raise ValueError("end must be after start")

      if step.total_seconds() <= 0:
         raise ValueError("step must be positive")

      current = start
      while current < end:
         yield current
         current = current + step

   # ------------------------------------------------------------------
   # Node capacity helpers
   # ------------------------------------------------------------------
   def resolve_total_nodes(
      self,
      *,
      force_refresh: bool = False,
      fallback: Optional[int] = None,
   ) -> Optional[int]:
      """Resolve and cache total cluster node count (sum of CPUs).

      Args:
         force_refresh: Force a new query via :class:`DataCollector`.
         fallback: Optional value when PBS queries fail.
      """

      if not force_refresh and self._total_nodes is not None:
         return self._total_nodes

      total_nodes = None

      if self.data_collector is not None:
         try:
            nodes = self.data_collector.get_nodes(force_refresh=force_refresh)
            total_nodes = len(nodes)
         except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning(f"Failed to retrieve nodes from PBS: {exc}")

      if not total_nodes:
         if fallback is not None:
            total_nodes = fallback
         else:
            self.logger.debug("Total nodes unavailable; occupancy will lack percentages")

      self._total_nodes = total_nodes
      return self._total_nodes

   # ------------------------------------------------------------------
   # Job fetching and enrichment
   # ------------------------------------------------------------------
   def fetch_running_jobs(self, tick: datetime, step: timedelta) -> List[Job]:
      """Fetch jobs overlapping the timestep window."""

      repository = self._get_playback_repository()
      try:
         return repository.get_jobs_at_tick(tick, step)
      except Exception as exc:
         self.logger.error(f"Failed to fetch playback jobs: {exc}")
         return []

   def build_snapshot(self, tick: datetime, step: timedelta) -> PlaybackSnapshot:
      """Construct a snapshot for the timestep beginning at ``tick``."""

      tick_end = tick + step
      jobs = self.fetch_running_jobs(tick, step)

      job_views = [self._build_job_view(job, tick_end) for job in jobs]

      occupied_nodes = sum(j.nodes or 0 for j in jobs if j.nodes)

      total_nodes = self._total_nodes
      if total_nodes is None:
         total_nodes = self.resolve_total_nodes()

      occupancy_percent = None
      if total_nodes and total_nodes > 0:
         occupancy_percent = (occupied_nodes / total_nodes) * 100.0

      return PlaybackSnapshot(
         tick=tick,
         tick_end=tick_end,
         jobs=job_views,
         occupied_nodes=occupied_nodes,
         total_nodes=total_nodes,
         occupancy_percent=occupancy_percent,
      )

   # ------------------------------------------------------------------
   # Internal helpers
   # ------------------------------------------------------------------
   def _build_job_view(self, job: Job, tick_end: datetime) -> PlaybackJobView:
      score = self._get_job_score(job)
      runtime_seconds = self._calculate_runtime_seconds(job, tick_end)

      state_value = None
      if job.state is not None:
         try:
            state_value = job.state.value  # type: ignore[attr-defined]
         except AttributeError:
            state_value = str(job.state)

      return PlaybackJobView(
         job_id=job.job_id,
         owner=getattr(job, 'owner', None),
         project=getattr(job, 'project', None),
         allocation=getattr(job, 'allocation_type', None),
         queue=getattr(job, 'queue', None),
         state=state_value,
         nodes=getattr(job, 'nodes', None),
         start_time=getattr(job, 'start_time', None),
         end_time=getattr(job, 'end_time', None),
         score_at_runtime=score,
         walltime_actual_seconds=runtime_seconds,
      )

   def _calculate_runtime_seconds(self, job: Job, tick_end: datetime) -> Optional[int]:
      start_time = getattr(job, 'start_time', None)
      self.logger.info(f"[{job.job_id}] Start time: {start_time}")
      if start_time is None:
         return None

      end_time = getattr(job, 'end_time', None)
      self.logger.info(f"[{job.job_id}] End time: {end_time}")
      effective_end = min(tick_end, end_time) if end_time else tick_end

      if effective_end <= start_time:
         return 0

      duration = effective_end - start_time
      return int(duration.total_seconds())

   def _get_job_score(self, job: Job) -> Optional[float]:
      if job.job_id in self._score_cache:
         return self._score_cache[job.job_id]

      raw_data = getattr(job, 'raw_pbs_data', None)
      if raw_data is None:
         self._score_cache[job.job_id] = None
         return None

      if isinstance(raw_data, str):
         try:
            raw_data = json.loads(raw_data)
         except json.JSONDecodeError:
            self.logger.debug(f"Job {job.job_id} raw_pbs_data is not valid JSON")
            self._score_cache[job.job_id] = None
            return None

      if not isinstance(raw_data, dict):
         self._score_cache[job.job_id] = None
         return None

      self._ensure_server_context()

      if not self._server_defaults:
         self._score_cache[job.job_id] = None
         return None

      try:
         score = self.pbs_commands.calculate_job_score(
            raw_data,
            server_defaults=self._server_defaults,
            server_data=self._server_data,
         )
      except Exception as exc:
         self.logger.debug(f"Failed to compute score for job {job.job_id}: {exc}")
         score = None

      self._score_cache[job.job_id] = score
      return score

   def _ensure_server_context(self) -> None:
      if self._server_defaults is not None and self._server_data is not None:
         return

      try:
         self._server_data = self.pbs_commands.qstat_server()
      except Exception as exc:
         self.logger.warning(f"Failed to retrieve PBS server data: {exc}")
         self._server_data = {}

      server_info = {}
      if isinstance(self._server_data, dict):
         server_info = self._server_data.get("Server", {})  # type: ignore[assignment]

      self._server_defaults = None
      if isinstance(server_info, dict):
         for server_details in server_info.values():
            if isinstance(server_details, dict):
               self._server_defaults = server_details.get("resources_default", {})
               break

      if not self._server_defaults:
         self._server_defaults = None

   def _get_playback_repository(self) -> PlaybackRepository:
      return self.repo_factory.get_playback_repository()

