"""Helpers for rendering playback output."""

from __future__ import annotations

from typing import Iterable, List, Optional
from datetime import datetime

from rich.console import Console
from rich.table import Table
from rich.text import Text

from ..config import Config
from ..utils.formatters import format_timestamp, format_duration, format_number


def build_header_text(tick: datetime, span_seconds: int, total_nodes: Optional[int], occupied_nodes: int, occupancy_percent: Optional[float]) -> Text:
   """Construct header text summarizing snapshot context."""

   time_str = format_timestamp(tick)
   span_str = format_duration(span_seconds)

   parts = [f"{time_str}", f"window {span_str}"]

   if total_nodes:
      parts.append(f"{occupied_nodes}/{total_nodes} nodes")
   else:
      parts.append(f"{occupied_nodes} nodes")

   if occupancy_percent is not None:
      parts.append(f"{occupancy_percent:.1f}%")

   header_text = " | ".join(parts)
   return Text(header_text, style="bold")


def render_occupancy_bar(percent: Optional[float], width: int = 60, filled_char: str = '#', empty_char: str = '-') -> str:
   """Render an ASCII occupancy bar."""

   if percent is None:
      return "Occupancy: N/A"

   clamped = max(0.0, min(100.0, percent))
   filled_width = int(round((clamped / 100.0) * width))
   filled = filled_char * filled_width
   empty = empty_char * (width - filled_width)
   return f"Occupancy: |{filled}{empty}| {clamped:.1f}%"


def render_jobs_table(config: Config, columns: List[str], jobs: Iterable[dict]) -> Table:
   """Render jobs into a Rich Table using provided columns."""

   table = Table(show_header=True, header_style="bold magenta")

   for column in columns:
      table.add_column(column)

   for job in jobs:
      row = [ _format_cell(column, job.get(column)) for column in columns ]
      table.add_row(*row)

   return table


def _format_cell(column: str, value) -> str:
   if column in {"nodes", "score_at_runtime", "walltime_actual"}:
      if column == "walltime_actual":
         if value is None:
            return "N/A"
         return format_duration(int(value))

      if column == "score_at_runtime":
         if value is None:
            return "N/A"
         return f"{value:.2f}"

      return format_number(value)

   if value is None:
      return "N/A"

   return str(value)

