# CLAUDE.md - Project Context for AI Agents

## Project Overview
PBS Monitor: A CLI toolkit for monitoring PBS scheduler environments with historical data storage and analytics.

## Development Commands
- **Install:** `pip install -e .`
- **Test:** `pytest` (Ensure `pytest` is installed in your environment)
- **Run Dev Web Server:** `python -m pbs_monitor.cli.main web --host 127.0.0.1 --port 18991`
- **Database Init:** `pbs-monitor database init`

## Coding Standards
- **Language:** Python 3.8+
- **Typing:** Use type hints for all function signatures.
- **Style:** Follow PEP 8; use f-strings for string interpolation.
- **Documentation:** Docstrings for all public functions and classes.
- **Error Handling:** Prefer specific exceptions over broad `except Exception`.

## Key Architecture
- `pbs_monitor/`: Core logic, models, and database management.
    - `cli/`: Command-line interface implementations.
    - `database/`: Database interactions and schema management.
    - `models/`: Data models (SQLAlchemy/similar).
    - `web/`: Web dashboard and API components.
- `docs/`: User documentation.
- `tests/`: Test suite.

## AI Workflow Rules
- When modifying database models, suggest corresponding migration/schema update steps.
- Before suggesting heavy data operations, check if a `database backup` is needed.
- If running in a terminal-only environment, prefer text-based outputs.
- Always verify if a change affects the `pbs-monitor` CLI entry points.
