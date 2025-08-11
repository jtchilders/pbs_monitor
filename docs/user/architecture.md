# Architecture Overview

High-level components:
- CLI: user commands (`status`, `jobs`, `nodes`, `queues`, `history`, `database`, `daemon`)
- Data Collector: orchestrates PBS queries and persistence
- Database: SQLAlchemy models, repositories, migrations
- Analytics: optional analysis utilities (e.g., run-score)

Data flow:
1. CLI command invoked
2. PBS commands queried for current state
3. Optional: data persisted to database (`--collect` or daemon)
4. Display formatted tables and summaries


