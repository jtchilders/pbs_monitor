# Performance and Maintenance

## Tips

- Prefer PostgreSQL for multi-user and larger deployments
- Set sensible collection intervals; avoid very frequent collection
- Use cleanup regularly to enforce retention

## Cleanup

```bash
pbs-monitor database cleanup --job-history-days 365 --snapshot-days 90
```

## Backups (SQLite)

```bash
pbs-monitor database backup [path]
pbs-monitor database restore <path>
```


