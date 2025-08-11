# Development Setup & Contributing

## Setup

```bash
pip install -r requirements.txt
pip install -e .
```

## Testing

```bash
pytest
pytest --cov=pbs_monitor
```

## Code Quality

```bash
black pbs_monitor/
flake8 pbs_monitor/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests and docs
4. Ensure all tests pass
5. Submit a pull request


