"""Utilities for handling database connection URLs."""


def mask_db_url(url: str) -> str:
    """Return ``url`` with the password segment replaced by ``***``.

    The masked form is safe to print, log, or include in error messages. The
    scheme, username, host, port, and database name are preserved so the URL
    remains useful for debugging.

    Examples:
        >>> mask_db_url("postgresql://u:secret@host:5432/db")
        'postgresql://u:***@host:5432/db'
        >>> mask_db_url("postgresql://host:5432/db")  # no auth
        'postgresql://host:5432/db'
        >>> mask_db_url("sqlite:///tmp/file.db")
        'sqlite:///tmp/file.db'

    Why this lives in utils rather than database/connection: the latter is
    bound to DatabaseManager (instantiable, stateful). This is a pure string
    function that CLI/scripts/web code can import without dragging in
    SQLAlchemy. Previously inlined in three places — see commit history.
    """
    if "://" not in url or "@" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" not in rest:
        return url
    auth, host_part = rest.split("@", 1)
    if ":" not in auth:
        return url
    user, _password = auth.split(":", 1)
    return f"{scheme}://{user}:***@{host_part}"
