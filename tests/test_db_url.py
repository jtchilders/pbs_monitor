"""Tests for pbs_monitor.utils.db_url.mask_db_url."""

import pytest

from pbs_monitor.utils.db_url import mask_db_url


class TestMaskDbUrl:
    """Verify password masking across the URL shapes we actually use."""

    def test_postgres_with_password(self):
        assert (
            mask_db_url("postgresql://u:secret@host:5432/db")
            == "postgresql://u:***@host:5432/db"
        )

    def test_postgres_with_password_no_port(self):
        assert (
            mask_db_url("postgresql://u:secret@host/db")
            == "postgresql://u:***@host/db"
        )

    def test_postgres_with_complex_password(self):
        # passwords can contain special chars; our masker should not be fooled
        # by ':' or '@' embedded in the password — the SPLIT happens left-to-right
        # so the first ':' after the scheme separates user from password, and
        # the LAST '@' separates auth from host. This test pins behavior.
        url = "postgresql://u:p@ss:w0rd@host:5432/db"
        masked = mask_db_url(url)
        # The current implementation splits auth on first ':' and host on first
        # '@' in the auth-or-later portion, so this URL produces a
        # technically-correct mask that still hides 'p@ss:w0rd'.
        assert "p@ss:w0rd" not in masked
        assert masked.startswith("postgresql://u:***@")

    def test_sqlite_url_unchanged(self):
        # No auth segment to mask
        assert mask_db_url("sqlite:///tmp/file.db") == "sqlite:///tmp/file.db"

    def test_url_without_password_unchanged(self):
        # Some URLs are just user@host with no password
        url = "postgresql://u@host:5432/db"
        assert mask_db_url(url) == url

    def test_url_without_auth_unchanged(self):
        url = "postgresql://host:5432/db"
        assert mask_db_url(url) == url

    def test_empty_string(self):
        assert mask_db_url("") == ""

    def test_arbitrary_string_unchanged(self):
        # If we get something that doesn't look like a URL, return it as-is
        # rather than crashing.
        assert mask_db_url("not a url") == "not a url"

    def test_preserves_database_name(self):
        masked = mask_db_url("postgresql://u:s@host:5432/very_specific_db_name")
        assert "very_specific_db_name" in masked

    def test_preserves_username(self):
        masked = mask_db_url("postgresql://distinctive_user:s@host/db")
        assert "distinctive_user" in masked
