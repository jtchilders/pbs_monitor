"""
Tests for pbs_monitor.config: the new ``pbs.system`` field and the
config-file permission check.
"""

import os
import stat
import textwrap

import pytest

from pbs_monitor.config import (
   Config,
   InsecureConfigError,
   _config_contains_credentials,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_yaml(path, text: str, mode: int = 0o600) -> str:
   """Write a config YAML at ``path`` with permissions ``mode``."""
   path.write_text(textwrap.dedent(text))
   os.chmod(path, mode)
   return str(path)


# ---------------------------------------------------------------------------
# pbs.system field
# ---------------------------------------------------------------------------

class TestPbsSystemField:
   """The new pbs.system field should round-trip through YAML config."""

   def test_default_is_none(self, tmp_path):
      """With no config file, pbs.system has no implicit default value.

      The migration plan calls for an explicit value at deploy time;
      a missing system should be an explicit error elsewhere, not
      silently default to a string.
      """
      cfg = Config(config_file=str(tmp_path / "nope.yaml"))
      assert cfg.pbs.system is None

   def test_system_loaded_from_yaml(self, tmp_path):
      """When ``pbs.system`` is set in YAML, the loaded config reflects it."""
      path = _write_yaml(
         tmp_path / "pbs_monitor.yaml",
         """\
         pbs:
            system: polaris
         """,
      )
      cfg = Config(config_file=path)
      assert cfg.pbs.system == "polaris"

   def test_system_aurora_value(self, tmp_path):
      """Sanity: a different value also round-trips."""
      path = _write_yaml(
         tmp_path / "pbs_monitor.yaml",
         """\
         pbs:
            system: aurora
         """,
      )
      cfg = Config(config_file=path)
      assert cfg.pbs.system == "aurora"


# ---------------------------------------------------------------------------
# _config_contains_credentials regex
# ---------------------------------------------------------------------------

class TestCredentialDetection:
   """The regex used to decide whether a config carries DB creds."""

   def test_passwordless_postgres_url_no_creds(self):
      assert not _config_contains_credentials(
         {"database": {"url": "postgresql://localhost/pbs_monitor_dev"}}
      )

   def test_postgres_with_user_only_no_creds(self):
      """Username without password should not trigger the check.

      A URL like postgresql://user@host/db relies on .pgpass or peer
      auth, not inline credentials.
      """
      assert not _config_contains_credentials(
         {"database": {"url": "postgresql://parton@localhost/pbs_monitor_dev"}}
      )

   def test_sqlite_url_no_creds(self):
      assert not _config_contains_credentials(
         {"database": {"url": "sqlite:////home/parton/.pbs_monitor.db"}}
      )

   def test_postgres_user_password_has_creds(self):
      assert _config_contains_credentials(
         {"database": {"url": "postgresql://pbs_monitor:s3cret@localhost:15432/pbs_monitor_data"}}
      )

   def test_empty_config_no_creds(self):
      assert not _config_contains_credentials({})
      assert not _config_contains_credentials(None)

   def test_no_database_section_no_creds(self):
      assert not _config_contains_credentials({"pbs": {"system": "polaris"}})


# ---------------------------------------------------------------------------
# Permission check end-to-end
# ---------------------------------------------------------------------------

class TestConfigPermissionCheck:
   """``Config(...)`` should refuse to load a credentialed file that's
   group/other readable."""

   CREDENTIAL_YAML = """\
      database:
         url: postgresql://pbs_monitor:s3cret@localhost:15432/pbs_monitor_data
      pbs:
         system: polaris
   """

   def test_credential_config_chmod_600_loads_fine(self, tmp_path):
      path = _write_yaml(tmp_path / "ok.yaml", self.CREDENTIAL_YAML, mode=0o600)
      cfg = Config(config_file=path)
      # Should not raise; values should load.
      assert cfg.pbs.system == "polaris"
      assert "pbs_monitor:s3cret" in cfg.database.url

   def test_credential_config_group_readable_raises(self, tmp_path):
      path = _write_yaml(tmp_path / "bad.yaml", self.CREDENTIAL_YAML, mode=0o640)
      with pytest.raises(InsecureConfigError) as exc:
         Config(config_file=path)
      # Helpful error message should mention chmod 600 and the path.
      msg = str(exc.value)
      assert "chmod 600" in msg
      assert str(path) in msg

   def test_credential_config_world_readable_raises(self, tmp_path):
      path = _write_yaml(tmp_path / "bad.yaml", self.CREDENTIAL_YAML, mode=0o644)
      with pytest.raises(InsecureConfigError):
         Config(config_file=path)

   def test_passwordless_config_lax_perms_ok(self, tmp_path):
      """A config without inline DB credentials shouldn't trip the check
      even if its permissions are lax (no secrets to protect)."""
      yaml_text = """\
         database:
            url: postgresql://localhost/pbs_monitor_dev
         pbs:
            system: polaris
      """
      path = _write_yaml(tmp_path / "ok.yaml", yaml_text, mode=0o644)
      cfg = Config(config_file=path)
      assert cfg.pbs.system == "polaris"

   def test_allow_insecure_config_bypasses_check(self, tmp_path):
      """The escape hatch for debugging should silently allow the load."""
      path = _write_yaml(tmp_path / "bad.yaml", self.CREDENTIAL_YAML, mode=0o644)
      cfg = Config(config_file=path, allow_insecure_config=True)
      # Loads without raising.
      assert cfg.pbs.system == "polaris"

   def test_nonexistent_config_does_not_raise(self, tmp_path):
      """Missing files were always silent; preserve that behavior."""
      cfg = Config(config_file=str(tmp_path / "does-not-exist.yaml"))
      # Defaults apply; no exception.
      assert cfg.pbs.system is None
