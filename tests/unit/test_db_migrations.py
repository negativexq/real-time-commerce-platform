"""Migration discovery, ordering, and checksum tests."""

import hashlib
from pathlib import Path

import pytest

from services.db_migrations.migrations import MigrationSettings, discover_migrations


def test_migrations_are_discovered_in_numeric_order(tmp_path: Path) -> None:
    (tmp_path / "002_second.sql").write_text("SELECT 2;")
    (tmp_path / "001_first.sql").write_text("SELECT 1;")
    migrations = discover_migrations(tmp_path)
    assert [item.version for item in migrations] == [1, 2]
    assert migrations[0].checksum == hashlib.sha256(b"SELECT 1;").hexdigest()


def test_invalid_migration_filename_fails(tmp_path: Path) -> None:
    (tmp_path / "latest.sql").write_text("SELECT 1;")
    with pytest.raises(ValueError, match="filename"):
        discover_migrations(tmp_path)


def test_duplicate_migration_version_fails(tmp_path: Path) -> None:
    (tmp_path / "001_first.sql").write_text("SELECT 1;")
    (tmp_path / "001_other.sql").write_text("SELECT 2;")
    with pytest.raises(ValueError, match="unique"):
        discover_migrations(tmp_path)


def test_settings_use_processor_defaults() -> None:
    settings = MigrationSettings.from_environment({})
    assert settings.postgres_dsn.endswith("@postgres:5432/commerce")
    assert settings.required_schema_version == 4


@pytest.mark.parametrize("value", ["0", "-1", "four"])
def test_settings_reject_invalid_schema_version(value: str) -> None:
    with pytest.raises(ValueError, match="PROCESSOR_REQUIRED_SCHEMA_VERSION"):
        MigrationSettings.from_environment({"PROCESSOR_REQUIRED_SCHEMA_VERSION": value})
