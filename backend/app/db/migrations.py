from __future__ import annotations

from pathlib import Path
from typing import Any

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import Engine, JSON, create_engine, inspect, text

from app.db.base import Base
from app.db import models  # noqa: F401  # 全モデルをmetadataへ登録する。
from app.db.session import engine


BASELINE_REVISION = "0001_baseline"
BACKEND_DIR = Path(__file__).resolve().parents[2]
ALEMBIC_INI_PATH = BACKEND_DIR / "alembic.ini"
ALEMBIC_SCRIPT_PATH = BACKEND_DIR / "alembic"


class LegacySchemaError(RuntimeError):
    """Raised when an unversioned non-empty database requires explicit review."""


def _alembic_config() -> Config:
    """Build an Alembic config without replacing application JSON logging."""

    config = Config(str(ALEMBIC_INI_PATH))
    config.set_main_option("script_location", str(ALEMBIC_SCRIPT_PATH))
    config.attributes["configure_logger"] = False
    return config


def _business_table_names(connection: Any) -> set[str]:
    """Return application tables while excluding Alembic's own version table."""

    return set(inspect(connection).get_table_names()) - {"alembic_version"}


def schema_differences(db_engine: Engine = engine) -> list[Any]:
    """Compare the live schema with current SQLAlchemy metadata."""

    def compare_server_default(
        _context: Any,
        _inspected_column: Any,
        metadata_column: Any,
        _inspected_default: Any,
        _metadata_default: Any,
        _rendered_metadata_default: Any,
    ) -> bool | None:
        # PostgreSQLのjson型には等値演算子がないため、JSON既定値はmigrationで明示確認する。
        if isinstance(metadata_column.type, JSON):
            return False
        return None

    with db_engine.connect() as connection:
        context = MigrationContext.configure(
            connection,
            opts={
                "compare_type": True,
                "compare_server_default": compare_server_default,
            },
        )
        return list(compare_metadata(context, Base.metadata))


def current_revision(db_engine: Engine = engine) -> str | None:
    """Return the applied Alembic revision, or None for an unversioned database."""

    with db_engine.connect() as connection:
        if not inspect(connection).has_table("alembic_version"):
            return None
        return connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one_or_none()


def head_revision() -> str:
    """Return the single head revision shipped with this application build."""

    revision = ScriptDirectory.from_config(_alembic_config()).get_current_head()
    if revision is None:
        raise RuntimeError("Alembic head revision is not defined.")
    return revision


def upgrade_database(revision: str = "head", db_engine: Engine = engine) -> None:
    """Upgrade an empty or already-versioned database without implicit stamping."""

    with db_engine.begin() as connection:
        tables = _business_table_names(connection)
        has_version_table = inspect(connection).has_table("alembic_version")
        applied_revision = None
        if has_version_table:
            applied_revision = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one_or_none()
        if tables and applied_revision is None:
            raise LegacySchemaError(
                "Unversioned or partially-versioned existing schema detected. Verify it "
                "and run the explicit stamp-legacy command before upgrade."
            )

        config = _alembic_config()
        config.attributes["connection"] = connection
        command.upgrade(config, revision)


def downgrade_database(revision: str, db_engine: Engine = engine) -> None:
    """Downgrade to an explicit revision for controlled rollback and tests."""

    with db_engine.begin() as connection:
        config = _alembic_config()
        config.attributes["connection"] = connection
        command.downgrade(config, revision)


def _schema_signature(db_engine: Engine) -> dict[str, Any]:
    """Build a cross-dialect structural signature for legacy schema validation."""

    signature: dict[str, Any] = {}
    with db_engine.connect() as connection:
        db_inspector = inspect(connection)
        for table_name in sorted(_business_table_names(connection)):
            columns = tuple(
                sorted(
                    (
                        column["name"],
                        bool(column.get("nullable", True)),
                        bool(column.get("primary_key", False)),
                    )
                    for column in db_inspector.get_columns(table_name)
                )
            )
            unique_constraints = tuple(
                sorted(
                    tuple(item.get("column_names") or ())
                    for item in db_inspector.get_unique_constraints(table_name)
                )
            )
            foreign_keys = tuple(
                sorted(
                    (
                        tuple(item.get("constrained_columns") or ()),
                        item.get("referred_table"),
                        tuple(item.get("referred_columns") or ()),
                    )
                    for item in db_inspector.get_foreign_keys(table_name)
                )
            )
            indexes = tuple(
                sorted(
                    (
                        item.get("name"),
                        tuple(item.get("column_names") or ()),
                        bool(item.get("unique", False)),
                    )
                    for item in db_inspector.get_indexes(table_name)
                    if str(item.get("name") or "").startswith("ix_")
                )
            )
            signature[table_name] = {
                "columns": columns,
                "unique_constraints": unique_constraints,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
            }
    return signature


def _baseline_schema_signature() -> dict[str, Any]:
    """Create a disposable baseline DB and return its frozen structure."""

    reference_engine = create_engine("sqlite://")
    try:
        upgrade_database(BASELINE_REVISION, reference_engine)
        return _schema_signature(reference_engine)
    finally:
        reference_engine.dispose()


def stamp_legacy_database(db_engine: Engine = engine) -> None:
    """Stamp only a schema that exactly matches baseline or the current head."""

    with db_engine.connect() as connection:
        tables = _business_table_names(connection)
        if not tables:
            raise LegacySchemaError("Legacy stamp requires a non-empty database.")
        if inspect(connection).has_table("alembic_version"):
            raise LegacySchemaError("Database already has an Alembic revision.")

    differences = schema_differences(db_engine)
    if not differences:
        target_revision = "head"
    elif _schema_signature(db_engine) == _baseline_schema_signature():
        target_revision = BASELINE_REVISION
    else:
        raise LegacySchemaError(
            f"Legacy schema differs from both baseline and head in "
            f"{len(differences)} operation(s); automatic stamp was refused."
        )

    with db_engine.begin() as connection:
        config = _alembic_config()
        config.attributes["connection"] = connection
        command.stamp(config, target_revision)
