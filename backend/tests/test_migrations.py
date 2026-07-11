from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text

from app.db.base import Base
from app.db.migrations import (
    BASELINE_REVISION,
    LegacySchemaError,
    current_revision,
    downgrade_database,
    head_revision,
    schema_differences,
    stamp_legacy_database,
    upgrade_database,
)


def _sqlite_engine(path: Path):
    """Create an isolated file-backed SQLite engine for migration tests."""

    return create_engine(f"sqlite:///{path}")


def test_baseline_upgrade_downgrade_and_reupgrade(tmp_path):
    db_engine = _sqlite_engine(tmp_path / "migration-cycle.db")

    upgrade_database(db_engine=db_engine)
    table_names = set(inspect(db_engine).get_table_names())
    assert set(Base.metadata.tables).issubset(table_names)
    assert current_revision(db_engine) == head_revision()
    assert schema_differences(db_engine) == []

    downgrade_database(BASELINE_REVISION, db_engine=db_engine)
    assert current_revision(db_engine) == BASELINE_REVISION
    assert "audit_logs" not in set(inspect(db_engine).get_table_names())

    downgrade_database("base", db_engine=db_engine)
    remaining_tables = set(inspect(db_engine).get_table_names()) - {"alembic_version"}
    assert remaining_tables == set()

    upgrade_database(db_engine=db_engine)
    assert current_revision(db_engine) == head_revision()
    assert schema_differences(db_engine) == []


def test_unversioned_schema_requires_exact_explicit_stamp(tmp_path):
    db_engine = _sqlite_engine(tmp_path / "legacy-schema.db")
    upgrade_database(BASELINE_REVISION, db_engine)
    with db_engine.begin() as connection:
        connection.execute(text("DROP TABLE alembic_version"))
        connection.execute(
            text(
                "INSERT INTO races (race_id, created_at, updated_at) "
                "VALUES ('legacy-race-001', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )

    with pytest.raises(LegacySchemaError, match="existing schema detected"):
        upgrade_database(db_engine=db_engine)

    with db_engine.connect() as connection:
        assert connection.execute(
            text("SELECT race_id FROM races WHERE race_id = 'legacy-race-001'")
        ).scalar_one() == "legacy-race-001"

    stamp_legacy_database(db_engine)
    assert current_revision(db_engine) == BASELINE_REVISION
    upgrade_database(db_engine=db_engine)
    assert current_revision(db_engine) == head_revision()
    assert schema_differences(db_engine) == []


def test_current_unversioned_schema_can_only_stamp_to_head(tmp_path):
    db_engine = _sqlite_engine(tmp_path / "current-unversioned.db")
    Base.metadata.create_all(db_engine)

    stamp_legacy_database(db_engine)

    assert current_revision(db_engine) == head_revision()
    assert schema_differences(db_engine) == []


def test_partial_version_table_is_not_treated_as_a_valid_migration(tmp_path):
    db_engine = _sqlite_engine(tmp_path / "partial-version.db")
    Base.metadata.create_all(db_engine)
    with db_engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)")
        )

    with pytest.raises(LegacySchemaError, match="partially-versioned"):
        upgrade_database(db_engine=db_engine)


def test_baseline_contains_run_and_horse_uniqueness_constraints(tmp_path):
    db_engine = _sqlite_engine(tmp_path / "constraints.db")
    upgrade_database(db_engine=db_engine)
    inspector = inspect(db_engine)

    race_entry_constraints = {
        item["name"] for item in inspector.get_unique_constraints("race_entries")
    }
    prediction_result_constraints = {
        item["name"] for item in inspector.get_unique_constraints("prediction_results")
    }
    ai_result_constraints = {
        item["name"] for item in inspector.get_unique_constraints("ai_horse_evaluations")
    }
    bet_checks = {
        item["name"] for item in inspector.get_check_constraints("bet_candidates")
    }
    bet_columns = {item["name"] for item in inspector.get_columns("bet_candidates")}

    assert "uq_race_entries_race_horse_no" in race_entry_constraints
    assert "uq_prediction_results_run_race_horse_no" in prediction_result_constraints
    assert "uq_ai_horse_evaluations_run_race_horse" in ai_result_constraints
    assert "ck_bet_candidates_status" in bet_checks
    assert "ck_bet_candidates_total_nonnegative" in bet_checks
    assert "ck_bet_candidates_source_type" in bet_checks
    assert "ck_bet_candidates_strategy_mode" in bet_checks
    assert "ck_bet_candidates_purchase_execution_disabled" in bet_checks
    assert {
        "source_type",
        "ai_analysis_id",
        "strategy_mode",
        "bet_rule_version",
        "warning_codes",
        "requires_confirmation",
        "purchase_execution_enabled",
        "source_snapshot_hash",
    }.issubset(bet_columns)
