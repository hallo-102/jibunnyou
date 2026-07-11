from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
from app.db.models import DataQualityIssue, OddsSnapshot, Race, RaceEntry, RaceQualityStatus
from app.legacy_bridge.excel_importer import import_race_workbook
from app.legacy_bridge.odds_importer import import_odds_csv
from app.services.data_quality import run_data_quality_checks
from app.services import data_quality as data_quality_service


def _open_test_session(tmp_path: Path) -> Session:
    """Create a disposable importer database for one contract test."""

    engine = create_engine(f"sqlite:///{tmp_path / 'input-contract.db'}")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_workbook_without_now_race_sheet_is_reported_without_partial_entries(tmp_path: Path) -> None:
    workbook = tmp_path / "馬の競走成績_20260710.xlsx"
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        # 必須シートとは無関係なデータだけを含む壊れた入力を再現する。
        pd.DataFrame([{"項目": "値"}]).to_excel(writer, sheet_name="README", index=False)

    with _open_test_session(tmp_path) as db:
        summary = import_race_workbook(db, workbook)
        issues = list(
            db.scalars(
                select(DataQualityIssue).where(DataQualityIssue.source_file == workbook.name)
            )
        )

        assert summary.races == 0
        assert summary.entries == 0
        assert summary.issues == 1
        assert [issue.code for issue in issues] == ["missing_now_race_sheet"]


def test_duplicate_horse_number_is_collapsed_and_reported(tmp_path: Path) -> None:
    workbook = tmp_path / "馬の競走成績_20260711.xlsx"
    duplicated_rows = pd.DataFrame(
        [
            {"レースID": "202699010101", "馬番": 1, "馬名": "契約テスト馬A", "場所": "東京"},
            {"レースID": "202699010101", "馬番": 1, "馬名": "契約テスト馬B", "場所": "東京"},
        ]
    )
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        duplicated_rows.to_excel(writer, sheet_name="今走レース情報", index=False)

    with _open_test_session(tmp_path) as db:
        summary = import_race_workbook(db, workbook)
        stored_entries = list(
            db.scalars(select(RaceEntry).where(RaceEntry.race_id == "202699010101"))
        )
        issue = db.scalar(
            select(DataQualityIssue).where(
                DataQualityIssue.source_file == workbook.name,
                DataQualityIssue.code == "duplicate_horse_no",
            )
        )

        # 読取行数は2件でも、業務キー上の出走馬は1件だけを保持する。
        assert summary.entries == 2
        assert len(stored_entries) == 1
        assert issue is not None
        assert issue.race_id == "202699010101"


def test_odds_row_missing_business_keys_is_rejected_and_reported(tmp_path: Path) -> None:
    odds_csv = tmp_path / "OZZU_20260712.csv"
    # 日付はファイル名から補えるが、開催場・R・式別がないため取込不可とする。
    pd.DataFrame([{"name": "契約テスト馬", "odds": 4.2}]).to_csv(
        odds_csv,
        index=False,
        encoding="utf-8-sig",
    )

    with _open_test_session(tmp_path) as db:
        summary = import_odds_csv(db, odds_csv)
        issue = db.scalar(
            select(DataQualityIssue).where(
                DataQualityIssue.source_file == odds_csv.name,
                DataQualityIssue.code == "missing_odds_key",
            )
        )

        assert summary.odds == 0
        assert summary.entries == 0
        assert summary.issues == 1
        assert issue is not None
        assert issue.row_number == 2


def test_odds_date_mismatch_is_rejected_before_snapshot_creation(tmp_path: Path) -> None:
    odds_csv = tmp_path / "OZZU_20260712.csv"
    pd.DataFrame(
        [
            {
                "date": "20260713",
                "racecourse": "東京",
                "race": "1R",
                "name": "日付不一致馬",
                "bet_type": "単勝",
                "combination": 1,
                "odds": 3.2,
            }
        ]
    ).to_csv(odds_csv, index=False, encoding="utf-8-sig")

    with _open_test_session(tmp_path) as db:
        summary = import_odds_csv(db, odds_csv)
        issue = db.scalar(
            select(DataQualityIssue).where(DataQualityIssue.code == "odds_date_mismatch")
        )

        assert summary.odds == 0
        assert summary.issues == 1
        assert db.query(OddsSnapshot).count() == 0
        assert issue is not None


def test_odds_duplicate_and_horse_identity_mismatch_are_not_auto_joined(tmp_path: Path) -> None:
    odds_csv = tmp_path / "OZZU_20260712.csv"
    pd.DataFrame(
        [
            {
                "date": "20260712",
                "racecourse": "東京",
                "race": "1R",
                "name": "照合テスト馬A",
                "bet_type": "単勝",
                "combination": 1,
                "odds": "2.2",
            },
            {
                "date": "20260712",
                "racecourse": "東京",
                "race": "1R",
                "name": "照合テスト馬A",
                "bet_type": "単勝",
                "combination": 1,
                "odds": "2.3",
            },
            {
                "date": "20260712",
                "racecourse": "東京",
                "race": "1R",
                "name": "別の馬名",
                "bet_type": "単勝",
                "combination": 2,
                "odds": "4.5",
            },
            {
                "date": "20260712",
                "racecourse": "東京",
                "race": "1R",
                "name": "照合テスト馬A",
                "bet_type": "複勝",
                "combination": 1,
                "odds": "1.5-2.0",
            },
        ]
    ).to_csv(odds_csv, index=False, encoding="utf-8-sig")

    with _open_test_session(tmp_path) as db:
        race = Race(
            race_id="202699010101",
            race_date=pd.Timestamp("2026-07-12").date(),
            race_number=1,
            venue="東京",
            headcount=2,
        )
        db.add(race)
        db.add_all(
            [
                RaceEntry(race_id=race.race_id, horse_no=1, horse_name="照合テスト馬A"),
                RaceEntry(race_id=race.race_id, horse_no=2, horse_name="照合テスト馬B"),
            ]
        )
        db.commit()

        summary = import_odds_csv(db, odds_csv)
        run_data_quality_checks(db, race_id=race.race_id)
        db.commit()
        first_entry = db.scalar(
            select(RaceEntry).where(RaceEntry.race_id == race.race_id, RaceEntry.horse_no == 1)
        )
        second_entry = db.scalar(
            select(RaceEntry).where(RaceEntry.race_id == race.race_id, RaceEntry.horse_no == 2)
        )
        place_range = db.scalar(
            select(OddsSnapshot).where(OddsSnapshot.bet_type == "複勝")
        )
        issue_codes = set(db.scalars(select(DataQualityIssue.code)))
        quality = db.scalar(
            select(RaceQualityStatus).where(RaceQualityStatus.race_id == race.race_id)
        )

        assert summary.odds == 3
        assert summary.entries == 1
        assert first_entry.win_odds == 2.2
        assert first_entry.place_odds is None
        assert second_entry.win_odds is None
        assert place_range.odds is None
        assert place_range.odds_min == 1.5
        assert place_range.odds_max == 2.0
        assert "duplicate_odds_key" in issue_codes
        assert "odds_entry_mismatch" in issue_codes
        assert quality.status == "RED"


def test_current_day_stale_odds_blocks_prediction_quality_gate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    fixed_now = datetime(2026, 7, 10, 0, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    monkeypatch.setattr(data_quality_service, "datetime", FixedDateTime)

    with _open_test_session(tmp_path) as db:
        race = Race(
            race_id="202699010101",
            race_date=fixed_now.astimezone(data_quality_service.ZoneInfo("Asia/Tokyo")).date(),
            race_number=1,
            venue="東京",
            start_time="23:59",
            headcount=1,
        )
        db.add(race)
        db.add(
            RaceEntry(
                race_id=race.race_id,
                horse_no=1,
                horse_name="鮮度テスト馬",
                win_odds=3.2,
            )
        )
        db.add(
            OddsSnapshot(
                source_file="OZZU_20260710.csv",
                race_date=race.race_date,
                racecourse="東京",
                race_no=1,
                horse_no=1,
                horse_name="鮮度テスト馬",
                bet_type="単勝",
                combination="1",
                raw_odds="3.2",
                odds=3.2,
                odds_min=3.2,
                odds_max=3.2,
                fetched_at=fixed_now - timedelta(minutes=121),
            )
        )
        db.commit()

        summary = run_data_quality_checks(db, race_id=race.race_id)
        db.commit()
        issue = db.scalar(
            select(DataQualityIssue).where(
                DataQualityIssue.race_id == race.race_id,
                DataQualityIssue.code == "DQ-012",
            )
        )

        assert summary.red == 1
        assert issue is not None
        assert "危険な鮮度" in issue.message
