from __future__ import annotations

import sqlite3

import pytest

from races_review.generate_daily_review import (
    COURSE_CODES,
    build_track_condition_stats,
    calculate_next_start_outcomes,
    create_database,
    official_race_ids_from_html,
    official_race_counts_from_html,
    verify_official_race_counts,
)


def test_all_jra_racecourses_are_supported() -> None:
    """JRAの全10競馬場コードを欠落なく名前へ変換できることを確認する。"""
    assert COURSE_CODES == {
        "01": "札幌",
        "02": "函館",
        "03": "福島",
        "04": "新潟",
        "05": "東京",
        "06": "中山",
        "07": "中京",
        "08": "京都",
        "09": "阪神",
        "10": "小倉",
    }


def test_official_race_count_parser_and_mismatch_guard() -> None:
    """JRA番組表から開催場別の実施数を読み、欠落時は保存前に停止する。"""
    html = """
    <html><body>
      <table class="basic"><div class="main">4回東京1日</div>
        <tr><th>レース番号</th></tr>
        <tr><td>1レース</td></tr><tr><td>2レース</td></tr>
      </table>
      <table class="basic"><div class="main">4回京都1日</div>
        <tr><td>京都競馬は中止。</td></tr>
      </table>
    </body></html>
    """

    counts = official_race_counts_from_html(html)

    assert counts == {"東京": 2, "京都": 0}
    verify_official_race_counts(["202605040101", "202605040102"], counts)
    with pytest.raises(RuntimeError, match="JRA公式レース数と不一致"):
        verify_official_race_counts(["202605040101"], counts)


def test_official_program_restores_race_ids_when_history_sheet_is_empty() -> None:
    """結果ブックが空でも公式の開催回・日・R番号から正規IDを復元する。"""
    html = """
    <html><body><table class="basic">
      <tr><th><div class="main">2回札幌2日</div></th></tr>
      <tr><th>レース番号</th></tr>
      <tr><td>1レース</td></tr><tr><td>2レース</td></tr>
    </table></body></html>
    """

    race_ids = official_race_ids_from_html(html, "20260816")

    assert race_ids == ["202601020201", "202601020202"]


def test_official_program_ignores_cancellation_notice_with_race_number() -> None:
    """「第8レース以降取りやめ」の注記を実施レースとして数えない。"""
    html = """
    <html><body><table class="basic">
      <tr><th><div class="main">1回東京3日</div></th></tr>
      <tr><th class="num">7<span>レース</span></th><td>実施レース</td></tr>
      <tr><td colspan="3">東京競馬は第8レース以降を取りやめ。</td></tr>
    </table></body></html>
    """

    counts = official_race_counts_from_html(html)
    race_ids = official_race_ids_from_html(html, "20260207")

    assert counts == {"東京": 1}
    assert race_ids == ["202605010307"]


def test_condition_clock_stats_use_distance_normalized_difference() -> None:
    """時計差を1000m当たりへ正規化し、距離・馬場・クラス別に分ける。"""
    races = [
        {
            "date": "20261010",
            "racecourse": "東京",
            "surface": "芝",
            "distance_group": "中距離",
            "going": "良",
            "class_name": "1勝",
            "clock_diff_per_1000m": -0.5,
        },
        {
            "date": "20261010",
            "racecourse": "東京",
            "surface": "芝",
            "distance_group": "中距離",
            "going": "良",
            "class_name": "1勝",
            "clock_diff_per_1000m": 0.1,
        },
        {
            "date": "20261010",
            "racecourse": "東京",
            "surface": "芝",
            "distance_group": "マイル",
            "going": "稍重",
            "class_name": "2勝",
            "clock_diff_per_1000m": 0.8,
        },
    ]

    rows = build_track_condition_stats(races)

    assert len(rows) == 2
    medium = next(row for row in rows if row["distance_group"] == "中距離")
    assert medium["race_count"] == 2
    assert medium["clock_diff_per_1000m_mean"] == pytest.approx(-0.2)
    assert medium["clock_diff_per_1000m_median"] == pytest.approx(-0.2)
    assert medium["going"] == "良"
    assert medium["class_name"] == "1勝"


def test_next_start_tracking_uses_horse_id_and_calculates_win_roi() -> None:
    """同名ではなくhorse_idで次走を結び、単勝100円換算の回収率を計算する。"""
    signals = [
        {
            "signal_type": "WATCH",
            "signal_date": "20260920",
            "horse_id": "2024100001",
            "horse_name": "テストホース",
            "signal_source_race_id": "202605010101",
            "valid_until": "2027-03-19",
        }
    ]
    history = [
        {
            "date": "20260927",
            "horse_id": "2024100001",
            "horse_name": "テストホース",
            "source_race_id": "202605020201",
            "racecourse": "東京",
            "race_no": 1,
            "finish": 1,
            "popularity": 3,
            "odds": 4.2,
        },
        {
            "date": "20260927",
            "horse_id": "9999999999",
            "horse_name": "テストホース",
            "source_race_id": "202605020202",
            "racecourse": "東京",
            "race_no": 2,
            "finish": 9,
            "popularity": 8,
            "odds": 30.0,
        },
    ]

    rows = calculate_next_start_outcomes(signals, history, as_of_date="20260927")

    assert len(rows) == 1
    assert rows[0]["next_source_race_id"] == "202605020201"
    assert rows[0]["next_popularity"] == 3
    assert rows[0]["next_odds"] == pytest.approx(4.2)
    assert rows[0]["win_stake"] == 100
    assert rows[0]["win_return"] == pytest.approx(420.0)
    assert rows[0]["win_roi_pct"] == pytest.approx(420.0)


def test_database_migrates_existing_review_schema(tmp_path) -> None:
    """旧DBへ馬ID列と追跡・条件別時計テーブルを非破壊で追加する。"""
    path = tmp_path / "review.db"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE horse_review (date TEXT, racecourse TEXT, race_no INTEGER, horse_name TEXT)"
    )
    connection.commit()
    connection.close()

    migrated = create_database(path)

    horse_columns = {row[1] for row in migrated.execute("PRAGMA table_info(horse_review)")}
    table_names = {
        row[0]
        for row in migrated.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    migrated.close()
    assert {"horse_id", "source_race_id"}.issubset(horse_columns)
    assert {"horse_master", "next_start_outcome", "track_condition_stats"}.issubset(table_names)
