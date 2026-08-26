# -*- coding: utf-8 -*-
"""レース内順位の一意性と再現性を検証する回帰テスト。"""

from __future__ import annotations

import importlib

import pandas as pd
import pandas.testing as pdt
import pytest


# 数字から始まる既存モジュール名に合わせ、通常のimport文ではなくimportlibを使う。
ranking = importlib.import_module("1_keibayosou_ranking")


def _rank(df: pd.DataFrame, raw_score_col: str = "score_raw") -> pd.Series:
    """本番と同じ同点解消列を指定して順位を作る。"""
    return ranking.create_unique_rank_series(
        df=df,
        race_id_col="rid_str",
        raw_score_col=raw_score_col,
        risk_score_col="risk_score",
        extra_penalty_col="extra_penalty",
        data_confidence_col="data_confidence",
        horse_number_col="馬番",
    )


def _base_rows() -> pd.DataFrame:
    """複数レース、表示score同点、5位境界同点を含む入力を返す。"""
    return pd.DataFrame(
        {
            "rid_str": ["R1"] * 6 + ["R2"] * 3,
            "馬番": [6, 9, 4, 5, 2, 10, 3, 1, 2],
            "馬名": ["A", "B", "C", "D", "E", "F", "G", "H", "I"],
            # R1の1・2位は表示時に61.01へ丸まり、5・6位も58.59へ丸まる。
            "score_raw": [61.0051, 61.0050, 60.0, 59.0, 58.5949, 58.5948, 70.0, 60.0, 50.0],
            "score": [61.01, 61.01, 60.0, 59.0, 58.59, 58.59, 70.0, 60.0, 50.0],
            "risk_score": [20.0, 90.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "extra_penalty": [0.0, 1.2, 0.0, 0.0, 0.0, 0.1, 0.0, 0.0, 0.0],
            "data_confidence": [90.0] * 9,
        }
    )


def test_same_display_score_gets_unique_rank_from_raw_score() -> None:
    """丸め後が同点でも、丸め前scoreの差で1位と2位に分かれる。"""
    df = _base_rows()

    ranks = _rank(df)

    assert ranks.iloc[0] == 1
    assert ranks.iloc[1] == 2
    assert not pd.DataFrame({"rid_str": df["rid_str"], "rank": ranks}).duplicated().any()


def test_exact_raw_tie_uses_required_tie_break_order() -> None:
    """完全同点時はリスク、減点、信頼度、馬番の優先順で決める。"""
    df = pd.DataFrame(
        {
            "rid_str": ["R1"] * 5,
            "馬番": [5, 4, 3, 2, 1],
            "score_raw": [50.0] * 5,
            "risk_score": [80.0, 80.0, 80.0, 80.0, 70.0],
            "extra_penalty": [0.2, 0.2, 0.2, 0.5, 0.0],
            "data_confidence": [60.0, 90.0, 90.0, 100.0, 100.0],
        }
    )

    ranks = _rank(df)

    # 馬番3と4までは全補助指標が同じなので、最後に馬番昇順で馬番3を上位にする。
    rank_by_horse = dict(zip(df["馬番"], ranks))
    assert rank_by_horse == {5: 3, 4: 2, 3: 1, 2: 4, 1: 5}


def test_each_race_has_exactly_one_rank1() -> None:
    """複数レースでも予想1位は各レース1頭だけになる。"""
    df = _base_rows()
    df["rank"] = _rank(df)

    ranking.validate_prediction_ranks(df, raw_score_col="score_raw")

    assert df.loc[df["rank"].eq(1)].groupby("rid_str").size().eq(1).all()


def test_top5_never_expands_when_fifth_display_score_is_tied() -> None:
    """5位境界の表示scoreが同点でもTOP5を6頭へ広げない。"""
    df = _base_rows()
    df["rank"] = _rank(df)

    top5 = ranking.select_top5_predictions(df, raw_score_col="score_raw")

    assert len(top5.loc[top5["rid_str"].eq("R1")]) == 5
    assert top5.loc[top5["rid_str"].eq("R1"), "rank"].tolist() == [1, 2, 3, 4, 5]


def test_ranks_are_consecutive_for_every_race() -> None:
    """各レースの順位が1から出走頭数までの連番になる。"""
    df = _base_rows()
    df["rank"] = _rank(df)

    for _, race in df.groupby("rid_str"):
        assert sorted(race["rank"].tolist()) == list(range(1, len(race) + 1))


def test_same_input_produces_same_rank_and_top5_order() -> None:
    """同一入力を2回処理して順位とTOP5の並びが完全一致する。"""
    first = _base_rows()
    second = _base_rows()
    first["rank"] = _rank(first)
    second["rank"] = _rank(second)

    first_top5 = ranking.select_top5_predictions(first, raw_score_col="score_raw")
    second_top5 = ranking.select_top5_predictions(second, raw_score_col="score_raw")

    pdt.assert_series_equal(first["rank"], second["rank"])
    pdt.assert_frame_equal(
        first_top5[["rid_str", "馬番", "rank"]].reset_index(drop=True),
        second_top5[["rid_str", "馬番", "rank"]].reset_index(drop=True),
    )


def test_best_and_five_block_ranks_are_both_unique() -> None:
    """新bestと5ブロックの両方が独立した丸め前scoreで一意になる。"""
    df = _base_rows().assign(
        five_block_score_raw=[55.0051, 55.0050, 54.0, 53.0, 52.0051, 52.0050, 65.0, 64.0, 63.0]
    )
    df["rank"] = _rank(df, raw_score_col="score_raw")
    df["five_block_rank"] = _rank(df, raw_score_col="five_block_score_raw")

    assert not df.duplicated(["rid_str", "rank"]).any()
    assert not df.duplicated(["rid_str", "five_block_rank"]).any()


def test_non_tied_existing_order_is_preserved() -> None:
    """同点がないレースでは従来のscore降順と同じ順位を維持する。"""
    df = pd.DataFrame(
        {
            "rid_str": ["R1"] * 4,
            "馬番": [8, 2, 5, 1],
            "score_raw": [40.0, 10.0, 30.0, 20.0],
        }
    )

    ranks = ranking.create_unique_rank_series(df, "rid_str", "score_raw", horse_number_col="馬番")

    assert ranks.tolist() == [1, 4, 2, 3]


def test_validation_rejects_duplicate_rank_with_diagnostics() -> None:
    """保存前検証は重複順位を具体的なrace_id付きで停止させる。"""
    df = _base_rows().iloc[:2].copy()
    df["rank"] = [1, 1]

    with pytest.raises(RuntimeError, match="R1"):
        ranking.validate_prediction_ranks(df, raw_score_col="score_raw")


def test_pipeline_uses_raw_score_before_rounding(monkeypatch: pytest.MonkeyPatch) -> None:
    """本番score計算でも表示丸めより前に一意順位を確定する。"""
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    monkeypatch.setattr(pipeline, "FEAT_COLS", ["feature"])
    monkeypatch.setattr(pipeline, "apply_weights", lambda values, place, surface: values)
    monkeypatch.setattr(pipeline, "score_sum", lambda values: float(values["feature"]))
    monkeypatch.setattr(pipeline, "calc_rest_dist_risk", lambda row: 0.0)
    monkeypatch.setattr(
        pipeline,
        "calc_extra_penalty_components",
        lambda row, rest_dist_risk: {
            "popular_underperformer": 0.0,
            "good_loser": 0.0,
            "ta_n": 0.0,
            "close_loss": 0.0,
            "rest_distance": 0.0,
        },
    )

    def fake_five_block(frame: pd.DataFrame) -> pd.DataFrame:
        """新bestの補助同点解消列と5ブロック参照列だけを付ける。"""
        out = frame.copy()
        out["risk_score"] = 0.0
        out["data_confidence"] = 100.0
        out["five_block_raw_score"] = out["total"]
        out["five_block_score_raw"] = out["score_raw"]
        out["five_block_rank"] = ranking.create_unique_rank_series(
            out,
            "rid_str",
            "five_block_score_raw",
            risk_score_col="risk_score",
            extra_penalty_col="extra_penalty",
            data_confidence_col="data_confidence",
            horse_number_col="馬番",
        )
        out["five_block_score"] = out["five_block_score_raw"].round(2)
        return out

    monkeypatch.setattr(pipeline, "compute_five_block_scores", fake_five_block)
    df = pd.DataFrame(
        {
            "rid_str": ["R1"] * 4,
            "馬番": [6, 9, 4, 5],
            # 先頭2頭は正規化後の表示scoreが同じになる程度だけ差を付ける。
            "feature": [529.921279, 530.079965, 484.242496, 424.870993],
        }
    )

    out = pipeline.compute_scores_with_pipeline_logic(
        df,
        place_map={},
        surface_map={},
        calc_fav_risk=lambda row: 0.0,
        alpha=0.0,
        extra_alpha=0.0,
    )

    assert out.loc[out["馬番"].eq(9), "rank"].item() == 1
    assert out.loc[out["馬番"].eq(6), "rank"].item() == 2
    assert not out.duplicated(["rid_str", "rank"]).any()


def test_optimizer_evaluation_uses_shared_unique_rank() -> None:
    """最適化評価側も丸め済みdense順位を作らず一意順位を返す。"""
    scoring = importlib.import_module("tokutyouryou_keisann.scoring")
    config = importlib.import_module("tokutyouryou_keisann.config")
    feature = config.FEAT_COLS[0]
    # 全特徴量を一括生成し、テスト用の小さいDataFrameでも列追加断片化を起こさない。
    data = {column: [0.0] * 4 for column in config.FEAT_COLS}
    data.update(
        {
            "rid_str": ["R1"] * 4,
            "馬番": [6, 9, 4, 5],
            "name_norm": ["A", "B", "C", "D"],
            feature: [529.921279, 530.079965, 484.242496, 424.870993],
        }
    )
    df = pd.DataFrame(data)
    weights = {"__default__": {feature: 1.0}}

    out = scoring.compute_scores_with_optimizer_weights(df, weights)

    assert out.loc[out["馬番"].eq(9), "rank"].item() == 1
    assert out.loc[out["馬番"].eq(6), "rank"].item() == 2
    assert not out.duplicated(["rid_str", "rank"]).any()


def test_bet_sheet_uses_unique_rank_and_is_reproducible() -> None:
    """買い目シートの1〜6位馬番も共通順位に従い、再実行で一致する。"""
    pipeline = importlib.import_module("1_keibayosou_pipeline")
    feat = _base_rows().loc[lambda frame: frame["rid_str"].eq("R1")].copy()
    feat["rank"] = _rank(feat)
    now = pd.DataFrame({"rid_str": ["R1"]})

    first = pipeline._build_bet_sheet(feat, now)
    second = pipeline._build_bet_sheet(feat, now)

    assert first.loc[0, "1位馬番"] == 6
    assert first.loc[0, "5位馬番"] == 2
    pdt.assert_frame_equal(first, second)
