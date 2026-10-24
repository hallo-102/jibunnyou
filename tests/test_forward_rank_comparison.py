import importlib
import json
import pytest

m = importlib.import_module("1_keibayosou_forward_rank_comparison")


def race(rid="209901010101"):
    return {"race_id": rid, "class": "1勝クラス", "horses": [
        {"horse_number": 1, "production_rank": 1, "conditional_rank": 2},
        {"horse_number": 2, "production_rank": 2, "conditional_rank": 1},
        {"horse_number": 3, "production_rank": 3, "conditional_rank": 3},
    ]}


def actual():
    return {"209901010101": {
        1: {"finish": 2, "win_payout": 0, "place_payout": 120},
        2: {"finish": 1, "win_payout": 500, "place_payout": 180},
        3: {"finish": 3, "win_payout": 0, "place_payout": 130},
    }}


def test_pre_post_and_immutable(tmp_path):
    pre, post = tmp_path / "pre.json", tmp_path / "post.json"
    sha = m.record_pre(pre, race_date="20990101", generation="cr_fwd_20990101",
                       recorded_at="2099-01-01T08:00:00+09:00", races=[race()])
    assert len(sha) == 64
    with pytest.raises(FileExistsError):
        m.record_pre(pre, race_date="20990101", generation="cr_fwd_20990101",
                     recorded_at="2099-01-01T08:00:00+09:00", races=[race()])
    m.settle_post(pre, post, actual=actual(), finalized_at="2099-01-01T20:00:00+09:00")
    with pytest.raises(FileExistsError):
        m.settle_post(pre, post, actual=actual(), finalized_at="2099-01-01T20:00:00+09:00")
    summary = m.summarize_post([post], generation="cr_fwd_20990101")
    assert summary["races"] == 1
    assert summary["changed"] == 1
    assert summary["production_wins"] == 0
    assert summary["conditional_wins"] == 1
    assert summary["production_places"] == summary["conditional_places"] == 1
    assert summary["production_win_roi"] == 0.0
    assert summary["conditional_win_roi"] == 5.0
    assert summary["production_place_roi"] == 1.2
    assert summary["conditional_place_roi"] == 1.8


def test_missing_and_duplicate_rank_fail(tmp_path):
    r = race()
    r["horses"][1]["conditional_rank"] = 2
    with pytest.raises(ValueError):
        m.record_pre(tmp_path / "pre.json", race_date="20990101",
                     generation="cr_fwd_20990101",
                     recorded_at="2099-01-01T08:00:00+09:00", races=[r])
    assert not (tmp_path / "pre.json").exists()


def test_results_must_match_pre(tmp_path):
    pre = tmp_path / "pre.json"
    m.record_pre(pre, race_date="20990101", generation="cr_fwd_20990101",
                 recorded_at="2099-01-01T08:00:00+09:00", races=[race()])
    with pytest.raises(ValueError):
        m.settle_post(pre, tmp_path / "post.json", actual={}, finalized_at="2099-01-01T20:00:00+09:00")
    assert not (tmp_path / "post.json").exists()


def test_reject_offline_and_cross_generation(tmp_path):
    pre, post = tmp_path / "pre.json", tmp_path / "post.json"
    m.record_pre(pre, race_date="20990101", generation="cr_fwd_20990101",
                 recorded_at="2099-01-01T08:00:00+09:00", races=[race()])
    m.settle_post(pre, post, actual=actual(), finalized_at="2099-01-01T20:00:00+09:00")
    with pytest.raises(ValueError):
        m.summarize_post([post], generation="cr_fwd_20990102")
    with pytest.raises(ValueError):
        m.summarize_post([post, post], generation="cr_fwd_20990101")
    payload = json.loads(post.read_text(encoding="utf-8"))
    payload["scope"] = "offline_integration_replay"
    post.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError):
        m.summarize_post([post], generation="cr_fwd_20990101")
