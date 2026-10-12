"""時系列比較の指標と、分割境界の未来情報遮断を検証する。"""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture
def comparison():
    # 比較ツールを読み込むだけで実データの再計算を始めない。
    path = Path(__file__).resolve().parents[1]/'tools/compare_rating_parameters.py'
    spec = importlib.util.spec_from_file_location('test_rating_comparison',path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rating_order_and_tied_top_are_scored_correctly(comparison):
    frame = pd.DataFrame({
        'race_id':['a']*3+['b']*3,'date':['20260601']*6,
        'rank':[1,2,3,1,2,3],'pre_rating':[1600,1500,1400,1600,1600,1400],
    })
    # 正しい順位は相関1、同点トップの勝率は均等選択の0.5にする。
    scores = comparison.per_race_metrics(frame)
    assert scores.loc['a','spearman']==pytest.approx(1)
    assert scores.loc['a','win']==1
    assert scores.loc['b','win']==0.5
    assert set(scores.split)=={'test'}


def test_next_result_crossing_boundary_is_not_used_for_training(comparison):
    dates = ['20251201','20251220','20260110','20260410','20260610','20260710']
    history_rows,level_rows = [],[]
    for index,date in enumerate(dates):
        rid = f'race{index}'
        for horse in [1,2]:
            history_rows.append({'race_id':rid,'horse_id':horse,'date':date,
                                 'rank':horse if index%2 else 3-horse,'pop':horse,
                                 'pre_rating':1500+10*horse,'field_size':2,'class':'未勝利'})
        level_rows.append({'race_id':rid,'pre_top3_mean':1515+index,
                           'pre_top5_mean':1515+index,'pre_mean':1515+index,
                           'pre_p50':1515+index,'result_time_level_adjustment':index})
    frame,result = comparison.compare_levels(pd.DataFrame(history_rows),pd.DataFrame(level_rows))
    # 12月→1月と4月→6月の目的変数は、学習・検証へ混入させない。
    assert set(frame.loc[frame.split=='train','starts'])=={2}
    assert set(frame.loc[frame.split=='validation','starts'])=={2}
    assert set(frame.loc[frame.split=='test','starts'])=={2}
    selected = frame.loc[frame.split=='validation'].sort_values(['mse','candidate']).iloc[0]
    assert result['selected_by_validation']==selected.candidate
