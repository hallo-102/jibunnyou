"""レーティング生成の重複・未出走・馬場差に対する回帰テスト。"""
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest


@pytest.fixture
def exporter():
    # 数字で始まるスクリプトを、通常のCLIを実行せずに読み込む。
    path = Path(__file__).resolve().parents[1] / 'etc_py/00_Export_To_Excel_4.py'
    spec = importlib.util.spec_from_file_location('test_rating_exporter', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def race_rows(race_id='202605010101', statuses=(1, 2, 3)):
    # 時計基準より一律2秒遅い、少頭数の結果を作る。
    return pd.DataFrame([{
        'race_id': race_id, 'horse_name': f'馬{i}', 'rank': status,
        'time_str': f'1:{42+i:04.1f}' if isinstance(status, int) else None,
        'margin_str': '' if i == 0 else '1/2', 'pop': i+1, 'odds': 2+i,
        'race_name': '未勝利', 'race_info': '東京 10:00発走 芝1600 馬場:良 未勝利',
    } for i, status in enumerate(statuses)])


def process(exporter, monkeypatch, tmp_path, sheets):
    # Excelの読み込み境界だけを差し替え、評価処理本体を通す。
    path = tmp_path / 'input.xlsx'
    path.write_bytes(b'test input placeholder')
    fake_xls = type('FakeExcel', (), {'sheet_names':list(sheets), 'close':lambda self:None})()
    monkeypatch.setattr(exporter, 'open_excelfile_safely', lambda _: (fake_xls, None))
    monkeypatch.setattr(exporter, 'load_place_baba_time_master', lambda _: {('東京','未勝利','良','芝1600'):100.0})
    monkeypatch.setattr(exporter.pd, 'read_excel', lambda _,sheet_name: sheets[sheet_name].copy())
    return exporter.process_excel_to_memory(path)


def test_repeated_result_is_processed_once(exporter, monkeypatch, tmp_path):
    df = race_rows()
    store = process(exporter, monkeypatch, tmp_path, {'20260501':df, '20260502':df})
    assert len(store.race_levels) == 1
    assert len(store.entries) == len(store.ratings_history) == 3
    assert set(store.start_counts.values()) == {1}
    assert store.races['202605010101']['date'] == '20260501'
    assert {r['date'] for r in store.ratings_history} == {'20260501'}


def test_different_result_with_same_id_is_rejected(exporter, monkeypatch, tmp_path):
    original = race_rows()
    conflicting = race_rows(statuses=(2,1,3))
    with pytest.raises(ValueError, match='レースID.*衝突'):
        process(exporter, monkeypatch, tmp_path, {'20260501':original,'20260502':conflicting})


def test_duplicate_with_later_passing_enrichment_is_not_relearned(exporter, monkeypatch, tmp_path):
    original = race_rows()
    enriched = original.copy()
    enriched['passing'] = ['1-1','2-2','3-3']
    # 同じ結果への補足情報の追加は、別の実出走として扱わない。
    store = process(exporter,monkeypatch,tmp_path,{'20260501':original,'20260502':enriched})
    assert len(store.ratings_history)==3
    assert set(store.start_counts.values())=={1}


@pytest.mark.parametrize('status', ['取消','除外'])
def test_non_starter_does_not_change_other_horses(exporter, monkeypatch, tmp_path, status):
    baseline = process(exporter, monkeypatch, tmp_path, {'20260501':race_rows()})
    df = race_rows(statuses=(1,2,3,status))
    actual = process(exporter, monkeypatch, tmp_path, {'20260501':df})
    for name in ['馬0','馬1','馬2']:
        assert actual.ratings[actual.horses[name]] == pytest.approx(baseline.ratings[baseline.horses[name]])
    hid = actual.horses['馬3']
    assert actual.start_counts.get(hid,0) == 0
    assert not any(r['horse_id']==hid for r in actual.ratings_history)
    record = next(r for r in actual.entries if r['horse_id']==hid)
    assert record['status'] == status
    assert record['rating_updated'] is False
    assert actual.race_levels[0]['field_size'] == 3


@pytest.mark.parametrize('status',['中止','失格'])
def test_did_not_finish_is_counted_without_ability_penalty(exporter, monkeypatch, tmp_path, status):
    store = process(exporter, monkeypatch, tmp_path, {'20260501':race_rows(statuses=(1,2,status))})
    hid = store.horses['馬2']
    record = next(r for r in store.ratings_history if r['horse_id']==hid)
    assert store.start_counts[hid] == 1
    assert record['post_rating'] == pytest.approx(record['pre_rating'])
    assert record['adjusted_delta'] == 0
    assert record['rating_updated'] is False
    assert sum(r['adjusted_delta'] for r in store.ratings_history) == pytest.approx(0,abs=1e-9)


def test_track_bias_uses_unadjusted_reference(exporter, monkeypatch, tmp_path):
    sheets = {'20260501':pd.concat([race_rows(f'20260501010{i}') for i in range(1,5)],ignore_index=True)}
    store = process(exporter, monkeypatch, tmp_path, sheets)
    assert [r['day_bias_sec'] for r in store.race_levels] == pytest.approx([0,2,2,2])


def test_empty_horse_groups_do_not_create_phantom_races(exporter, monkeypatch, tmp_path):
    df = race_rows()
    empty = race_rows('202605010199')
    empty['horse_name'] = None
    store = process(exporter, monkeypatch, tmp_path, {'20260501':pd.concat([df,empty])})
    assert len(store.races) == len(store.race_levels) == 1


@pytest.mark.parametrize('values', [
    {'surface_weight':1.1}, {'k_scale':0}, {'performance_time_scale':-1},
    {'level_weights':(0.2,0.2,0.2,0.2)}, {'level_time_scale':float('nan')},
])
def test_invalid_comparison_parameters_are_rejected(exporter, values):
    # 比較ツールから不正な係数が渡されても計算を開始しない。
    with pytest.raises(ValueError):
        exporter.RatingParameters(**values)


def test_surface_weight_is_applied_to_pre_rating(exporter):
    # 条件別の重みを変更したとき、レース前の実効評価も同じ設定を使う。
    store = exporter.MemoryStore(parameters=exporter.RatingParameters(surface_weight=0.25))
    store.ratings[1] = 1500
    store.surface_ratings['芝'][1] = 1600
    assert store.get_combined_rating(1,'芝') == 1525


def test_excel_output_keeps_dates_and_non_starter_status(exporter, monkeypatch, tmp_path):
    store = process(exporter,monkeypatch,tmp_path,{'20260501':race_rows(statuses=(1,2,'取消'))})
    # 保存と再読み込みも通し、追加列と取消の扱いがExcel出力で失われないことを確認する。
    monkeypatch.undo()
    output = tmp_path/'verified.xlsx'
    exporter.write_store_to_excel(store,output,tmp_path/'input.xlsx')
    with pd.ExcelFile(output) as book:
        assert set(book.sheet_names)=={'horses','races','entries','ratings','ratings_history','race_levels','README'}
        history = pd.read_excel(book,sheet_name='ratings_history')
        entries = pd.read_excel(book,sheet_name='entries')
        assert history.date.astype(str).eq('20260501').all()
        cancelled = entries.loc[entries.status=='取消'].iloc[0]
        assert not cancelled.rating_updated
        assert cancelled.horse_id not in history.horse_id.values
