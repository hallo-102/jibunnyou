"""Regression coverage for comparison-only rankings and workbook protection."""
import importlib.util
from pathlib import Path
from copy import copy
import pandas as pd
import pytest
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font, Border, Side

spec = importlib.util.spec_from_file_location('conditional', Path(__file__).parents[1] / '1_keibayosou_conditional_rerank.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

def frames(klass='1勝クラス', scores=None, forms=None):
    # Six runners verify that the sixth runner never enters the original top five.
    target = pd.DataFrame({'rid_str': ['202605040101'] * 6, '馬番': list(range(1, 7)),
                           'rank': list(range(1, 7)), 'score': scores or [80, 77, 76, 73, 70, 79],
                           'recent_form_score': forms or [50, 70, 65, 90, 95, 100]})
    now = target[['rid_str', '馬番']].copy()
    now['クラス'] = klass
    return target, now

@pytest.mark.parametrize('klass', ['未勝利', '新馬', '2歳未勝利', 'メイクデビュー新馬'])
def test_exclusion(klass):
    target, now = frames(klass)
    ranks, audit = m.build_conditional_ranking(target, now)
    assert ranks['条件付き順位'].tolist() == target['rank'].tolist()
    assert audit['変更'].sum() == 0

@pytest.mark.parametrize('score,form,changed', [(76, 70, True), (75.99, 90, False), (79, 59.99, False), (77, 60, False), (77, 60.01, True), (79, 60, True), (float('nan'), 70, False), (77, float('nan'), False)])
def test_boundaries_missing_and_tie(score, form, changed):
    target, now = frames(scores=[80, score, 65, 60, 55, 50], forms=[50, form, 50, 50, 50, 100])
    ranks, audit = m.build_conditional_ranking(target, now)
    assert bool(audit.iloc[0]['変更']) == changed
    assert sorted(ranks['条件付き順位']) == list(range(1, 7))
    assert set(ranks.loc[ranks['条件付き順位'] <= 5, '馬番']) == {1, 2, 3, 4, 5}
    assert target['rank'].tolist() == list(range(1, 7))

def test_multiple_candidates_and_deterministic_tie():
    target, now = frames(scores=[80, 77, 77, 70, 60, 79], forms=[50, 70, 70, 100, 100, 100])
    ranks, audit = m.build_conditional_ranking(target, now)
    assert audit.iloc[0]['条件付き1位馬番'] == 2
    assert ranks['条件付き順位'].tolist() == [2, 1, 3, 4, 5, 6]

def test_missing_leader():
    target, now = frames(forms=[float('nan'), 70, 70, 70, 70, 70])
    ranks, audit = m.build_conditional_ranking(target, now)
    assert audit['変更'].sum() == 0

def test_float_race_id_matches_text_metadata():
    target, now = frames('未勝利')
    # Excel numeric IDs become floats when a column also contains missing cells.
    target['rid_str'] = target['rid_str'].astype(float)
    ranks, audit = m.build_conditional_ranking(target, now)
    assert audit.iloc[0]['理由'] == '未勝利・新馬: 対象外'
    assert ranks['rid_str'].unique().tolist() == ['202605040101']

def test_input_frames_unchanged():
    target, now = frames()
    original, info = target.copy(deep=True), now.copy(deep=True)
    m.build_conditional_ranking(target, now)
    pd.testing.assert_frame_equal(target, original)
    pd.testing.assert_frame_equal(now, info)

def snapshot(wb):
    # Compare actual styles, dimensions and every original cell including formulas.
    result = {}
    for ws in wb:
        result[ws.title] = {'cells': {c.coordinate: (c.value, c.data_type, copy(c.font), copy(c.fill), copy(c.border), copy(c.alignment), c.number_format, copy(c.protection)) for row in ws for c in row},
                            'columns': {k: (v.width, v.hidden) for k, v in ws.column_dimensions.items()},
                            'rows': {k: (v.height, v.hidden) for k, v in ws.row_dimensions.items()},
                            'merged': str(ws.merged_cells), 'freeze': ws.freeze_panes}
    return result

def assert_preserved(before, after):
    for name, sheet in before.items():
        assert name in after
        for coordinate, value in sheet['cells'].items():
            assert after[name]['cells'][coordinate] == value, (name, coordinate)
        for kind in ('columns', 'rows', 'merged', 'freeze'):
            assert after[name][kind] == sheet[kind], (name, kind)

def test_excel_protection_and_idempotence(tmp_path):
    target, now = frames()
    path = tmp_path / 'fixture.xlsx'
    wb = Workbook()
    wb.remove(wb.active)
    for name, df in [('TARGET', target), ('今走レース情報', now)]:
        ws = wb.create_sheet(name)
        ws.append(df.columns.tolist())
        for values in df.itertuples(index=False, name=None):
            ws.append(values)
        ws['A2'].fill = PatternFill('solid', fgColor='FFFF0000')
        ws['A2'].font = Font(color='FF00FF00', bold=True)
        ws['A2'].border = Border(left=Side(style='thin', color='FF000000'))
        ws['A2'].number_format = '0'
        ws.column_dimensions['A'].width = 25
        ws.row_dimensions[2].height = 30
        ws.freeze_panes = 'B2'
    bets = wb.create_sheet('買い目候補')
    bets['A1'] = '=SUM(1,2)'
    bets['B1'] = '1-2-3'
    wb.save(path)
    before = snapshot(load_workbook(path))
    m.append_conditional_ranking_sheet(str(path))
    once = snapshot(load_workbook(path))
    assert_preserved(before, once)
    m.append_conditional_ranking_sheet(str(path))
    twice = snapshot(load_workbook(path))
    assert once == twice

def test_unmatched_excel_aborts_without_save(tmp_path):
    target, now = frames()
    now.loc[0, '馬番'] = 99
    path = tmp_path / 'invalid.xlsx'
    with pd.ExcelWriter(path) as writer:
        target.to_excel(writer, sheet_name='TARGET', index=False)
        now.to_excel(writer, sheet_name='今走レース情報', index=False)
    original = path.read_bytes()
    with pytest.raises(ValueError, match='照合'):
        m.append_conditional_ranking_sheet(str(path))
    assert path.read_bytes() == original

def test_runner_guard_blocks_external_writes_and_actions(tmp_path):
    safety_spec = importlib.util.spec_from_file_location('validation_guard', Path(__file__).parents[1] / 'tools/final_validation_runner.py')
    safety = importlib.util.module_from_spec(safety_spec)
    safety_spec.loader.exec_module(safety)
    safety.RUN = tmp_path / 'isolated'
    # Check production paths and symlink-resolved boundaries before any filesystem write.
    with pytest.raises(PermissionError, match='書込禁止'):
        safety.guard('open', (str(safety.PROD / 'protected.xlsx'), 'w', 0))
    for event in ('socket.connect', 'subprocess.Popen', 'os.system', 'os.startfile'):
        with pytest.raises(PermissionError, match='外部処理禁止'):
            safety.guard(event, ())
    with pytest.raises(PermissionError, match='変更禁止'):
        safety.guard('os.rename', (str(safety.RUN / 'output.xlsx'), str(safety.PROD / 'output.xlsx')))
    safety.guard('open', (str(safety.RUN / 'output.xlsx'), 'w', 0))
