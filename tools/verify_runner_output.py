"""Compare supplementary runner output against its paired baseline and saved production."""
from pathlib import Path
import json
import sys
import shutil
import pandas as pd
from verify_final_workbooks import ROOT, compare, oracle, m

run = Path(sys.argv[1]).resolve()
output = run / 'data/output/馬の競走成績_with_feat_20261012.xlsx'
baseline = run / 'before_conditional.xlsx'
production = ROOT.parent / 'keiba_yosou_2026-2/data/output' / output.name
paired = compare(baseline, output)
stats = oracle(output)
fixed = run / 'fixed_rerank_output.xlsx'
shutil.copy2(output, fixed)
# Reapply the final repaired module to a fresh copy, leaving the runner output intact.
m.append_conditional_ranking_sheet(str(fixed))
fixed_issues = compare(output, fixed)

def keyed(path):
    target = pd.read_excel(path, sheet_name='TARGET')
    keys = ['rid_str', '馬番']
    cols = [c for c in ('rank', 'score', 'score_raw', 'total') if c in target]
    return target.set_index(keys)[cols].sort_index()

old, new = keyed(production), keyed(output)
rank_score_issues = []
if not old.index.equals(new.index):
    rank_score_issues.append('race/horse universe differs')
else:
    for column in old:
        left, right = old[column], new[column]
        bad = ~(left.eq(right) | (left.isna() & right.isna()))
        if bad.any():
            rank_score_issues.append({'column': column, 'differences': int(bad.sum())})
bet_issues = []
for name in ('買い目_レース別1行', '回収率重視_買い目候補'):
    try:
        pd.testing.assert_frame_equal(pd.read_excel(production, sheet_name=name), pd.read_excel(output, sheet_name=name), check_dtype=False)
    except (AssertionError, ValueError) as error:
        bet_issues.append({'sheet': name, 'difference': str(error)[:800]})
result = {'run': str(run), 'paired_issues': paired, 'fixed_reapplication_issues': fixed_issues,
          'oracle': stats, 'production_rank_score_issues': rank_score_issues, 'production_bet_issues': bet_issues}
# Optional destination keeps prior validation evidence intact across additional runs.
results_path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else ROOT / 'reports/runner_output_results.json'
results_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps({**result, 'paired_issues': len(paired), 'fixed_reapplication_issues': len(fixed_issues)}, ensure_ascii=False, indent=2))
