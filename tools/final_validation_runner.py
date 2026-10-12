"""Run the unchanged runner with filesystem/network guards and isolated copies."""
from pathlib import Path
import sys
import os
import shutil
import importlib
import json
import hashlib
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
PROD = ROOT.parent / 'keiba_yosou_2026-2'
RUN = ROOT / 'reports' / datetime.now().strftime('isolated_%Y%m%d_%H%M%S')

def sha(path):
    # Hash without modifying the source.
    return hashlib.sha256(path.read_bytes()).hexdigest()

def prepare():
    # Preserve source/config files byte-for-byte; all generated files stay in RUN.
    RUN.mkdir(parents=True)
    for source in ROOT.glob('*.py'):
        shutil.copy2(source, RUN / source.name)
    for name in ('config', 'yosou_py', 'tokutyouryou_keisann', 'lap'):
        shutil.copytree(ROOT / name, RUN / name, ignore=shutil.ignore_patterns('__pycache__'))
    sources = [PROD / 'data/input/馬の競走成績_20261012.xlsx',
               PROD / 'data/master/race_levels.xlsx',
               PROD / 'data/master/場所_馬場_タイム.xlsx',
               PROD / 'data/master/racedata_results.xlsx',
               PROD / 'data/ozzu_csv/OZZU_20261012.csv']
    # Historical predictions are necessary for the unchanged estimated-place-rate logic.
    sources += list((PROD / 'data/output').glob('馬の競走成績_with_feat_*.xlsx'))
    manifest = []
    for source in sources:
        dest = RUN / source.relative_to(PROD)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
        manifest.append({'source': str(source), 'sha256': sha(source), 'copy_sha256': sha(dest)})
    # Move prediction history away from the new output so no copied Excel is overwritten.
    (RUN / 'data/output').rename(RUN / 'data/history')
    (RUN / 'data/output').mkdir()
    (RUN / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    return manifest

def guard(event, args):
    # Reject external writes, process execution and all network access.
    if event == 'open':
        path, mode, flags = args
        if isinstance(path, (str, bytes, os.PathLike)) and ((isinstance(mode, str) and any(c in mode for c in 'wax+')) or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC)):
            if not Path(path).resolve().is_relative_to(RUN):
                raise PermissionError(f'外部書込禁止: {path}')
    if event in ('os.remove', 'os.rmdir', 'os.mkdir', 'os.rename'):
        for path in args[:2] if event == 'os.rename' else args[:1]:
            if isinstance(path, (str, bytes, os.PathLike)) and not Path(path).resolve().is_relative_to(RUN):
                raise PermissionError(f'外部変更禁止: {path}')
    if event.startswith('socket.') or event in ('subprocess.Popen', 'os.system', 'os.startfile'):
        raise PermissionError(f'外部処理禁止: {event}')

if __name__ == '__main__':
    supplemental = '--no-shadow' in sys.argv
    manifest = prepare()
    print(f'ISOLATED_ROOT={RUN}', flush=True)
    sys.path.insert(0, str(RUN))
    sys.dont_write_bytecode = True
    os.chdir(RUN)
    os.environ['TEMP'] = os.environ['TMP'] = str(RUN)
    sys.addaudithook(guard)
    runner = importlib.import_module('1_keibayosou_best_import_roi_runner')
    # Read exactly the copied history, excluding newly generated predictions.
    original_loader = runner._load_historical_prediction_df
    def history_loader(*args, **kwargs):
        original = runner.OUTPUT_DIR
        try:
            runner.OUTPUT_DIR = RUN / 'data/history'
            # Translate exclusions after moving history, so today's saved forecast stays excluded.
            excluded = kwargs.get('exclude_paths')
            if excluded is not None:
                kwargs['exclude_paths'] = [RUN / 'data/history' / Path(p).name for p in excluded]
            return original_loader(*args, **kwargs)
        finally:
            runner.OUTPUT_DIR = original
    runner._load_historical_prediction_df = history_loader
    if supplemental:
        # Explicit supplementary mode: this cannot count as an unchanged runner pass.
        pipeline = runner.run_pipeline
        def without_shadow(*args, **kwargs):
            kwargs['ENABLE_SHADOW'] = False
            return pipeline(*args, **kwargs)
        runner.run_pipeline = without_shadow
        print('SUPPLEMENTAL: ENABLE_SHADOW=False; all model files/settings unchanged', flush=True)
    conditional = importlib.import_module('1_keibayosou_conditional_rerank')
    original_append = conditional.append_conditional_ranking_sheet
    def capture(path):
        # Capture the actual pre-addition workbook for exact delta comparison.
        shutil.copy2(path, RUN / 'before_conditional.xlsx')
        return original_append(path)
    conditional.append_conditional_ranking_sheet = capture
    original_pace = runner.append_pace_prediction_sheet_to_excel
    def paired_pace(path):
        # Apply the identical pace operation to the pre-addition baseline as well.
        baseline = RUN / 'before_conditional.xlsx'
        original_pace(str(baseline))
        return original_pace(path)
    runner.append_pace_prediction_sheet_to_excel = paired_pace
    sys.argv = ['1_keibayosou_best_import_roi_runner.py', '--raceday', '20261012']
    try:
        runner.main()
    finally:
        checks = [{'source': item['source'], 'unchanged': sha(Path(item['source'])) == item['sha256']} for item in manifest]
        (RUN / 'production_unchanged.json').write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding='utf-8')
