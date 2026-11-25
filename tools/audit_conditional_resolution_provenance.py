"""Record Git-backed causes, immutable production files and reviewed proposal differences."""
from pathlib import Path
import subprocess
import hashlib
import json
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/merge_blocked_resolution_20261013'

def git_bytes(reference):
    # Read an exact Git blob; no checkout, ref movement or production mutation.
    return subprocess.check_output(['git', 'show', reference], cwd=ROOT)

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def blob(raw):
    return hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()

old_forward = json.loads((OUT / 'forward_validation.json').read_text(encoding='utf-8'))
new_forward = json.loads((ROOT / 'config/forward_validation.json').read_text(encoding='utf-8'))
old_shadow = json.loads((OUT / 'shadow_model.json').read_text(encoding='utf-8'))
new_shadow = json.loads((ROOT / 'config/shadow_model.json').read_text(encoding='utf-8'))
old_production = git_bytes('265e0a4^:config/production_model.json')
promoted_production = git_bytes('265e0a4:config/production_model.json')
weight_git = git_bytes('f5293ef:yosou_py/best_feature_weights_20260817.py')
current_weight = (ROOT / 'yosou_py/best_feature_weights_20260817.py').read_bytes()
protected = []
for relative in ('config/production_model.json', 'yosou_py/best_feature_weights_20260730.py', 'yosou_py/best_feature_weights_20260817.py'):
    local = (ROOT / relative).read_bytes()
    production = (ROOT.parent / 'keiba_yosou_2026-2' / relative).read_bytes()
    tracked = git_bytes('265e0a4:' + relative)
    protected.append({'file': relative, 'local_sha256': digest(local), 'production_sha256': digest(production), 'main_git_sha256': digest(tracked), 'unchanged': local == production == tracked})
result = {'timestamp_jst': datetime.now().isoformat(),
          'production_promotion': {'commit': '265e0a4', 'before_blob': blob(old_production), 'after_blob': blob(promoted_production), 'before_model': json.loads(old_production)['model_name'], 'after_model': json.loads(promoted_production)['model_name']},
          'runner': {'before_blob': blob(git_bytes('265e0a4:1_keibayosou_best_import_roi_runner.py')), 'pr_blob': blob(git_bytes('d7d779b:1_keibayosou_best_import_roi_runner.py')), 'local_equals_pr': (ROOT / '1_keibayosou_best_import_roi_runner.py').read_bytes() == git_bytes('d7d779b:1_keibayosou_best_import_roi_runner.py')},
          'shadow_line_endings': {'lf_policy_commit': 'd66c425', 'original_git_weight_equals_current': weight_git == current_weight, 'git_sha256': digest(weight_git), 'lf_sha256': digest(current_weight), 'crlf_sha256': digest(current_weight.replace(b'\n', b'\r\n')), 'changes': {key: [old_shadow.get(key), new_shadow.get(key)] for key in set(old_shadow) | set(new_shadow) if old_shadow.get(key) != new_shadow.get(key)}},
          'historical_generations_unchanged': all(new_forward['generations'][key] == value for key, value in old_forward['generations'].items()),
          'protected_files': protected,
          'active_generation_before': old_forward['active_generation'], 'active_generation_proposal': new_forward['active_generation']}
assert all(item['unchanged'] for item in protected)
assert result['historical_generations_unchanged']
assert result['shadow_line_endings']['original_git_weight_equals_current']
(OUT / 'provenance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(result, ensure_ascii=False, indent=2))
