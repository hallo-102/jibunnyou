"""Audit the actual committed PR payload, without printing candidate secret values."""
from pathlib import Path
import subprocess
import json
import re
import hashlib

ROOT = Path(__file__).resolve().parents[1]
def git(*args):
    # All commands are read-only; only the selected feature branch is inspected.
    return subprocess.check_output(['git', *args], cwd=ROOT)

names = git('diff', '--name-only', 'origin/main...HEAD').decode().splitlines()
patterns = [r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----', r'\bgh[pousr]_[A-Za-z0-9]{30,}',
            r'\bgithub_pat_[A-Za-z0-9_]{30,}', r'\bsk-(?:proj-)?[A-Za-z0-9_-]{30,}',
            r'\bAKIA[A-Z0-9]{16}\b', r'\bxox[baprs]-[A-Za-z0-9-]{20,}',
            r'(?i)(?:password|client_secret|api_key|access_token)\s*[:=]\s*[\x22\x27][^\x22\x27\n]{12,}[\x22\x27]']
issues, files = [], []
for name in names:
    data = git('show', 'HEAD:' + name)
    files.append({'file': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    if len(data) > 1024 * 1024 or Path(name).suffix.lower() in {'.xlsx', '.csv', '.db', '.sqlite', '.pkl', '.pt', '.pth', '.zip', '.png'}:
        issues.append({'file': name, 'issue': 'data/binary/large-file'})
    if name.startswith(('reports/', 'data/', 'outputs/')):
        issues.append({'file': name, 'issue': 'generated-data-directory'})
    decoded = data.decode('utf-8')
    for number, line in enumerate(decoded.splitlines(), 1):
        if any(re.search(pattern, line) for pattern in patterns):
            issues.append({'file': name, 'line': number, 'issue': 'potential-secret; value suppressed'})
for relative in ('config/production_model.json', 'yosou_py/best_feature_weights_20260730.py', 'yosou_py/best_feature_weights_20260817.py'):
    if git('show', 'HEAD:' + relative) != git('show', 'origin/main:' + relative):
        issues.append({'file': relative, 'issue': 'protected-production-file-change'})
objects = git('rev-list', '--objects', 'origin/feature/conditional-rerank-non-maiden-20261012..HEAD').decode().splitlines()
for entry in objects:
    oid = entry.split()[0]
    kind = git('cat-file', '-t', oid).decode().strip()
    if kind == 'blob' and int(git('cat-file', '-s', oid)) > 1024 * 1024:
        issues.append({'object': oid, 'issue': 'large-blob-in-push-history'})
result = {'head': git('rev-parse', 'HEAD').decode().strip(), 'file_count': len(files),
          'total_changed_bytes': sum(x['bytes'] for x in files), 'files': files, 'issues': issues}
(ROOT / 'reports/pr4_final_review/push_content_audit.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps({k: v for k, v in result.items() if k != 'files'}, ensure_ascii=False))
if issues:
    raise SystemExit(1)
