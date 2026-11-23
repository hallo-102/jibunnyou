"""当日予想Excelへ、GUIと同じ固定判定を追記する無人実行入口。"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED
from lxml import etree as ET

from evaluation_utils import JST, race_start
from excel_loader import load_races_from_xlsx
from excel_export import export_judgements
from independent import analyze_race, load_policy, policy_version
from odds_loader import attach_nas_odds
from research_context import load_context, enrich_race
from storage import EvaluationStore, compute_sha256
from validation import build_evaluation_record

BASE = Path(__file__).resolve().parent
DEFAULT_DIRECTORY = Path(r'\\192.168.1.92\docker\keiba_yosou_2026_nas\data\output')
MAIN = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL = 'http://schemas.openxmlformats.org/package/2006/relationships'
DOCREL = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
CT = 'http://schemas.openxmlformats.org/package/2006/content-types'
SHEETS = ('判定一覧', '判定馬別詳細', '判定出力情報')


def xml_bytes(root):
    return ET.tostring(root, encoding='utf-8', xml_declaration=True)


def append_report(source, report, destination):
    """原本のシートXML・数式キャッシュ・画像等をそのまま保持して追記する。"""
    with ZipFile(source) as original, ZipFile(report) as generated:
        workbook = ET.fromstring(original.read('xl/workbook.xml'))
        sheets = workbook.find(f'{{{MAIN}}}sheets')
        if set(SHEETS) & {s.get('name') for s in sheets}:
            raise ValueError('判定シートが既に存在します。自動上書きは行いません。')
        relationships = ET.fromstring(original.read('xl/_rels/workbook.xml.rels'))
        content_types = ET.fromstring(original.read('[Content_Types].xml'))
        styles = ET.fromstring(original.read('xl/styles.xml'))
        fonts = styles.find(f'{{{MAIN}}}fonts')
        fills = styles.find(f'{{{MAIN}}}fills')
        xfs = styles.find(f'{{{MAIN}}}cellXfs')
        # 原本のスタイル番号を変更せず、追記シート専用の5スタイルだけを末尾へ追加する。
        font_id = len(fonts)
        font = ET.SubElement(fonts, f'{{{MAIN}}}font')
        ET.SubElement(font, f'{{{MAIN}}}name', val='Meiryo')
        ET.SubElement(font, f'{{{MAIN}}}sz', val='10')
        fonts.set('count', str(len(fonts)))
        style_ids = []
        for color in ('FFFFFF', 'DCE6F1', 'E0F1E7', 'FBE7E7', 'FFF4D9'):
            fill_id = len(fills)
            fill = ET.SubElement(fills, f'{{{MAIN}}}fill')
            pattern = ET.SubElement(fill, f'{{{MAIN}}}patternFill', patternType='solid')
            ET.SubElement(pattern, f'{{{MAIN}}}fgColor', rgb='FF' + color)
            style_ids.append(len(xfs))
            xf = ET.SubElement(xfs, f'{{{MAIN}}}xf', numFmtId='0', fontId=str(font_id),
                               fillId=str(fill_id), borderId='0', xfId='0', applyAlignment='1')
            ET.SubElement(xf, f'{{{MAIN}}}alignment', vertical='top', wrapText='1')
        fills.set('count', str(len(fills)))
        xfs.set('count', str(len(xfs)))
        changes = {}
        next_sheet_id = max(int(s.get('sheetId')) for s in sheets) + 1
        used_rids = {r.get('Id') for r in relationships}
        used_paths = set(original.namelist())
        for index, name in enumerate(SHEETS, 1):
            number = index
            while f'xl/worksheets/sheet{number}.xml' in used_paths:
                number += 1
            part = f'xl/worksheets/sheet{number}.xml'
            used_paths.add(part)
            rid = f'rIdJudgement{index}'
            while rid in used_rids:
                rid += '_'
            used_rids.add(rid)
            ET.SubElement(sheets, f'{{{MAIN}}}sheet', name=name, sheetId=str(next_sheet_id),
                          attrib={f'{{{DOCREL}}}id': rid})
            next_sheet_id += 1
            ET.SubElement(relationships, f'{{{REL}}}Relationship', Id=rid,
                          Type=DOCREL + '/worksheet', Target=part.removeprefix('xl/'))
            ET.SubElement(content_types, f'{{{CT}}}Override', PartName='/' + part,
                          ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml')
            sheet = ET.fromstring(generated.read(f'xl/worksheets/sheet{index}.xml'))
            for row in sheet.findall(f'.//{{{MAIN}}}sheetData/{{{MAIN}}}row'):
                row_no = int(row.get('r'))
                style = style_ids[1] if row_no <= 4 else style_ids[0]
                if index == 1 and row_no >= 5:
                    status = next((c.find(f'{{{MAIN}}}is/{{{MAIN}}}t').text
                                   for c in row if c.get('r') == f'H{row_no}'), '')
                    style = {'購入候補': style_ids[2], '見送り候補': style_ids[3],
                             '判定保留': style_ids[4]}.get(status, style)
                for cell in row:
                    cell.set('s', str(style))
            changes[part] = xml_bytes(sheet)
        changes.update({'xl/workbook.xml': xml_bytes(workbook),
                        'xl/_rels/workbook.xml.rels': xml_bytes(relationships),
                        '[Content_Types].xml': xml_bytes(content_types),
                        'xl/styles.xml': xml_bytes(styles)})
        with ZipFile(destination, 'w', ZIP_DEFLATED) as output:
            for item in original.infolist():
                output.writestr(item, changes.pop(item.filename, original.read(item.filename)))
            for name, contents in changes.items():
                output.writestr(name, contents)


def run_daily(day=None, directory=DEFAULT_DIRECTORY, state_dir=None):
    day = day or datetime.now(JST).date()
    state_dir = Path(state_dir or BASE / 'data' / 'daily_judgement')
    state_dir.mkdir(parents=True, exist_ok=True)
    source = Path(directory) / f'馬の競走成績_with_feat_{day:%Y%m%d}.xlsx'
    if not source.parent.is_dir():
        raise OSError(f'NASフォルダに接続できません：{source.parent}')
    if not source.is_file():
        return {'status': 'skipped', 'reason': '当日分の予想Excelなし', 'path': str(source)}
    # 排他的なファイル作成で同時実行を防止する。異常終了で残った場合はログを確認して削除する。
    lock = state_dir / f'{day:%Y%m%d}.lock'
    with lock.open('x', encoding='utf-8') as handle:
        handle.write(str(os.getpid()))
    try:
        current_sha = compute_sha256(source)
        manifest = state_dir / f'{day:%Y%m%d}.json'
        previous = json.loads(manifest.read_text(encoding='utf-8')) if manifest.exists() else None
        if previous and previous.get('output_sha256') == current_sha:
            return dict(previous, status='already_done')
        # 判定追記前の原本を内容ハッシュ別に保存し、再実行時の入力改変を防ぐ。
        backup = state_dir / 'originals' / current_sha / source.name
        backup.parent.mkdir(parents=True, exist_ok=True)
        if not backup.exists():
            shutil.copy2(source, backup)
        if compute_sha256(backup) != current_sha:
            raise RuntimeError('Excelが読取中に更新されました。再実行してください。')
        with ZipFile(backup) as archive:
            root = ET.fromstring(archive.read('xl/workbook.xml'))
            if set(SHEETS) & {s.get('name') for s in root.find(f'{{{MAIN}}}sheets')}:
                raise ValueError('既存判定シートと実行記録が一致しません。原本は変更しません。')
        data = load_races_from_xlsx(str(backup))
        data = attach_nas_odds(data, archive_dir=str(BASE / 'data' / 'odds_snapshots'))
        if not data['races']:
            raise ValueError('対象レースがありません。')
        if any(str(r.get('date')) != day.isoformat() for r in data['races']):
            raise ValueError('ファイル名と開催日が一致しません。')
        context, policy = load_context(), load_policy()
        version = policy_version(policy)
        fingerprint = hashlib.sha256((data['sha256'] + str(context.get('sha256')) + version
                                      + data.get('odds_fingerprint', '')).encode()).hexdigest()
        store = EvaluationStore(str(BASE / 'data' / 'evaluations.jsonl'))
        stored = {(str(e.get('race_key') or e.get('race_id')), e.get('input_fingerprint') or e.get('sha256'),
                   e.get('condition_version')): e for e in store.load_all()}
        records = {}
        for race in data['races']:
            rid = str(race.get('race_key') or race.get('race_id'))
            record = stored.get((rid, fingerprint, version))
            if record is None:
                now = datetime.now(JST)
                start = race_start(race)
                enriched = enrich_race(race, context, min(now, start) if start else now, now)
                analysis = analyze_race(enriched, as_of=now, policy=policy, context_sha256=context.get('sha256'))
                analysis.update(input_fingerprint=fingerprint, odds_sources=data.get('odds_sources', []))
                record = store.get_or_append(build_evaluation_record(race, analysis, source.name, current_sha))
            records[rid] = record
        with tempfile.TemporaryDirectory() as work:
            report = Path(work) / 'report.xlsx'
            export_judgements(report, data['races'], records, source_file=source.name, source_path=str(source))
            # 同一NASフォルダ内で一時保存・検査後に置換し、通信失敗で原本を部分上書きしない。
            fd, temporary = tempfile.mkstemp(prefix='.judgement-', suffix='.xlsx', dir=source.parent)
            os.close(fd)
            try:
                append_report(backup, report, temporary)
                with ZipFile(temporary) as archive:
                    if archive.testzip() is not None:
                        raise ValueError('出力ExcelのZIP検査に失敗しました。')
                output_sha = compute_sha256(temporary)
                if compute_sha256(source) != current_sha:
                    raise RuntimeError('処理中に予想Excelが更新されました。原本は変更しません。')
                os.replace(temporary, source)
            finally:
                Path(temporary).unlink(missing_ok=True)
        result = {'status': 'completed', 'path': str(source), 'backup': str(backup),
                  'source_sha256': current_sha, 'output_sha256': output_sha,
                  'completed_at': datetime.now(JST).isoformat(), 'race_count': len(records),
                  'counts': dict(Counter(r['analysis']['judgement'] for r in records.values())),
                  'warnings': data.get('warnings', []),
                  'evaluation_ids': [r['evaluation_id'] for r in records.values()]}
        temporary_manifest = manifest.with_suffix('.tmp')
        temporary_manifest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(temporary_manifest, manifest)
        return result
    finally:
        lock.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', help='手動検証用の対象日 YYYYMMDD。省略時は日本時間の当日。')
    parser.add_argument('--directory', type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument('--state-dir', type=Path)
    args = parser.parse_args()
    log_dir = BASE / 'data' / 'daily_judgement' / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    code = 0
    try:
        day = datetime.strptime(args.date, '%Y%m%d').date() if args.date else None
        result = run_daily(day, args.directory, args.state_dir)
    except Exception as exc:
        result, code = {'status': 'failed', 'error': str(exc)}, 1
    result['logged_at'] = datetime.now(JST).isoformat()
    text = json.dumps(result, ensure_ascii=False, indent=2)
    with (log_dir / f'{datetime.now(JST):%Y%m%d}.jsonl').open('a', encoding='utf-8') as handle:
        handle.write(json.dumps(result, ensure_ascii=False) + '\n')
    print(text)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
