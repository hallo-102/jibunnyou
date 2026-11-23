"""NASの対象日CSVを読み、今走の単勝オッズから推定人気を得る。NASへは書き込まない。"""
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import copy
import csv
import hashlib
import io
import json
import re
import unicodedata

from evaluation_utils import JST, integer, number, parse_date, parse_timestamp

CONFIG = Path(__file__).with_name('odds_source.json')
REQUIRED = ('date', 'racecourse', 'race', 'name', 'bet_type', 'combination', 'odds')


def _name(value):
    return unicodedata.normalize('NFKC', str(value or '')).replace(' ', '').replace('　', '')


def _race_number(value):
    match = re.fullmatch(r'(\d{1,2})(?:R)?', str(value).strip(), re.IGNORECASE)
    return int(match[1]) if match and 1 <= int(match[1]) <= 12 else None


def read_odds_csv(path, target_date, observed_at, archive_dir=None):
    path = Path(path)
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or len(raw) != after.st_size:
        raise ValueError('オッズCSVが読込中に更新されました。再読込してください。')
    # Windows出力のCP932とUTF-8 BOM付きの両方を扱う。
    try:
        content = raw.decode('utf-8-sig')
        encoding = 'utf-8-sig'
    except UnicodeDecodeError:
        content = raw.decode('cp932')
        encoding = 'cp932'
    reader = csv.DictReader(io.StringIO(content, newline=''))
    if not reader.fieldnames or not set(REQUIRED).issubset(reader.fieldnames):
        raise ValueError('オッズCSVの必須列が不足しています：' + ','.join(REQUIRED))
    official_column = next((c for c in ('popularity_rank', 'pop', '人気順位', '人気') if c in reader.fieldnames), None)
    time_column = next((c for c in ('observed_at', 'acquired_at', '取得時刻', '取得日時') if c in reader.fieldnames), None)
    groups = defaultdict(dict)
    errors = defaultdict(list)
    for line, row in enumerate(reader, 2):
        if row.get('bet_type', '').strip() != '単勝':
            continue
        if parse_date(row.get('date')) != target_date:
            raise ValueError(f'CSV {line}行の日付が対象日と一致しません。')
        venue, race_no = row.get('racecourse', '').strip(), _race_number(row.get('race'))
        no, odds = integer(row.get('combination')), number(row.get('odds'))
        key = (venue, race_no)
        if not venue or race_no is None or no is None or no < 1:
            raise ValueError(f'CSV {line}行の競馬場・R・単勝馬番が不正です。')
        stamp = parse_timestamp(row.get(time_column)) if time_column else observed_at
        value = {'horse_no': no, 'horse_name': row.get('name', '').strip(), 'odds': odds,
                 'official_rank': integer(row.get(official_column)) if official_column else None,
                 'observed_at': stamp.isoformat() if stamp else None,
                 'time_source': f'CSVの{time_column}列' if time_column else 'NAS CSV読取時刻（CSV内に取得時刻列なし）',
                 'source_row': line}
        if no in groups[key]:
            previous = groups[key][no]
            if any(previous[k] != value[k] for k in ('horse_name', 'odds', 'official_rank', 'observed_at')):
                errors[key].append(f'{no}番の単勝データが重複・矛盾')
        else:
            groups[key][no] = value
        if odds is None or odds <= 0 or not value['horse_name']:
            errors[key].append(f'{no}番の単勝オッズまたは馬名が不足')
    if not groups:
        raise ValueError('対象日の単勝データがありません。')
    for key, by_number in groups.items():
        rows = list(by_number.values())
        if official_column:
            ranks = [r['official_rank'] for r in rows]
            if any(n is None or n <= 0 or n > len(rows) for n in ranks) or len(set(ranks)) != len(rows):
                errors[key].append('CSVの人気順位が欠損・重複・範囲外')
        for row in rows:
            if official_column:
                low = high = row['official_rank']
            elif row['odds'] is not None and not errors[key]:
                low = 1 + sum(r['odds'] < row['odds'] for r in rows)
                high = sum(r['odds'] <= row['odds'] for r in rows)
            else:
                low = high = None
            row.update(rank_min=low, rank_max=high)
    sha = hashlib.sha256(raw).hexdigest()
    archived = None
    if archive_dir is not None:
        folder = Path(archive_dir)
        folder.mkdir(parents=True, exist_ok=True)
        archived = folder / f'OZZU_{target_date:%Y%m%d}_{sha}.csv'
        try:
            with archived.open('xb') as stream:
                stream.write(raw)
        except FileExistsError:
            if hashlib.sha256(archived.read_bytes()).hexdigest() != sha:
                raise ValueError('保存済みオッズCSVのSHA256が一致しません。')
    return {'path': str(path), 'date': target_date.isoformat(), 'sha256': sha, 'observed_at': observed_at.isoformat(),
            'modified_at': datetime.fromtimestamp(after.st_mtime, JST).isoformat(),
            'archive_path': str(archived) if archived else None, 'encoding': encoding,
            'method': 'CSV記載の人気順位' if official_column else '単勝オッズ昇順の推定人気（同値は順位区間）',
            'groups': dict(groups), 'errors': dict(errors)}


def attach_nas_odds(data, directory=None, archive_dir=None, now=None):
    """Excelの人気へ暗黙フォールバックせず、NAS未取得なら人気を未知にする。"""
    observed = now or datetime.now(JST)
    if observed.tzinfo is None:
        raise ValueError('CSV読取時刻にはタイムゾーンが必要です。')
    observed = observed.astimezone(JST)
    if directory is None:
        directory = json.loads(CONFIG.read_text(encoding='utf-8-sig'))['directory']
    data = copy.deepcopy(data)
    warnings = data.setdefault('warnings', [])
    dates = {parse_date(r.get('date')) for r in data.get('races', [])}
    sources, cache = [], {}
    for day in sorted(d for d in dates if d is not None):
        path = Path(directory) / f'OZZU_{day:%Y%m%d}.csv'
        try:
            source = read_odds_csv(path, day, observed, archive_dir)
        except (OSError, ValueError, UnicodeError, csv.Error) as exc:
            source = {'path': str(path), 'date': day.isoformat(), 'sha256': None,
                      'observed_at': observed.isoformat(), 'error': f'NASオッズ未取得：{exc}', 'groups': {}, 'errors': {}}
        cache[day] = source
        sources.append({k: v for k, v in source.items() if k not in ('groups', 'errors')})
    matched = 0
    for race in data.get('races', []):
        day = parse_date(race.get('date'))
        source = cache.get(day, {'path': str(directory), 'error': '開催日不明のためNAS CSVを選択できません。', 'groups': {}, 'errors': {}})
        key = (str(race.get('place', '')).strip(), integer(race.get('race_no')))
        rows = source['groups'].get(key, {})
        problems = list(source['errors'].get(key, []))
        expected = integer(race.get('field_size'))
        if source.get('error'):
            problems.append(source['error'])
        elif not rows:
            problems.append('対象の競馬場・Rの単勝行がCSVにありません。')
        elif expected is None or expected != len(rows):
            problems.append(f'原本頭数{expected}とCSV単勝頭数{len(rows)}が一致しません。')
        if problems:
            warnings.append(f"{race.get('place')} {race.get('race_no')}R：" + '／'.join(dict.fromkeys(problems)))
        # CSVに明記された今走の馬名・馬番なら、Excelで未取得だった出走馬も取得できる。
        by_name = defaultdict(list)
        for row in rows.values():
            by_name[_name(row['horse_name'])].append(row)
        known_numbers, known_names = set(), set()
        for horse in race.get('horses', []):
            no = integer(horse.get('horse_no'))
            candidates = by_name.get(_name(horse.get('horse_name')), [])
            if no is None and len(candidates) == 1 and not problems:
                no = candidates[0]['horse_no']
                horse['horse_no'] = no
                horse['roster_source'] = {'file': source['path'], 'row': candidates[0]['source_row']}
            known_numbers.add(no)
            known_names.add(_name(horse.get('horse_name')))
        if not problems:
            for no, row in rows.items():
                if no not in known_numbers and _name(row['horse_name']) not in known_names and len(race['horses']) < expected:
                    race['horses'].append({'horse_no': no, 'horse_name': row['horse_name'], 'production_rank': None,
                                           'production_score': None, 'history': [],
                                           'ai_observed_at': observed.isoformat(),
                                           'input_source': {'sheet': None, 'row': None, 'roster_csv_row': row['source_row']}})
        for horse in race.get('horses', []):
            horse['excel_popularity_rank'] = horse.get('popularity_rank')
            horse.update(popularity_rank=None, popularity_rank_min=None, popularity_rank_max=None,
                         popularity_observed_at=observed.isoformat(), popularity_time_source='NAS CSV未照合',
                         popularity_method=source.get('method'), popularity_odds=None,
                         popularity_source={'file': source['path'], 'sha256': source.get('sha256'),
                                            'archive_path': source.get('archive_path'), 'csv_row': None})
            no = integer(horse.get('horse_no'))
            row = rows.get(no)
            error = '／'.join(problems) if problems else None
            if row is None:
                error = error or '対象馬の単勝オッズ行がありません。'
            elif _name(horse.get('horse_name')) != _name(row['horse_name']):
                error = error or 'ExcelとNAS CSVで馬番に対応する馬名が一致しません。'
            if error:
                horse['popularity_error'] = error
                continue
            horse.update(popularity_rank=row['rank_min'], popularity_rank_min=row['rank_min'], popularity_rank_max=row['rank_max'],
                         popularity_observed_at=row['observed_at'], popularity_time_source=row['time_source'],
                         popularity_odds=row['odds'], popularity_rank_interval=True, popularity_error=None)
            horse['popularity_source']['csv_row'] = row['source_row']
            matched += 1
    data['odds_sources'] = sources
    data['odds_matched_horses'] = matched
    # 観測時刻は証跡だが、同一バイト列の再読込で別判定を量産するキーには含めない。
    stable = [{k: s.get(k) for k in ('path', 'date', 'sha256', 'error')} for s in sources]
    data['odds_fingerprint'] = hashlib.sha256(json.dumps(stable, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return data
