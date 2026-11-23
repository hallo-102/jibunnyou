"""時点を確認できるローカルマスターだけを、第2ロジックの根拠へ結合する。"""
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from statistics import median
from datetime import datetime, time
import copy
import re
import unicodedata

from storage import compute_sha256
from evaluation_utils import JST, integer, number, parse_date, parse_timestamp, clock_seconds, course_parts

DEFAULT_MASTER = Path(__file__).resolve().parent.parent / 'data/master/race_levels.xlsx'


def _name(value):
    return unicodedata.normalize('NFKC', str(value or '')).replace(' ', '').replace('　', '')


def _rows(sheet):
    iterator = iter(sheet.iter_rows(values_only=True))
    headers = next(iterator, ())
    for row_number, row in enumerate(iterator, 2):
        yield {str(key): value for key, value in zip(headers, row) if key is not None} | {'_row': row_number}


@lru_cache(maxsize=1)
def _load(path, fingerprint):
    import openpyxl
    races, levels, entries, by_horse = {}, {}, defaultdict(list), defaultdict(list)
    with open(path, 'rb') as source:
        wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
        try:
            available = None
            if 'README' in wb.sheetnames:
                for row in wb['README'].iter_rows(values_only=True):
                    for cell in row:
                        match = re.search(r'作成日時[:：]\s*(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})', str(cell))
                        if match:
                            available = parse_timestamp(match[1])
            names = {str(r['id']): r.get('name') for r in _rows(wb['horses'])} if 'horses' in wb.sheetnames else {}
            if 'races' in wb.sheetnames:
                for row in _rows(wb['races']):
                    rid = str(row.get('race_id'))
                    if rid in races:
                        races[rid]['_ambiguous'] = True
                    else:
                        races[rid] = row
            if 'race_levels' in wb.sheetnames:
                for row in _rows(wb['race_levels']):
                    rid = str(row.get('race_id'))
                    if rid in levels:
                        levels[rid]['_ambiguous'] = True
                    else:
                        levels[rid] = row
            if 'entries' in wb.sheetnames:
                for row in _rows(wb['entries']):
                    row['horse_name'] = names.get(str(row.get('horse_id')))
                    entries[str(row.get('race_id'))].append(row)
                    by_horse[str(row.get('horse_id'))].append(row)
            for history in by_horse.values():
                history.sort(key=lambda r: str(r.get('date')))
        finally:
            wb.close()
    return {'path': path, 'sha256': fingerprint, 'available_at': available.isoformat() if available else None,
            'races': races, 'levels': levels, 'entries': dict(entries), 'by_horse': dict(by_horse)}


def load_context(path=DEFAULT_MASTER):
    path = Path(path)
    if not path.exists():
        return {'sha256': None, 'available_at': None, 'warning': '相手評価マスターがありません。第2判定の不足として記録します。'}
    fingerprint = compute_sha256(path)
    context = _load(str(path), fingerprint)
    if compute_sha256(path) != fingerprint:
        raise ValueError('マスターが読込中に変更されました。再読込してください。')
    return context


def enrich_race(race, context, cutoff, observed_at):
    """原本の過去走を変更せず複製へ結合。レースIDだけでなく開催日も照合する。"""
    race = copy.deepcopy(race)
    available = parse_timestamp(context.get('available_at'))
    if available is None or available > cutoff:
        return race
    target_day = parse_date(race.get('date'))
    races, entries = context.get('races', {}), context.get('entries', {})
    # 最新ratingsは使用しない。entriesのpre_ratingだけを相手の発走前評価とする。
    baselines = defaultdict(list)
    for rid, rows in entries.items():
        info = races.get(rid, {})
        day = parse_date(info.get('date'))
        if info.get('_ambiguous') or not day or not target_day or day >= target_day or day > cutoff.date() or day > available.date():
            continue
        clocks = {clock_seconds(r.get('time_sec')) for r in rows if integer(r.get('rank')) == 1}
        clocks.discard(None)
        if len(clocks) == 1:
            key = (info.get('place'), info.get('ground'), integer(info.get('distance')))
            baselines[key].append((next(iter(clocks)), day, rid))
    for horse in race.get('horses', []):
        for past in horse.get('history', []):
            rid = str(past.get('past_race_id'))
            info, level = races.get(rid, {}), context.get('levels', {}).get(rid, {})
            day = parse_date(past.get('date'))
            surface, distance = course_parts(past.get('distance'))
            if (not day or day > available.date() or parse_date(info.get('date')) != day or info.get('_ambiguous')
                    or info.get('ground') != surface or integer(info.get('distance')) != distance):
                continue
            past['place'] = info.get('place')
            past['context_source'] = {'file': context.get('path'), 'sha256': context.get('sha256'),
                                      'available_at': context.get('available_at'), 'race_id': rid}
            rows = entries.get(rid, [])
            own = [r for r in rows if _name(r.get('horse_name')) == _name(horse.get('horse_name'))]
            if len(own) == 1:
                own_id = str(own[0].get('horse_id'))
                opponents = []
                for row in rows:
                    if str(row.get('horse_id')) == own_id:
                        continue
                    opponent = {'horse_name': row.get('horse_name'), 'pre_rating': row.get('pre_rating'),
                                'rating_available_at': context['available_at'],
                                'source_row': row.get('_row'), 'source_sheet': 'entries'}
                    for future in context.get('by_horse', {}).get(str(row.get('horse_id')), []):
                        fday = parse_date(future.get('date'))
                        # 最初の次走が結果未取得でも、その次の好成績へ飛ばさない。
                        if fday and fday > day:
                            if fday > available.date():
                                break
                            frows = entries.get(str(future.get('race_id')), [])
                            opponent['next_start'] = {'date': fday.isoformat(), 'finish': future.get('rank'),
                                                      'field_size': len(frows), 'available_at': context['available_at'],
                                                      'race_id': future.get('race_id'), 'source_row': future.get('_row')}
                            break
                    opponents.append(opponent)
                past['opponents'] = opponents
            winners = {clock_seconds(row.get('time_sec')) for row in rows if integer(row.get('rank')) == 1}
            winners.discard(None)
            if len(winners) == 1:
                past['winner_clock'] = next(iter(winners))
            if not level.get('_ambiguous') and number(level.get('race_level_score')) is not None:
                past['race_level'] = number(level['race_level_score'])
                past['race_level_available_at'] = context['available_at']
            samples = baselines.get((past.get('place'), surface, distance), [])
            if len(samples) >= 3:
                past['baseline'] = {'place': past.get('place'), 'surface': surface, 'distance': distance,
                                    'seconds': median(v[0] for v in samples), 'sample_count': len(samples),
                                    'created_at': observed_at.isoformat(), 'data_cutoff': cutoff.isoformat(),
                                    'source_available_at': context['available_at'], 'source_sha256': context['sha256'],
                                    'source_race_ids': [v[2] for v in samples],
                                    'method': '予想日より前の同競馬場・同芝ダート・同距離の勝ち時計中央値（時点再構成）'}
    return race
