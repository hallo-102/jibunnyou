"""予想時点の固定保存と、購入券の有無によらないTOP5捕捉率比較。"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from uuid import uuid4
import re

from independent import VERSION, integer, parse_date

JST = timezone(timedelta(hours=9))


def build_evaluation_record(race, analysis, source_file, sha256, now=None):
    now = now or datetime.now(JST)
    if now.tzinfo is None:
        raise ValueError("保存時刻にはタイムゾーンが必要です。")
    now = now.astimezone(JST)
    day, start = parse_date(race.get("date")), str(race.get("start_time", ""))
    scheduled = None
    if day is not None:
        for fmt in ("%H:%M", "%H:%M:%S"):
            try:
                scheduled = datetime.combine(day, datetime.strptime(start, fmt).time(), JST)
                break
            except ValueError:
                continue
    return {
        "evaluation_id": str(uuid4()), "recorded_at": now.isoformat(timespec="seconds"),
        "race_id": str(race.get("race_id")), "race_name": race.get("race_name"),
        "race_key": str(race.get("race_key") or race.get("race_id")),
        "date": race.get("date"), "place": race.get("place"), "race_no": race.get("race_no"),
        "source_file": source_file, "sha256": sha256, "condition_version": analysis.get('version', VERSION),
        "input_fingerprint": analysis.get('input_fingerprint'),
        "context_sha256": analysis.get('context_sha256'),
        "virtual_tickets": race.get('virtual_tickets'),
        "scheduled_start": scheduled.isoformat() if scheduled else None,
        "is_prospective": bool(scheduled and now < scheduled and race.get("race_id") not in (None, "", "不明")),
        "analysis": analysis,
    }


def finish_numbers(values):
    if not isinstance(values, (list, tuple)) or len(values) != 3:
        return None
    numbers = [integer(v) for v in values]
    if any(n is None or n <= 0 for n in numbers) or len(set(numbers)) != 3:
        return None
    return numbers


def parse_finish_text(text):
    # 不正な文字を読み飛ばして「正しい3頭」と扱わない。
    parts = re.split(r"[,、\s]+", str(text or "").strip())
    return finish_numbers(parts)


def _capture(top5, finish):
    if not isinstance(top5, list) or not top5:
        return None
    numbers = [integer(n) for n in top5]
    if any(n is None or n <= 0 for n in numbers) or len(set(numbers)) != len(numbers):
        return None
    return sum(n in numbers for n in finish)


def _combo_key(kind, combination):
    parts = str(combination).split('-')
    numbers = [integer(p) for p in parts]
    if any(n is None or n < 1 for n in numbers) or len(set(numbers)) != len(numbers):
        return None
    if kind in ('馬連', 'ワイド', '3連複'):
        numbers.sort()
    return kind, tuple(numbers)


def parse_virtual_prices(text):
    import csv
    values = []
    for row in csv.reader(str(text or '').splitlines()):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) != 3:
            raise ValueError('仮想払戻は 券種,組み合わせ,100円払戻額 の3列で入力してください。')
        kind, combo, raw = (cell.strip() for cell in row)
        amount = integer(raw)
        sizes = {'単勝': 1, '複勝': 1, '馬連': 2, 'ワイド': 2, '馬単': 2, '3連複': 3, '3連単': 3}
        key = _combo_key(kind, combo)
        if kind not in sizes or key is None or len(key[1]) != sizes[kind] or amount is None or amount < 0:
            raise ValueError('仮想払戻の券種・組み合わせ・100円払戻額が不正です。')
        values.append({'ticket_type': kind, 'combination': combo, 'payout_per_100': amount})
    return values


def _virtual_money(evaluation, finish, results):
    from core import check_ticket_hit
    plans = evaluation.get('virtual_tickets')
    if not plans:
        return {'investment': None, 'payout': None, 'reason': '事前の仮想買い目・金額が未取得'}
    for ticket in plans:
        amount = integer(ticket.get('amount'))
        if (amount is None or amount <= 0 or amount % 100 or ticket.get('ticket_type') == '枠連'
                or _combo_key(ticket.get('ticket_type'), ticket.get('combination')) is None):
            return {'investment': None, 'payout': None, 'reason': '事前買い目・金額の形式が不正／枠連未対応'}
    prices = {}
    for result in results:
        for price in result.get('virtual_payouts', []):
            key = _combo_key(price.get('ticket_type'), price.get('combination'))
            value = integer(price.get('payout_per_100'))
            if key and value is not None and value >= 0:
                prices.setdefault(key, set()).add(value)
    investment, payout = sum(integer(t['amount']) for t in plans), 0
    for ticket in plans:
        kind, combo, amount = ticket.get('ticket_type'), ticket.get('combination'), integer(ticket.get('amount'))
        key = _combo_key(kind, combo)
        if amount is None or amount <= 0 or amount % 100 or key is None or kind == '枠連':
            return {'investment': None, 'payout': None, 'reason': '事前買い目・金額の形式が不正／枠連未対応'}
        hit = check_ticket_hit(kind, combo, finish)
        if hit is None:
            return {'investment': investment, 'payout': None, 'reason': '券種の的中照合が未対応'}
        if hit:
            values = prices.get(key, set())
            if len(values) != 1:
                return {'investment': investment, 'payout': None,
                        'reason': '的中買い目の100円払戻額が不足・矛盾'}
            payout += next(iter(values)) * (amount // 100)
    return {'investment': investment, 'payout': payout, 'reason': None}


def _group_stats(captures):
    stats = {'race_count': len(captures)}
    for idx, label in enumerate(('production', 'independent')):
        known = [pair[idx] for pair in captures if pair[idx] is not None]
        stats[label] = {'recorded_races': len(known), 'captured': sum(known), 'slots': len(known) * 3,
                        'rate': sum(known) / (len(known) * 3) * 100 if known else None}
    money = [pair[2] for pair in captures]
    investment = sum(m['investment'] for m in money) if money and all(m['investment'] is not None for m in money) else None
    payout = sum(m['payout'] for m in money) if money and all(m['payout'] is not None for m in money) else None
    stats['virtual'] = {'investment': investment, 'payout': payout,
                        'profit': payout - investment if payout is not None and investment is not None else None,
                        'recovery_rate': payout / investment * 100 if payout is not None and investment else None,
                        'missing_races': sum(m['investment'] is None or m['payout'] is None for m in money)}
    return stats


def aggregate_capture(evaluations, results):
    """明示された保存IDだけに結合。事後保存・着順矛盾・重複レースは除外する。"""
    buckets = {name: [] for name in ("全体", "購入候補", "見送り候補", "判定保留")}
    excluded = {"事後・時刻不明": 0, "結果未入力": 0, "結果矛盾": 0,
                "保存IDなし・不明": 0, "複数の事前保存": 0}
    by_id = {r["evaluation_id"]: r for r in evaluations if r.get("evaluation_id")}
    results_by_id, rows_by_id = {}, {}
    for result in results:
        eid = result.get("evaluation_id")
        if eid not in by_id:
            excluded["保存IDなし・不明"] += 1
            continue
        rows_by_id.setdefault(eid, []).append(result)
        # 券だけの追記は着順を空にせず、同じIDの確定着順記録と合流させる。
        finish = finish_numbers(result.get("finish_top3"))
        if finish:
            if str(result.get("race_id")) != str(by_id[eid].get("race_id")):
                results_by_id.setdefault(eid, set()).add(("矛盾",))
            else:
                results_by_id.setdefault(eid, set()).add(tuple(finish))
    eligible = [e for e in evaluations if e.get("is_prospective")]
    # 同一レースで異なる原本・方式の事前保存がある場合、良い方を選ばない。
    counts = {}
    for e in eligible:
        key = (e.get("condition_version"), str(e.get("race_id")))
        counts[key] = counts.get(key, 0) + 1
    versions, second_versions = {}, {}
    for e in evaluations:
        if not e.get("is_prospective"):
            excluded["事後・時刻不明"] += 1
            continue
        if counts[(e.get("condition_version"), str(e.get("race_id")))] > 1:
            excluded["複数の事前保存"] += 1
            continue
        finishes = results_by_id.get(e.get("evaluation_id"), set())
        if not finishes:
            excluded["結果未入力"] += 1
            continue
        if len(finishes) != 1 or ("矛盾",) in finishes:
            excluded["結果矛盾"] += 1
            continue
        finish = list(next(iter(finishes)))
        analysis = e["analysis"]
        money = _virtual_money(e, finish, rows_by_id.get(e.get('evaluation_id'), []))
        captures = (_capture(analysis.get("predicted_top5"), finish), _capture(analysis.get("independent_top5"), finish), money)
        key = e.get("condition_version", "不明")
        group = versions.setdefault(key, {name: [] for name in buckets})
        group["全体"].append(captures)
        group[analysis["judgement"]].append(captures)
        if 'logic2' in analysis:
            second = analysis['logic2']
            label = ('未算出・保留' if second.get('disagreement_count') is None or second.get('unjudged_count') or analysis.get('roster_errors')
                     else ('不一致2頭以上（研究群）' if second['disagreement_count'] >= 2 else '不一致0〜1頭（研究群）'))
            second_group = second_versions.setdefault(key, {name: [] for name in ('全体', '不一致0〜1頭（研究群）', '不一致2頭以上（研究群）', '未算出・保留')})
            second_group['全体'].append(captures)
            second_group[label].append(captures)
    output = {}
    for version, groups in versions.items():
        output[version] = {}
        for name, captures in groups.items():
            output[version][name] = _group_stats(captures)
    second_output = {version: {name: _group_stats(values) for name, values in groups.items()} for version, groups in second_versions.items()}
    return {"versions": output, 'logic2_versions': second_output, "excluded": excluded}
