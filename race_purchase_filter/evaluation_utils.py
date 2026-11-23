"""欠損を0にしない、評価用の値・日時変換。"""
import math
import re
from datetime import date, datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))


def number(value):
    if isinstance(value, bool):
        return None
    try:
        result = float(str(value).replace(',', '').strip())
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def integer(value):
    value = number(value)
    return int(value) if value is not None and value.is_integer() else None


def parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    for fmt in ('%Y-%m-%d', '%Y/%m/%d', '%Y%m%d', '%Y.%m.%d'):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse_timestamp(value):
    if isinstance(value, datetime):
        return value.astimezone(JST) if value.tzinfo else value.replace(tzinfo=JST)
    try:
        value = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return value.astimezone(JST) if value.tzinfo else value.replace(tzinfo=JST)
    except (TypeError, ValueError):
        return None


def race_start(race):
    day = parse_date(race.get('date'))
    if day:
        for fmt in ('%H:%M', '%H:%M:%S'):
            try:
                return datetime.combine(day, datetime.strptime(str(race.get('start_time')), fmt).time(), JST)
            except ValueError:
                continue
    return None


def course_parts(value):
    text = str(value).replace(' ', '').replace('ダート', 'ダ').replace('障害', '障')
    match = re.search(r'(芝|ダ|障)(\d{3,4})', text)
    return (match[1], int(match[2])) if match else (None, None)


def clock_seconds(value):
    if isinstance(value, (int, float)):
        value = number(value)
        return value if value is not None and value > 0 else None
    match = re.fullmatch(r'(\d{1,2}):(\d{2}(?:\.\d+)?)', str(value).strip())
    if not match or float(match[2]) >= 60:
        return None
    return int(match[1]) * 60 + float(match[2])
