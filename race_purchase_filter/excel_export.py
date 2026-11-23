"""表示対象と固定保存済みの判定を、既存のopenpyxl依存でExcelへ出力する。"""
from datetime import datetime
from pathlib import Path
import json
import os
import tempfile
import math
import unicodedata

from evaluation_utils import JST

SUMMARY_HEADERS = [
    '開催日', '競馬場', 'R', '発走', 'レース名', '出走頭数', '取得馬数',
    '第1購入適性', '第1不一致数', '第1未判定数', '第1不一致馬番', '評価困難馬数',
    '第2判定', '第2不一致数', '固定保存', '判定保存時刻', 'レースID', '保存ID',
]
DETAIL_HEADERS = [
    '開催日', '競馬場', 'R', 'レースID', '馬番', '馬名', '第1馬判定', '人気順位',
    '本番AI順位', '順位差(AI−人気)', '単勝オッズ', '第1判定理由', '人気取得時刻',
    'AI取得時刻', '人気算出方法', '人気CSV・行番号', '第2状態', '独立順位',
    '順位差(AI−独立)', '第2未判定理由', '評価困難分類', '評価困難の根拠', '保存ID',
]
COLORS = {'購入候補': 'E0F1E7', '見送り候補': 'FBE7E7', '判定保留': 'FFF4D9'}


def _value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if isinstance(value, Path):
        return str(value)
    return value.isoformat() if isinstance(value, datetime) else value


def _append(sheet, values):
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
    row = sheet.max_row + 1
    for col, value in enumerate(values, 1):
        value = _value(value)
        if isinstance(value, str):
            value = ILLEGAL_CHARACTERS_RE.sub('\uFFFD', value)
            if len(value) > 32767:
                raise ValueError('出力項目がExcelの1セル文字数制限を超えています。')
        cell = sheet.cell(row, col, value)
        if isinstance(value, str):
            # 馬名やメモが「=」で始まっても、式として実行させず文字列のまま保存する。
            cell.data_type = 's'


def _sheet(workbook, name, headers, subtitle):
    from openpyxl.styles import Font, PatternFill, Alignment
    sheet = workbook.create_sheet(name)
    sheet.cell(1, 1, name).font = Font(name='Meiryo', size=17, bold=True, color='16324F')
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(headers))
    sheet.cell(2, 1, subtitle)
    sheet.merge_cells(start_row=2, start_column=1, end_row=2, end_column=len(headers))
    sheet.cell(2, 1).alignment = Alignment(wrap_text=True, vertical='top')
    sheet.row_dimensions[1].height = 30
    sheet.row_dimensions[2].height = 35
    for col, label in enumerate(headers, 1):
        cell = sheet.cell(4, col, label)
        cell.font = Font(name='Meiryo', size=10, bold=True, color='FFFFFF')
        cell.fill = PatternFill('solid', fgColor='16324F')
        cell.alignment = Alignment(wrap_text=True, vertical='center')
    sheet.row_dimensions[4].height = 32
    sheet.freeze_panes = 'F5' if name == '馬別詳細' else 'C5'
    sheet.sheet_view.showGridLines = False
    return sheet


def _style(sheet, widths, statuses=None):
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    for index, width in enumerate(widths, 1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    for row in sheet.iter_rows(min_row=5):
        shade = COLORS.get((statuses or {}).get(row[0].row))
        if shade is None and row[0].row % 2:
            shade = 'F3F6F9'
        for cell in row:
            cell.font = Font(name='Meiryo', size=10, color='20354A')
            cell.alignment = Alignment(vertical='top', wrap_text=True)
            if shade:
                cell.fill = PatternFill('solid', fgColor=shade)
        # 長い理由・出典は折返し行数に合わせて高さを確保する。
        line_count = 1
        for cell in row:
            width = widths[cell.column - 1] - 2
            for line in str(cell.value or '').splitlines():
                units = sum(2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1 for ch in line)
                line_count = max(line_count, math.ceil(units / width))
        sheet.row_dimensions[row[0].row].height = min(409, max(36, line_count * 14 + 8))
    sheet.auto_filter.ref = f'A4:{get_column_letter(sheet.max_column)}{max(4, sheet.max_row)}'
    sheet.print_title_rows = '1:4'
    sheet.sheet_properties.pageSetUpPr.fitToPage = True
    sheet.page_setup.orientation = 'landscape'
    sheet.page_setup.paperSize = sheet.PAPERSIZE_A3
    sheet.page_setup.fitToWidth = 1
    sheet.page_setup.fitToHeight = 0


def export_judgements(path, races, records, source_file='', source_path=None, filters=None, exported_at=None):
    """再評価せず、現在表示中のレースと対応する固定保存だけを出力する。"""
    from openpyxl import Workbook
    if not races:
        raise ValueError('出力対象のレースがありません。')
    target = Path(path)
    if target.suffix.lower() != '.xlsx':
        raise ValueError('保存形式は .xlsx を指定してください。')
    if source_path and str(target.resolve()).casefold() == str(Path(source_path).resolve()).casefold():
        raise ValueError('読み込んだ原本Excelには上書きできません。別のファイル名を指定してください。')
    exported_at = exported_at or datetime.now(JST)
    filters = filters or {'競馬場': 'すべて', '購入適性': 'すべて'}
    context = f"元データ：{source_file}／出力{len(races)}レース／絞り込み：" + '・'.join(f'{k}={v}' for k, v in filters.items())
    wb = Workbook()
    wb.remove(wb.active)
    wb.properties.title = 'レース購入適性 判定一覧'
    summary = _sheet(wb, '判定一覧', SUMMARY_HEADERS, context)
    detail = _sheet(wb, '馬別詳細', DETAIL_HEADERS, '表示対象レースの固定保存済みデータ。第1・第2の判定を混ぜず、未算出は未算出のまま記載します。')
    info = _sheet(wb, '出力情報', ['項目', '内容'], '判定を再計算せず、固定保存済みの値を出力しています。')
    _append(info, ['出力日時（日本時間）', exported_at.astimezone(JST).isoformat(timespec='seconds')])
    _append(info, ['原本ファイル', source_file])
    _append(info, ['原本パス', source_path])
    _append(info, ['出力レース数', len(races)])
    _append(info, ['画面の絞り込み', filters])
    _append(info, ['注意', 'このファイルの編集はアプリの固定保存や判定を変更しません。未判定・未算出は0に補完していません。'])
    _append(info, ['第1定義', '人気5位以内 AND 本番AI能力順位6位以下。全頭を判定できた場合に不一致0〜1頭は購入候補、2頭以上は見送り候補。'])
    _append(info, ['第2', '係数・閾値未確定の状態はそのまま表示。第1購入適性とは別判定。'])
    shades, sources = {}, set()
    try:
        for race in races:
            key = str(race.get('race_key') or race.get('race_id'))
            record = records[key]
            analysis = record['analysis']
            second = analysis.get('logic2', {})
            saved = '発走前保存' if record.get('is_prospective') else '事後・時刻不明（検証外）'
            _append(summary, [race.get('date'), race.get('place'), race.get('race_no'), race.get('start_time'), race.get('race_name'),
                              race.get('field_size'), len(race.get('horses', [])), analysis.get('judgement'),
                              analysis.get('disagreement_count'), analysis.get('unjudged_count'),
                              ','.join(map(str, analysis.get('disagreement_horse_numbers', []))) or 'なし',
                              analysis.get('difficulty_horse_count'), second.get('status', '未算出'),
                              second.get('disagreement_count') if second.get('disagreement_count') is not None else '未算出',
                              saved, record.get('recorded_at'), str(record.get('race_id')), record.get('evaluation_id')])
            shades[summary.max_row] = analysis.get('judgement')
            second_by_horse = {(h.get('horse_no'), h.get('horse_name')): h for h in second.get('horses', [])}
            for horse in analysis.get('logic1', {}).get('horses', []):
                h = second_by_horse.get((horse.get('horse_no'), horse.get('horse_name')), {})
                flags = h.get('features', {}).get('difficulty_flags', [])
                gap = horse.get('rank_difference')
                if gap is None and horse.get('rank_difference_min') is not None:
                    gap = f"{horse['rank_difference_min']}〜{horse['rank_difference_max']}"
                independent_rank = h.get('independent_rank_min')
                if independent_rank is not None and independent_rank != h.get('independent_rank_max'):
                    independent_rank = f"{independent_rank}〜{h.get('independent_rank_max')}"
                _append(detail, [race.get('date'), race.get('place'), race.get('race_no'), str(record.get('race_id')),
                                 horse.get('horse_no'), horse.get('horse_name'), horse.get('status'),
                                 horse.get('popularity_rank_display') or horse.get('popularity_rank'), horse.get('production_rank'), gap,
                                 horse.get('popularity_odds'), '／'.join(horse.get('reasons', [])),
                                 horse.get('popularity_observed_at'), horse.get('ai_observed_at'), horse.get('popularity_method'),
                                 horse.get('popularity_source'), h.get('status'), independent_rank if independent_rank is not None else '未算出',
                                 h.get('rank_difference'), '／'.join(h.get('reasons', [])),
                                 '／'.join(f.get('category', '') for f in flags), flags, record.get('evaluation_id')])
            _append(info, [f"{race.get('place')}{race.get('race_no')}R 保存情報",
                           {'保存ID': record.get('evaluation_id'), '原本SHA256': record.get('sha256'),
                            '方式': record.get('condition_version'), '保存時刻': record.get('recorded_at'),
                            '発走前保存': record.get('is_prospective'), '評価時刻': analysis.get('as_of'),
                            'データ締切': analysis.get('data_cutoff')}])
            for source in analysis.get('odds_sources', []):
                serialized = json.dumps(source, ensure_ascii=False, sort_keys=True)
                if serialized not in sources:
                    sources.add(serialized)
                    _append(info, ['人気CSVの取得元・SHA256・取得時刻', source])
        _style(summary, [13, 10, 5, 9, 27, 10, 10, 15, 12, 12, 19, 14, 29, 14, 28, 29, 18, 39], shades)
        _style(detail, [13, 10, 5, 18, 8, 23, 15, 12, 13, 18, 13, 58, 29, 29, 43, 65, 18, 14, 18, 44, 40, 85, 39])
        _style(info, [34, 120])
        # 保存途中の失敗で既存ファイルを壊さないよう、同じフォルダの一時ファイルから置換する。
        target.parent.mkdir(parents=True, exist_ok=True)
        handle, temporary = tempfile.mkstemp(prefix='.judgement-export-', suffix='.xlsx', dir=target.parent)
        os.close(handle)
        try:
            wb.save(temporary)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
    finally:
        wb.close()
    return {'path': str(target.resolve()), 'race_count': len(races), 'horse_count': detail.max_row - 4}
