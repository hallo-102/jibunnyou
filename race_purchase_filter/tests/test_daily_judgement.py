"""無人追記の原本保持、再実行、失敗時の保護を検証する。"""
from datetime import date, datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

import openpyxl
from assessment import analyze_race
from evaluation_fixtures import sample_race
from evaluation_utils import JST
from excel_export import export_judgements
from validation import build_evaluation_record
from storage import EvaluationStore
from daily_judgement import append_report, run_daily, SHEETS


class TestDailyJudgement(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.source = self.folder / '馬の競走成績_with_feat_20261122.xlsx'
        wb = openpyxl.Workbook()
        wb.active.title = '既存予想'
        wb.active['A1'] = '=1+2'
        wb.active['B1'] = '本番AIは変更しない'
        wb.save(self.source)
        wb.close()
        self.race = sample_race()
        now = datetime(2026, 11, 22, 10, tzinfo=JST)
        analysis = analyze_race(self.race, as_of=now)
        self.record = build_evaluation_record(self.race, analysis, self.source.name, 'test', now)
        self.report = self.folder / 'report.xlsx'
        export_judgements(self.report, [self.race], {self.race['race_id']: self.record})

    def test_append_preserves_all_original_parts_except_package_metadata(self):
        output = self.folder / 'output.xlsx'
        append_report(self.source, self.report, output)
        metadata = {'xl/workbook.xml', 'xl/styles.xml', 'xl/_rels/workbook.xml.rels', '[Content_Types].xml'}
        with ZipFile(self.source) as before, ZipFile(output) as after:
            for name in before.namelist():
                if name not in metadata:
                    self.assertEqual(before.read(name), after.read(name), name)
        wb = openpyxl.load_workbook(output)
        try:
            self.assertEqual(wb.sheetnames, ['既存予想', *SHEETS])
            self.assertEqual(wb['既存予想']['A1'].value, '=1+2')
            self.assertEqual(wb['判定一覧']['H5'].value, '購入候補')
            self.assertEqual(wb['判定一覧']['H5'].fill.fgColor.rgb[-6:], 'E0F1E7')
            self.assertEqual(wb['判定一覧'].freeze_panes, 'C5')
            self.assertEqual(wb['判定馬別詳細'].max_row, 12)
        finally:
            wb.close()
        with self.assertRaisesRegex(ValueError, '既に存在'):
            append_report(output, self.report, self.folder / 'duplicate.xlsx')

    def test_daily_run_is_idempotent_and_does_not_touch_other_dates(self):
        day = date(2026, 11, 22)
        state = self.folder / 'state'
        ledger = EvaluationStore(str(self.folder / 'evaluations.jsonl'))
        data = {'file_name': self.source.name, 'sha256': 'test', 'races': [self.race], 'warnings': []}
        with patch('daily_judgement.load_races_from_xlsx', return_value=data), \
             patch('daily_judgement.attach_nas_odds', side_effect=lambda d, **kw: d), \
             patch('daily_judgement.load_context', return_value={'sha256': 'context'}), \
             patch('daily_judgement.enrich_race', side_effect=lambda r, *args: r), \
             patch('daily_judgement.EvaluationStore', return_value=ledger):
            first = run_daily(day, self.folder, state)
            second = run_daily(day, self.folder, state)
            self.assertEqual(first['status'], 'completed')
            self.assertEqual(second['status'], 'already_done')
            self.assertEqual(first['output_sha256'], second['output_sha256'])
            self.assertEqual(len(ledger.load_all()), 1)
            self.assertTrue(Path(first['backup']).exists())
            self.assertEqual(run_daily(date(2026, 11, 23), self.folder, state)['status'], 'skipped')

    def test_failed_replacement_keeps_original_and_releases_lock(self):
        original = self.source.read_bytes()
        ledger = EvaluationStore(str(self.folder / 'evaluations.jsonl'))
        data = {'file_name': self.source.name, 'sha256': 'test', 'races': [self.race], 'warnings': []}
        with patch('daily_judgement.load_races_from_xlsx', return_value=data), \
             patch('daily_judgement.attach_nas_odds', side_effect=lambda d, **kw: d), \
             patch('daily_judgement.load_context', return_value={'sha256': 'context'}), \
             patch('daily_judgement.enrich_race', side_effect=lambda r, *args: r), \
             patch('daily_judgement.EvaluationStore', return_value=ledger), \
             patch('daily_judgement.os.replace', side_effect=PermissionError('Excel使用中')):
            with self.assertRaises(PermissionError):
                run_daily(date(2026, 11, 22), self.folder, self.folder / 'state')
        self.assertEqual(self.source.read_bytes(), original)
        self.assertFalse((self.folder / 'state' / '20261122.lock').exists())
        self.assertEqual(list(self.folder.glob('.judgement-*')), [])


if __name__ == '__main__':
    unittest.main()
