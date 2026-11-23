"""Excel出力の値、対象範囲、固定保存の非変更、原本保護を確認する。"""
import copy
import tempfile
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from evaluation_fixtures import sample_race
from evaluation_utils import JST
from assessment import analyze_race
from validation import build_evaluation_record
from excel_export import export_judgements

try:
    import openpyxl
except ImportError:
    openpyxl = None


@unittest.skipIf(openpyxl is None, 'openpyxl unavailable')
class TestExcelExport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.race = sample_race()
        stamp = datetime(2026, 11, 22, 10, tzinfo=JST)
        analysis = analyze_race(self.race, as_of=stamp)
        self.record = build_evaluation_record(self.race, analysis, 'original.xlsx', 'source-sha', now=stamp)
        self.records = {self.race['race_id']: self.record}

    def test_workbook_preserves_snapshot_values_filters_and_identifiers(self):
        before = copy.deepcopy(self.records)
        path = self.folder / '一覧.xlsx'
        result = export_judgements(path, [self.race], self.records, source_file='original.xlsx',
                                   filters={'競馬場': '京都', '購入適性': '購入候補'})
        wb = openpyxl.load_workbook(path, data_only=False)
        try:
            self.assertEqual(wb.sheetnames, ['判定一覧', '馬別詳細', '出力情報'])
            sheet = wb['判定一覧']
            self.assertEqual(sheet.max_row, 5)
            self.assertEqual(sheet['H5'].value, '購入候補')
            self.assertEqual(sheet['I5'].value, 0)
            self.assertEqual(sheet['N5'].value, '未算出')
            self.assertEqual(sheet['Q5'].value, self.race['race_id'])
            self.assertEqual(sheet['Q5'].data_type, 's')
            self.assertEqual(sheet.freeze_panes, 'C5')
            self.assertEqual(sheet.auto_filter.ref, 'A4:R5')
            self.assertEqual(sheet['H5'].fill.fgColor.rgb[-6:], 'E0F1E7')
            self.assertEqual(wb['馬別詳細'].max_row, 12)
            info = '\n'.join(str(cell.value) for row in wb['出力情報'] for cell in row)
            self.assertIn('source-sha', info)
            self.assertIn('京都', info)
            self.assertEqual(result['horse_count'], 8)
        finally:
            wb.close()
        self.assertEqual(self.records, before)

    def test_strings_cannot_become_excel_formulas(self):
        self.record['analysis']['logic1']['horses'][0]['horse_name'] = '=1+2'
        path = self.folder / 'safe.xlsx'
        export_judgements(path, [self.race], self.records)
        wb = openpyxl.load_workbook(path, data_only=False)
        try:
            cell = wb['馬別詳細']['F5']
            self.assertEqual(cell.value, '=1+2')
            self.assertEqual(cell.data_type, 's')
        finally:
            wb.close()

    def test_original_cannot_be_overwritten_and_failed_export_leaves_target_unchanged(self):
        original = self.folder / 'original.xlsx'
        original.write_bytes(b'original remains unchanged')
        with self.assertRaisesRegex(ValueError, '原本'):
            export_judgements(original, [self.race], self.records, source_path=original)
        self.assertEqual(original.read_bytes(), b'original remains unchanged')
        target = self.folder / 'existing.xlsx'
        target.write_bytes(b'previous export')
        with patch('excel_export.os.replace', side_effect=PermissionError('Excelで開いています')):
            with self.assertRaises(PermissionError):
                export_judgements(target, [self.race], self.records)
        self.assertEqual(target.read_bytes(), b'previous export')
        self.assertEqual(list(self.folder.glob('.judgement-export-*')), [])


if __name__ == '__main__':
    unittest.main()
