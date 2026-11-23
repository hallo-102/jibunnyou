"""Excel読込から自動評価・固定保存・結果入力までのGUI結合テスト。"""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from datetime import datetime
import app
from evaluation_fixtures import sample_race
from storage import default_data_paths
from evaluation_utils import JST


class TestOverviewGUI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if app.tk is None:
            raise unittest.SkipTest('tkinter unavailable')
        try:
            cls.root = app.tk.Tk()
            cls.root.withdraw()
        except app.tk.TclError as exc:
            raise unittest.SkipTest(str(exc))

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        with patch.object(app, 'default_data_paths', return_value=default_data_paths(self.temp.name)):
            self.ui = app.App(self.root)
        race = sample_race()
        other = copy.deepcopy(race)
        other.update(race_id='202603030511', place='福島', race_name='未勝利')
        other['horses'][-1]['history'] = []
        other['horses'][-1]['popularity_rank'] = None
        self.data = {'file_name': 'test.xlsx', 'sha256': 'sha', 'races': [race, other],
                     'warnings': ['不足情報を表示します。']}

    def tearDown(self):
        for widget in self.root.winfo_children():
            widget.destroy()

    def _load(self):
        with patch.object(app.filedialog, 'askopenfilename', return_value='test.xlsx'), \
             patch('excel_loader.load_races_from_xlsx', return_value=self.data), \
             patch.object(app, 'attach_nas_odds', side_effect=lambda data, **_: copy.deepcopy(data)), \
             patch.object(app, 'load_context', return_value={'sha256': None}), \
             patch.object(app, 'datetime') as clock, \
             patch.object(app.messagebox, 'showerror') as error:
            clock.now.return_value = datetime(2026, 11, 22, 10, tzinfo=JST)
            self.ui.load_xlsx()
        error.assert_not_called()

    def test_three_tabs_and_immediate_judgement_with_auto_save(self):
        self._load()
        self.assertEqual([self.ui.nb.tab(t, 'text') for t in self.ui.nb.tabs()], ['判定一覧', 'レース後結果', '結果検証'])
        self.assertFalse(hasattr(self.ui, 'tab_pre'))
        self.assertEqual(len(self.ui.race_tree.get_children()), 2)
        self.assertEqual(self.ui.race_tree.set('0', 'judgement'), '購入候補')
        self.assertEqual(self.ui.race_tree.set('1', 'judgement'), '判定保留')
        self.assertEqual(len(self.ui.eval_store.load_all()), 2)
        self.assertIn('根拠', self.ui.list_text.get('1.0', 'end'))
        self.assertEqual(self.ui.result_store.load_all(), [])

    def test_reload_uses_frozen_analysis(self):
        self._load()
        saved = self.ui.eval_store.load_all()
        self.data['races'][0]['horses'][0]['history'] = []
        self._load()
        self.assertEqual(saved, self.ui.eval_store.load_all())
        self.assertEqual(self.ui.race_tree.set('0', 'judgement'), '購入候補')

    def test_filters_and_result_link_use_exact_evaluation_id(self):
        self._load()
        self.ui._set_place_filter('福島')
        self.assertEqual(len(self.ui.race_tree.get_children()), 1)
        self.ui.judge_filter.set('購入候補')
        self.ui.refresh_race_list()
        self.assertEqual(len(self.ui.race_tree.get_children()), 0)
        self.ui.judge_filter.set('すべて')
        self.ui._set_place_filter('京都')
        self.ui.race_tree.selection_set('0')
        self.ui._send_to_result()
        self.ui.rvars['finish'].set('1,2,3')
        self.ui.save_result()
        result = self.ui.result_store.load_all()[0]
        self.assertEqual(result['evaluation_id'], self.ui.records['202608050511']['evaluation_id'])
        self.assertEqual(result['tickets'], [])

    def test_invalid_finish_is_not_saved(self):
        self._load()
        self.ui.race_tree.selection_set('0')
        self.ui._send_to_result()
        self.ui.rvars['finish'].set('1,1,3')
        with patch.object(app.messagebox, 'showerror') as error:
            self.ui.save_result()
        self.assertTrue(error.called)
        self.assertEqual(self.ui.result_store.load_all(), [])

    def test_export_button_exports_only_visible_races_without_changing_snapshots(self):
        self.assertTrue(self.ui.export_button.instate(['disabled']))
        self._load()
        self.ui._set_place_filter('京都')
        path = Path(self.temp.name) / 'filtered.xlsx'
        before = self.ui.eval_store.load_all()
        with patch.object(app.filedialog, 'asksaveasfilename', return_value=str(path)), \
             patch.object(app.messagebox, 'showinfo') as info:
            self.ui.export_button.invoke()
        self.assertTrue(path.exists())
        self.assertTrue(info.called)
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True)
        try:
            self.assertEqual(wb['判定一覧'].max_row, 5)
            self.assertEqual(wb['判定一覧']['B5'].value, '京都')
            self.assertEqual(wb['出力情報']['B5'].value is not None, True)
        finally:
            wb.close()
        self.assertEqual(self.ui.eval_store.load_all(), before)
        self.ui.judge_filter.set('見送り候補')
        self.ui.refresh_race_list()
        self.assertTrue(self.ui.export_button.instate(['disabled']))

    def test_export_dialog_cancel_writes_nothing(self):
        self._load()
        with patch.object(app.filedialog, 'asksaveasfilename', return_value=''), \
             patch.object(app.messagebox, 'showinfo') as info:
            self.ui.export_button.invoke()
        self.assertEqual(list(Path(self.temp.name).glob('*.xlsx')), [])
        info.assert_not_called()


if __name__ == '__main__':
    unittest.main()
