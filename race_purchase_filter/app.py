"""Excel読込・独立評価・固定保存・結果検証を行う3タブのGUI。"""
from __future__ import annotations

import csv
import os
import sys
import hashlib
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import filter_races_by_place, list_places, format_rate, check_ticket_hit
from independent import VERSION, analyze_race, format_analysis, integer, load_policy, policy_version
from evaluation_utils import race_start
from research_context import load_context, enrich_race
from odds_loader import attach_nas_odds
from storage import EvaluationStore, ResultStore, default_data_paths
from validation import JST, aggregate_capture, build_evaluation_record, parse_finish_text, parse_virtual_prices

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except ImportError:
    tk = None


class App:
    @staticmethod
    def _race_key(race):
        # IDがない原本も内部キーで区別し、「不明」同士を上書きしない。
        return str(race.get("race_key") or race.get("race_id"))

    def __init__(self, root):
        self.root = root
        root.title("独立レース購入適性判定")
        root.geometry("1180x850")
        root.minsize(960, 650)
        paths = default_data_paths()
        self.eval_store = EvaluationStore(paths["evaluations"])
        self.result_store = ResultStore(paths["results"])
        self.loaded = {"file_name": "", "sha256": "", "races": [], "warnings": []}
        self.records = {}
        self._filtered_races = []
        self._result_choices = {}
        self.source_path = None

        toolbar = ttk.Frame(root)
        toolbar.pack(fill="x", padx=8, pady=7)
        self.load_button = ttk.Button(toolbar, text="Excel読込 → 第1・第2判定を固定保存", command=self.load_xlsx)
        self.load_button.pack(side="left")
        self.export_button = ttk.Button(toolbar, text='判定一覧をExcel出力', command=self.export_xlsx, state='disabled')
        self.export_button.pack(side='left', padx=8)
        self.loaded_file_text = tk.StringVar(value="対象日のExcelを選んでください")
        ttk.Label(toolbar, textvariable=self.loaded_file_text).pack(side="left", padx=12)
        self.odds_source_text = tk.StringVar(value='人気：NASの対象日 OZZU_YYYYMMDD.csv を自動読込（単勝オッズ順）')
        ttk.Label(root, textvariable=self.odds_source_text, wraplength=1140).pack(anchor='w', padx=8, pady=(0, 4))
        self.nb = ttk.Notebook(root)
        self.nb.pack(fill="both", expand=True)
        self.tab_list, self.tab_result, self.tab_compare = (ttk.Frame(self.nb) for _ in range(3))
        self.nb.add(self.tab_list, text="判定一覧")
        self.nb.add(self.tab_result, text="レース後結果")
        self.nb.add(self.tab_compare, text="結果検証")
        self._build_list()
        self._build_result()
        self._build_compare()
        self.refresh_race_list()
        self.refresh_compare()

    def _build_list(self):
        f = self.tab_list
        self.summary_text = tk.StringVar()
        self.warning_text = tk.StringVar()
        self.place_filter = tk.StringVar(value="すべて")
        self.judge_filter = tk.StringVar(value="すべて")
        ttk.Label(f, textvariable=self.summary_text).pack(anchor="w", padx=8, pady=5)
        self.filter_frame = ttk.Frame(f)
        self.filter_frame.pack(fill="x", padx=8, pady=4)
        selector = ttk.Frame(f)
        selector.pack(fill="x", padx=8)
        ttk.Label(selector, text="購入適性：").pack(side="left")
        judge = ttk.Combobox(selector, textvariable=self.judge_filter, state="readonly", width=15,
                             values=["すべて", "購入候補", "見送り候補", "判定保留"])
        judge.pack(side="left")
        judge.bind("<<ComboboxSelected>>", lambda _: self.refresh_race_list())
        ttk.Label(selector, text="第1＝人気5位以内かつAI6位以下。評価困難・第2判定は別記録です。").pack(side="left", padx=12)
        ttk.Label(f, textvariable=self.warning_text, foreground="#9a5c00", wraplength=1120).pack(anchor="w", padx=8, pady=4)
        panes = ttk.Panedwindow(f, orient="vertical")
        panes.pack(fill="both", expand=True, padx=8, pady=4)
        table, details = ttk.Frame(panes), ttk.Frame(panes)
        panes.add(table, weight=3)
        panes.add(details, weight=2)
        columns = [("date", "開催日", 90), ("place", "競馬場", 55), ("race_no", "R", 35),
                   ("start_time", "発走", 50), ("race_name", "レース名", 170),
                   ("field_size", "出走/取得", 85), ("judgement", "第1購入適性", 100),
                   ("disagreement_count", "第1不一致", 75), ("unjudged_count", "第1未判定", 75),
                   ("disagreement_horses", "第1不一致馬番", 120), ("difficulty_count", "評価困難", 75),
                   ("logic2_status", "第2判定", 170), ("logic2_count", "第2不一致", 85), ("saved", "固定保存", 170)]
        self.race_tree = ttk.Treeview(table, columns=[k for k, _, _ in columns], show="headings", height=16, selectmode="browse")
        for key, title, width in columns:
            self.race_tree.heading(key, text=title)
            self.race_tree.column(key, width=width, minwidth=width, stretch=key in ("race_name", "disagreement_horses"))
        self._scroll_table(table, self.race_tree)
        for tag, color in (("購入候補", "#e0f1e7"), ("見送り候補", "#fbe7e7"), ("判定保留", "#fff4d9")):
            self.race_tree.tag_configure(tag, background=color)
        self.race_tree.bind("<<TreeviewSelect>>", self._on_race_select)
        self.race_tree.bind("<Double-Button-1>", self._send_to_result)
        ttk.Label(details, text="不一致馬・未判定馬と根拠データ（行を選択／ダブルクリックで結果入力へ）").pack(anchor="w")
        self.list_text = tk.Text(details, height=13, wrap="word", state="disabled")
        scroll = ttk.Scrollbar(details, command=self.list_text.yview)
        scroll.pack(side="right", fill="y")
        self.list_text.pack(fill="both", expand=True)
        self.list_text.configure(yscrollcommand=scroll.set)

    @staticmethod
    def _scroll_table(frame, tree):
        vertical = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        horizontal = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vertical.grid(row=0, column=1, sticky="ns")
        horizontal.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

    def load_xlsx(self):
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if not path:
            return
        self.load_button.configure(state="disabled")
        self.loaded_file_text.set("人気・AI順位と過去走・相手評価マスターを読込中…")
        self.root.update_idletasks()
        try:
            from excel_loader import load_races_from_xlsx
            data = load_races_from_xlsx(path)
            archive_dir = os.path.join(os.path.dirname(self.eval_store.path), 'odds_snapshots')
            data = attach_nas_odds(data, archive_dir=archive_dir)
            context = load_context()
            policy = load_policy()
            version = policy_version(policy)
            fingerprint = hashlib.sha256((data['sha256'] + str(context.get('sha256')) + version
                                          + data.get('odds_fingerprint', '')).encode()).hexdigest()
            stored = {(str(e.get("race_key") or e.get("race_id")), e.get('input_fingerprint') or e.get("sha256"), e.get("condition_version")): e for e in self.eval_store.load_all()}
            records = {}
            for race in data["races"]:
                rid = self._race_key(race)
                key = (rid, fingerprint, version)
                record = stored.get(key)
                if record is None:
                    now = datetime.now(JST)
                    start = race_start(race)
                    cutoff = min(now, start) if start else now
                    enriched = enrich_race(race, context, cutoff, now)
                    analysis = analyze_race(enriched, as_of=now, policy=policy, context_sha256=context.get('sha256'))
                    analysis['input_fingerprint'] = fingerprint
                    analysis['odds_sources'] = data.get('odds_sources', [])
                    record = build_evaluation_record(race, analysis, data["file_name"], data["sha256"])
                    # 読込時点の評価・全根拠・TOP5を一度だけ固定し、再読込では上書きしない。
                    record = self.eval_store.get_or_append(record)
                records[rid] = record
        except Exception as exc:
            messagebox.showerror("読込・評価エラー", str(exc))
            self.loaded_file_text.set(self.loaded.get("file_name") or "読込に失敗しました")
            return
        finally:
            self.load_button.configure(state="normal")
        self.loaded, self.records = data, records
        self.source_path = os.path.abspath(path)
        self.loaded_file_text.set(f"{data['file_name']}（{len(records)}レースを固定保存）")
        sources = data.get('odds_sources', [])
        names = ' / '.join(s['path'] for s in sources)
        self.odds_source_text.set(f"人気CSV：{names or '未取得'} ／ 人気照合 {data.get('odds_matched_horses', 0)}頭")
        self.place_filter.set("すべて")
        self.judge_filter.set("すべて")
        self.refresh_race_list()
        self._refresh_result_choices()
        self.refresh_compare()
        self.nb.select(self.tab_list)
        items = self.race_tree.get_children()
        if items:
            self.race_tree.selection_set(items[0])
            self._on_race_select()

    def _set_place_filter(self, place):
        self.place_filter.set(place)
        self.refresh_race_list()

    def refresh_race_list(self):
        for widget in self.filter_frame.winfo_children():
            widget.destroy()
        all_races = self.loaded.get("races", [])
        ttk.Label(self.filter_frame, text="競馬場：").pack(side="left")
        for place in ["すべて"] + list_places(all_races):
            count = len(filter_races_by_place(all_races, place))
            ttk.Button(self.filter_frame, text=f"{place} ({count})", command=lambda p=place: self._set_place_filter(p)).pack(side="left", padx=2)
        races = filter_races_by_place(all_races, self.place_filter.get())
        if self.judge_filter.get() != "すべて":
            races = [r for r in races if self.records[self._race_key(r)]["analysis"]["judgement"] == self.judge_filter.get()]
        self._filtered_races = races
        self.race_tree.delete(*self.race_tree.get_children())
        for index, race in enumerate(races):
            record = self.records[self._race_key(race)]
            analysis = record["analysis"]
            values = [race.get("date"), race.get("place"), race.get("race_no"), race.get("start_time"), race.get("race_name"),
                      f"{race.get('field_size', '不明')} / {len(race.get('horses', []))}", analysis["judgement"],
                      analysis["disagreement_count"], analysis["unjudged_count"],
                      ",".join(map(str, analysis["disagreement_horse_numbers"])) or "なし",
                      analysis['difficulty_horse_count'], analysis['logic2']['status'],
                      analysis['logic2']['disagreement_count'] if analysis['logic2']['disagreement_count'] is not None else '未算出',
                      "発走前保存" if record["is_prospective"] else "事後・時刻不明（検証外）"]
            self.race_tree.insert("", "end", iid=str(index), values=values, tags=(analysis["judgement"],))
        counts = {label: sum(e["analysis"]["judgement"] == label for e in self.records.values()) for label in ("購入候補", "見送り候補", "判定保留")}
        self.summary_text.set(f"第1：全{len(all_races)}レース／購入候補{counts['購入候補']}／見送り候補{counts['見送り候補']}／判定保留{counts['判定保留']}／表示{len(races)}")
        self.warning_text.set("  ".join(self.loaded.get("warnings", [])))
        self._set_details("")
        self.export_button.configure(state='normal' if races else 'disabled')

    def export_xlsx(self):
        if not self._filtered_races:
            messagebox.showinfo('Excel出力', 'Excelを読み込み、出力対象のレースを表示してください。')
            return
        filename = '判定一覧_' + datetime.now(JST).strftime('%Y%m%d_%H%M%S') + '.xlsx'
        path = filedialog.asksaveasfilename(title='表示中の判定一覧をExcel出力', defaultextension='.xlsx',
                                          initialfile=filename, filetypes=[('Excelブック', '*.xlsx')],
                                          confirmoverwrite=True)
        if not path:
            return
        try:
            from excel_export import export_judgements
            result = export_judgements(path, list(self._filtered_races), self.records,
                                       source_file=self.loaded.get('file_name', ''), source_path=self.source_path,
                                       filters={'競馬場': self.place_filter.get(), '購入適性': self.judge_filter.get()})
        except Exception as exc:
            messagebox.showerror('Excel出力エラー', str(exc))
            return
        messagebox.showinfo('Excel出力完了', f"{result['race_count']}レース・{result['horse_count']}頭を出力しました。\n{result['path']}")

    def _selected_race(self):
        selection = self.race_tree.selection()
        return self._filtered_races[int(selection[0])] if selection and int(selection[0]) < len(self._filtered_races) else None

    def _set_details(self, text):
        self.list_text.configure(state="normal")
        self.list_text.delete("1.0", "end")
        self.list_text.insert("end", text)
        self.list_text.configure(state="disabled")

    def _on_race_select(self, _event=None):
        race = self._selected_race()
        if race:
            record = self.records[self._race_key(race)]
            self._set_details(format_analysis(race, record["analysis"], record))

    def _send_to_result(self, _event=None):
        race = self._selected_race()
        if race:
            eid = self.records[self._race_key(race)]["evaluation_id"]
            for label, record in self._result_choices.items():
                if record["evaluation_id"] == eid:
                    self.result_selection.set(label)
                    self._result_selection_changed()
                    break
            self.nb.select(self.tab_result)

    def _build_result(self):
        f = self.tab_result
        ttk.Label(f, text="固定保存した判定を選び、確定1〜3着を入力します。TOP5は保存時点の値で比較します。", wraplength=1000).pack(anchor="w", padx=8, pady=8)
        self.result_selection = tk.StringVar()
        self.result_combo = ttk.Combobox(f, textvariable=self.result_selection, state="readonly", width=100)
        self.result_combo.pack(fill="x", padx=8)
        self.result_combo.bind("<<ComboboxSelected>>", self._result_selection_changed)
        self.rvars = {}
        for key, label in (("race_id", "race_id（保存記録から反映）"), ("finish", "確定1〜3着馬番（例 3,5,7）"),
                           ("ticket_type", "券種（購入券がある場合のみ）"), ("combination", "組み合わせ（例 3-5）"),
                           ("amount", "購入額（円、任意）"), ("payout", "払戻総額（円、任意）")):
            row = ttk.Frame(f)
            row.pack(fill="x", padx=8, pady=4)
            ttk.Label(row, text=label, width=42).pack(side="left")
            self.rvars[key] = tk.StringVar()
            ttk.Entry(row, textvariable=self.rvars[key], state="readonly" if key == "race_id" else "normal").pack(side="left", fill="x", expand=True)
        ttk.Button(f, text="確定結果を追記保存", command=self.save_result).pack(anchor="w", padx=8, pady=8)
        ttk.Label(f, text='仮想検証用の確定払戻（任意）：1行に 券種,組み合わせ,100円払戻額。例：馬連,3-5,2500').pack(anchor='w', padx=8)
        self.virtual_price_text = tk.Text(f, height=4)
        self.virtual_price_text.pack(fill='x', padx=8, pady=4)
        ttk.Button(f, text="結果CSV取込", command=lambda: self.import_csv(False)).pack(anchor="w", padx=8, pady=2)
        ttk.Button(f, text="チケットCSV取込", command=lambda: self.import_csv(True)).pack(anchor="w", padx=8, pady=2)
        ttk.Button(f, text="仮想払戻CSV取込（payout_per_100列）", command=lambda: self.import_csv(True, True)).pack(anchor="w", padx=8, pady=2)
        self.result_label = ttk.Label(f, text="購入券なしでもTOP5捕捉率を検証できます。事後保存は前向き検証に含めません。", wraplength=1100)
        self.result_label.pack(anchor="w", padx=8, pady=8)
        self._refresh_result_choices()

    def _refresh_result_choices(self):
        choices = {}
        for record in self.eval_store.load_all():
            label = (f"{record.get('date')} {record.get('place')} {record.get('race_no')}R {record.get('race_name')} "
                     f"[{record.get('recorded_at')}] {record['evaluation_id']}")
            choices[label] = record
        self._result_choices = choices
        self.result_combo.configure(values=list(choices))
        if self.result_selection.get() not in choices:
            self.result_selection.set("")
            self.rvars["race_id"].set("")

    def _result_selection_changed(self, _event=None):
        record = self._result_choices.get(self.result_selection.get())
        for variable in self.rvars.values():
            variable.set("")
        self.virtual_price_text.delete('1.0', 'end')
        if record:
            self.rvars["race_id"].set(str(record["race_id"]))

    @staticmethod
    def _ticket(row, finish):
        kind = str(row.get("ticket_type") or "").strip()
        if not kind:
            return []
        amounts = {}
        for field in ("amount", "payout"):
            raw = str(row[field]).strip() if row.get(field) is not None else ""
            value = integer(raw)
            if raw and (value is None or value < 0):
                raise ValueError("金額・払戻は0以上の整数または空欄で入力してください。")
            amounts[field] = value
        # 枠番データを持たないため枠連の誤った的中判定を出さない。
        hit = check_ticket_hit(kind, row.get("combination"), finish) if kind != "枠連" else None
        return [{"ticket_type": kind, "combination": row.get("combination"), **amounts, "hit": hit}]

    def save_result(self):
        record = self._result_choices.get(self.result_selection.get())
        if not record:
            messagebox.showerror("保存記録未選択", "照合する固定保存を選択してください。")
            return
        finish = parse_finish_text(self.rvars["finish"].get())
        if finish is None:
            messagebox.showerror("着順エラー", "確定した異なる3頭の正の馬番を、1着から順に入力してください。")
            return
        try:
            tickets = self._ticket({key: variable.get() for key, variable in self.rvars.items()}, finish)
            virtual_payouts = parse_virtual_prices(self.virtual_price_text.get('1.0', 'end'))
            self.result_store.append_result({"recorded_at": datetime.now(JST).isoformat(timespec="seconds"),
                                            "race_id": record["race_id"], "evaluation_id": record["evaluation_id"],
                                            "finish_top3": finish, "tickets": tickets, 'virtual_payouts': virtual_payouts})
        except (ValueError, OSError) as exc:
            messagebox.showerror("保存エラー", str(exc))
            return
        self.result_label.configure(text="確定結果を追記しました。結果検証タブで保存時点のTOP5と比較できます。")
        self.refresh_compare()

    def import_csv(self, tickets_only=False, virtual_only=False):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
        if not path:
            return
        try:
            all_records = self.eval_store.load_all()
            by_id = {e["evaluation_id"]: e for e in all_records}
            pending = []
            with open(path, newline="", encoding="utf-8-sig") as stream:
                for row_no, row in enumerate(csv.DictReader(stream), 2):
                    rid, eid = str(row.get("race_id", "")), row.get("evaluation_id")
                    if not eid:
                        matches = [e for e in all_records if str(e["race_id"]) == rid]
                        if len(matches) != 1:
                            raise ValueError(f"CSV {row_no}行: 固定保存を特定できません。evaluation_id を指定してください。")
                        eid = matches[0]["evaluation_id"]
                    if eid not in by_id or str(by_id[eid]["race_id"]) != rid:
                        raise ValueError(f"CSV {row_no}行: 保存IDとrace_idが一致しません。")
                    finish = parse_finish_text(row.get("finish_top3"))
                    if finish is None and (not tickets_only or str(row.get("finish_top3") or "").strip()):
                        raise ValueError(f"CSV {row_no}行: 確定1〜3着が不足・不正です。")
                    virtual_payouts = []
                    if virtual_only:
                        text = f"{row.get('ticket_type','')},{row.get('combination','')},{row.get('payout_per_100','')}"
                        virtual_payouts = parse_virtual_prices(text)
                    pending.append({"recorded_at": datetime.now(JST).isoformat(timespec="seconds"), "race_id": rid,
                                    "evaluation_id": eid, "finish_top3": finish or [],
                                    "tickets": self._ticket(row, finish or []) if tickets_only and not virtual_only else [],
                                    'virtual_payouts': virtual_payouts})
            # 内容を全行検証してから書くことで、形式エラーによる途中取込を避ける。
            for result in pending:
                self.result_store.append_result(result)
            self.result_label.configure(text=f"CSVを{len(pending)}件追記しました。同一レースのTOP5捕捉率は1回だけ数えます。")
            self.refresh_compare()
        except (ValueError, OSError) as exc:
            messagebox.showerror("CSV取込エラー", str(exc))

    def _build_compare(self):
        f = self.tab_compare
        ttk.Label(f, text="発走前に固定保存した判定だけを使用し、全体・購入候補・見送り候補・保留のTOP5捕捉率を比較します。", wraplength=1100).pack(anchor="w", padx=8, pady=8)
        ttk.Label(f, text="第1と第2は別集計。第2の2頭以上見送り効果は未検証。仮想収支は事前の原本買い目と確定払戻だけを使用します。", wraplength=1100).pack(anchor="w", padx=8)
        self.compare_text = tk.Text(f, wrap="word", state="disabled")
        self.compare_text.pack(fill="both", expand=True, padx=8, pady=8)
        ttk.Button(f, text="再計算", command=self.refresh_compare).pack(anchor="w", padx=8, pady=6)

    def refresh_compare(self):
        stats = aggregate_capture(self.eval_store.load_all(), self.result_store.load_all())
        lines = []
        for logic_title, versions in (('第1判定', stats['versions']), ('第2判定（購入効果未検証）', stats.get('logic2_versions', {}))):
            for version, groups in versions.items():
                lines.append(f"{logic_title}／方式：{version}")
                for name, group in groups.items():
                    lines.append(f"• {name}：{group['race_count']}レース")
                    for title, key in (("本番AI", "production"), ("独立評価", "independent")):
                        values = group[key]
                        lines.append(f"  - {title} TOP5捕捉率 {format_rate(values['rate'])} "
                                     f"（{values['captured']} / {values['slots']}頭枠・{values['recorded_races']}レース）")
                    money = group['virtual']
                    fmt = lambda value: '未計算' if value is None else f'{value:,.0f}円'
                    lines.append(f"  - 仮想投資{fmt(money['investment'])}／払戻{fmt(money['payout'])}／利益{fmt(money['profit'])}／回収率{format_rate(money['recovery_rate'])}（金額不足{money['missing_races']}R）")
        if not lines:
            lines.append("比較可能な発走前保存と確定結果の組み合わせがありません。TOP5捕捉率は未計算です。")
        lines.append("\n検証から除外した保存・結果：")
        lines.extend(f"• {reason}：{count}件" for reason, count in stats["excluded"].items())
        lines.append("\n事後保存を事前保存へ変更することはできません。着順が食い違う追記は矛盾として除外します。")
        self.compare_text.configure(state="normal")
        self.compare_text.delete("1.0", "end")
        self.compare_text.insert("end", "\n".join(lines))
        self.compare_text.configure(state="disabled")


def main():
    if tk is None:
        print("tkinterが必要です。PythonのGUI対応環境で起動してください。")
        return
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
