"""tkinter GUI: 4 tabs.

- 事前判定入力 / レース一覧 / レース後結果 / 方式比較
Evaluation disagreement count is MANUAL INPUT ONLY.
算出式未確定のため手動入力 (formula unconfirmed).
This judgement is provisional for verification, not an automatic purchase decision.
"""
from __future__ import annotations

import csv
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core import (  # noqa: E402
    CONDITION_VERSION_DEFAULT,
    aggregate_methods,
    check_ticket_hit,
    compute_top5_capture,
    filter_races_by_place,
    format_rate,
    list_places,
    is_excluded_race,
    judge_c,
    judge_disagreement,
    parse_horse_numbers,
)
from storage import DuplicateSnapshotError, ResultStore, SnapshotStore, default_data_paths  # noqa: E402

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except ImportError:  # pragma: no cover - environment without tkinter
    tk = None


MANUAL_NOTE = "評価不一致馬の数：算出式未確定のため手動入力（自動計算は行いません）"
PROVISIONAL_NOTE = "この判定は検証用の暫定判定であり、自動購入可否ではありません。"
COMPARE_PREMISE = "方式比較の前提：同一原本・同一券種・同一金額で比較します。"
C_NOTE = "C方式：閾値未確定のため「判断保留・未算出」。ログ保存のみ行い、回収率・利益は計算しません。"


class App:
    def __init__(self, root):
        self.root = root
        root.title("独立レース購入適性判定")
        paths = default_data_paths()
        self.snap_store = SnapshotStore(paths["snapshots"])
        self.result_store = ResultStore(paths["results"])
        self.loaded = {"file_name": "", "sha256": "", "races": []}
        self.condition_version = tk.StringVar(value=CONDITION_VERSION_DEFAULT)
        self.exclude_enabled = tk.BooleanVar(value=True)

        nb = ttk.Notebook(root)
        self.nb = nb
        nb.pack(fill="both", expand=True)
        self.tab_pre = ttk.Frame(nb)
        self.tab_list = ttk.Frame(nb)
        self.tab_result = ttk.Frame(nb)
        self.tab_compare = ttk.Frame(nb)
        nb.add(self.tab_pre, text="事前判定入力")
        nb.add(self.tab_list, text="レース一覧")
        nb.add(self.tab_result, text="レース後結果")
        nb.add(self.tab_compare, text="方式比較")
        self._build_pre()
        self._build_list()
        self._build_result()
        self._build_compare()
        self.refresh_compare()

    # --- 事前判定入力 ---
    def _build_pre(self):
        f = self.tab_pre
        ttk.Label(f, text=MANUAL_NOTE).pack(anchor="w", padx=8, pady=4)
        ttk.Label(f, text=PROVISIONAL_NOTE).pack(anchor="w", padx=8)
        ttk.Label(f, text=C_NOTE).pack(anchor="w", padx=8, pady=4)
        ttk.Button(f, text="xlsx 読込", command=self.load_xlsx).pack(anchor="w", padx=8, pady=4)
        self.file_label = ttk.Label(f, text="ファイル未読込（手動入力で継続できます）")
        self.file_label.pack(anchor="w", padx=8)
        self.vars = {}
        fields = [
            ("race_id", "race_id"),
            ("model_generation", "モデル世代（手動入力 例 20260730 / 20260817）"),
            ("disagreement_count", "評価不一致馬の数（手動入力のみ）"),
            ("disagreement_horses", "評価不一致 該当馬番号（カンマ区切り）"),
            ("predicted_top5", "予想TOP5馬番（カンマ区切り、任意）"),
            ("race_name", "レース名・クラス"),
            ("opp_count", "相手関係比較困難 該当頭数"),
            ("opp_horses", "相手関係比較困難 該当馬番号"),
            ("opp_memo", "相手関係比較困難 根拠メモ"),
            ("time_count", "時計比較困難 該当頭数"),
            ("time_horses", "時計比較困難 該当馬番号"),
            ("time_memo", "時計比較困難 根拠メモ"),
        ]
        for key, label in fields:
            row = ttk.Frame(f)
            row.pack(fill="x", padx=8, pady=2)
            ttk.Label(row, text=label, width=48).pack(side="left")
            var = tk.StringVar()
            self.vars[key] = var
            ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)
        self.opp_missing = tk.BooleanVar()
        self.time_missing = tk.BooleanVar()
        ttk.Checkbutton(f, text="相手関係比較困難 データ未取得", variable=self.opp_missing).pack(anchor="w", padx=8)
        ttk.Checkbutton(f, text="時計比較困難 データ未取得", variable=self.time_missing).pack(anchor="w", padx=8)
        ttk.Checkbutton(f, text="新馬・未勝利戦を購入対象外とする", variable=self.exclude_enabled).pack(anchor="w", padx=8)
        row = ttk.Frame(f)
        row.pack(fill="x", padx=8, pady=2)
        ttk.Label(row, text="条件バージョン", width=48).pack(side="left")
        ttk.Entry(row, textvariable=self.condition_version).pack(side="left", fill="x", expand=True)
        self.judge_label = ttk.Label(f, text="判定: 判定保留 / C: 判断保留・未算出")
        self.judge_label.pack(anchor="w", padx=8, pady=4)
        ttk.Button(f, text="判定を表示", command=self.show_judge).pack(anchor="w", padx=8)
        ttk.Button(f, text="事前判定スナップショットを保存（追記専用・変更不可）", command=self.save_snapshot).pack(anchor="w", padx=8, pady=4)

    def _disagreement_value(self):
        s = self.vars["disagreement_count"].get().strip()
        if s == "":
            return None
        try:
            return int(s)
        except ValueError:
            return None

    def show_judge(self):
        dc = self._disagreement_value()
        self.judge_label.config(text=f"判定: {judge_disagreement(dc)} / C: {judge_c()}")

    def load_xlsx(self):
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx")])
        if not path:
            return
        try:
            from excel_loader import OpenpyxlMissingError, load_races_from_xlsx
            try:
                data = load_races_from_xlsx(path)
            except OpenpyxlMissingError as e:
                messagebox.showwarning("openpyxl未インストール", str(e))
                return
        except Exception as e:  # noqa: BLE001
            messagebox.showerror("読込エラー", str(e))
            return
        self.loaded = data
        self.file_label.config(text=f"{data['file_name']} SHA256={data['sha256']}")
        self.refresh_race_list()

    def save_snapshot(self):
        dc = self._disagreement_value()
        race_name = self.vars["race_name"].get().strip()
        record = {
            "recorded_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "source_file": self.loaded.get("file_name", ""),
            "sha256": self.loaded.get("sha256", ""),
            "model_generation": self.vars["model_generation"].get().strip() or "不明",
            "race_id": self.vars["race_id"].get().strip() or "不明",
            "race_name": race_name,
            "is_excluded_target": is_excluded_race(race_name, self.exclude_enabled.get()),
            "disagreement_count": dc,
            "disagreement_horse_numbers": parse_horse_numbers(self.vars["disagreement_horses"].get()),
            "predicted_top5": parse_horse_numbers(self.vars["predicted_top5"].get()),
            "comparison_difficulties": {
                "opponent": {
                    "count": self.vars["opp_count"].get().strip() or None,
                    "horse_numbers": parse_horse_numbers(self.vars["opp_horses"].get()),
                    "memo": self.vars["opp_memo"].get().strip(),
                    "data_missing": bool(self.opp_missing.get()),
                },
                "time": {
                    "count": self.vars["time_count"].get().strip() or None,
                    "horse_numbers": parse_horse_numbers(self.vars["time_horses"].get()),
                    "memo": self.vars["time_memo"].get().strip(),
                    "data_missing": bool(self.time_missing.get()),
                },
            },
            "judgements": {
                "disagreement": judge_disagreement(dc),
                "A": judge_disagreement(dc),
                "B": judge_disagreement(dc),
                "C": judge_c(),
            },
            "condition_version": self.condition_version.get().strip(),
        }
        try:
            self.snap_store.append_snapshot(record)
        except DuplicateSnapshotError as e:
            messagebox.showerror("二重保存エラー", str(e))
            return
        messagebox.showinfo("保存完了", "スナップショットを追記保存しました（変更・削除はできません）")
        self.refresh_compare()

    # --- レース一覧 ---
    def _build_list(self):
        f = self.tab_list
        self.place_filter = tk.StringVar(value="すべて")
        self.filter_frame = ttk.Frame(f)
        self.filter_frame.pack(fill="x", padx=8, pady=4)
        ttk.Label(f, text="レースをクリックすると出走馬を表示、ダブルクリックで事前判定入力へ送ります").pack(anchor="w", padx=8)
        self.race_listbox = tk.Listbox(f, height=12)
        self.race_listbox.pack(fill="x", padx=8, pady=4)
        self.race_listbox.bind("<<ListboxSelect>>", self._on_race_select)
        self.race_listbox.bind("<Double-Button-1>", self._on_race_send)
        ttk.Button(f, text="事前判定入力へ送る", command=self._on_race_send).pack(anchor="w", padx=8)
        self.list_text = tk.Text(f, height=16)
        self.list_text.pack(fill="both", expand=True, padx=8, pady=8)
        self._filtered_races = []

    def _rebuild_filter_buttons(self):
        for w in self.filter_frame.winfo_children():
            w.destroy()
        races = self.loaded.get("races", [])
        ttk.Label(self.filter_frame, text="競馬場:").pack(side="left")
        places = ["すべて"] + list_places(races)
        for p in places:
            count = len(races) if p == "すべて" else len(filter_races_by_place(races, p))
            ttk.Button(self.filter_frame, text=f"{p} ({count})",
                       command=lambda x=p: self._set_place_filter(x)).pack(side="left", padx=2)

    def _set_place_filter(self, place):
        self.place_filter.set(place)
        self.refresh_race_list()

    def refresh_race_list(self):
        self._rebuild_filter_buttons()
        self._filtered_races = filter_races_by_place(self.loaded.get("races", []), self.place_filter.get())
        self.race_listbox.delete(0, "end")
        for r in self._filtered_races:
            self.race_listbox.insert("end", f"{r.get('date')} {r.get('place')} {r.get('race_no')}R race_id={r.get('race_id')} {r.get('race_name')}（{len(r.get('horses', []))}頭）")
        self.list_text.delete("1.0", "end")

    def _selected_race(self):
        sel = self.race_listbox.curselection()
        if not sel or sel[0] >= len(self._filtered_races):
            return None
        return self._filtered_races[sel[0]]

    def _on_race_select(self, _event=None):
        race = self._selected_race()
        self.list_text.delete("1.0", "end")
        if race is None:
            return
        self.list_text.insert("end", f"{race.get('date')} {race.get('place')} {race.get('race_no')}R race_id={race.get('race_id')} {race.get('race_name')}\n")
        for h in race.get("horses", []):
            self.list_text.insert("end", f"  {h.get('horse_no')} {h.get('horse_name')}\n")

    def _on_race_send(self, _event=None):
        race = self._selected_race()
        if race is None:
            return
        if race.get("race_id") not in (None, "", "不明"):
            self.vars["race_id"].set(str(race["race_id"]))
        if race.get("race_name") not in (None, "", "不明"):
            self.vars["race_name"].set(str(race["race_name"]))
        self.nb.select(self.tab_pre)

    # --- レース後結果 ---
    def _build_result(self):
        f = self.tab_result
        self.rvars = {}
        for key, label in [
            ("race_id", "race_id"),
            ("finish", "確定1〜3着馬番（カンマ区切り 例 3,5,7）"),
            ("predicted_top5", "予想TOP5馬番（カンマ区切り、任意）"),
            ("ticket_type", "券種（単勝/複勝/枠連/馬連/ワイド/馬単/3連複/3連単）"),
            ("combination", "組み合わせ（例 3-5 / 3-5-7）"),
            ("amount", "金額（円）"),
            ("payout", "払戻額（円）"),
        ]:
            row = ttk.Frame(f)
            row.pack(fill="x", padx=8, pady=2)
            ttk.Label(row, text=label, width=48).pack(side="left")
            var = tk.StringVar()
            self.rvars[key] = var
            ttk.Entry(row, textvariable=var).pack(side="left", fill="x", expand=True)
        ttk.Button(f, text="結果を保存（results.jsonl へ追記）", command=self.save_result).pack(anchor="w", padx=8, pady=4)
        ttk.Button(f, text="結果CSV取込", command=self.import_results_csv).pack(anchor="w", padx=8)
        ttk.Button(f, text="チケットCSV取込", command=self.import_tickets_csv).pack(anchor="w", padx=8)
        self.result_label = ttk.Label(f, text="")
        self.result_label.pack(anchor="w", padx=8)

    def save_result(self):
        finish = parse_horse_numbers(self.rvars["finish"].get())
        top5 = parse_horse_numbers(self.rvars["predicted_top5"].get())
        capture = compute_top5_capture(finish, top5) if top5 else None
        ticket = None
        if self.rvars["ticket_type"].get().strip():
            amount_s = self.rvars["amount"].get().strip()
            payout_s = self.rvars["payout"].get().strip()
            ticket = {
                "ticket_type": self.rvars["ticket_type"].get().strip(),
                "combination": self.rvars["combination"].get().strip(),
                "amount": int(amount_s) if amount_s.isdigit() else None,
                "payout": int(payout_s) if payout_s.isdigit() else None,
                "hit": check_ticket_hit(self.rvars["ticket_type"].get().strip(), self.rvars["combination"].get().strip(), finish),
            }
        record = {
            "recorded_at": datetime.datetime.now().isoformat(timespec="seconds"),
            "race_id": self.rvars["race_id"].get().strip() or "不明",
            "finish_top3": finish,
            "predicted_top5": top5,
            "top5_capture_count": capture,
            "top5_capture_display": capture if capture is not None else "未計算",
            "tickets": [ticket] if ticket else [],
        }
        self.result_store.append_result(record)
        self.result_label.config(text=f"保存しました。的中={ticket['hit'] if ticket else '不明/未計算'} TOP5捕捉={record['top5_capture_display']}")
        self.refresh_compare()

    def import_results_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
        if not path:
            return
        with open(path, newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                finish = parse_horse_numbers(row.get("finish_top3", ""))
                top5 = parse_horse_numbers(row.get("predicted_top5", ""))
                capture = compute_top5_capture(finish, top5) if top5 else None
                self.result_store.append_result({
                    "recorded_at": datetime.datetime.now().isoformat(timespec="seconds"),
                    "race_id": row.get("race_id", "不明"),
                    "finish_top3": finish,
                    "predicted_top5": top5,
                    "top5_capture_count": capture,
                    "tickets": [],
                })
        self.refresh_compare()

    def import_tickets_csv(self):
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
        if not path:
            return
        with open(path, newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                # Tickets CSV is stored as a result record carrying one ticket,
                # keyed by race_id, so aggregation can join it.
                finish = parse_horse_numbers(row.get("finish_top3", "")) if row.get("finish_top3") else []
                amount = row.get("amount", "")
                payout = row.get("payout", "")
                self.result_store.append_result({
                    "recorded_at": datetime.datetime.now().isoformat(timespec="seconds"),
                    "race_id": row.get("race_id", "不明"),
                    "finish_top3": finish,
                    "predicted_top5": parse_horse_numbers(row.get("predicted_top5", "")),
                    "top5_capture_count": None,
                    "tickets": [{
                        "ticket_type": row.get("ticket_type", ""),
                        "combination": row.get("combination", ""),
                        "amount": int(amount) if str(amount).isdigit() else None,
                        "payout": int(payout) if str(payout).isdigit() else None,
                        "hit": check_ticket_hit(row.get("ticket_type", ""), row.get("combination", ""), finish) if finish else None,
                    }],
                })
        self.refresh_compare()

    # --- 方式比較 ---
    def _build_compare(self):
        ttk.Label(self.tab_compare, text=COMPARE_PREMISE).pack(anchor="w", padx=8, pady=4)
        ttk.Label(self.tab_compare, text=C_NOTE).pack(anchor="w", padx=8)
        self.compare_text = tk.Text(self.tab_compare, height=25)
        self.compare_text.pack(fill="both", expand=True, padx=8, pady=8)
        ttk.Button(self.tab_compare, text="再計算", command=self.refresh_compare).pack(anchor="w", padx=8)

    def refresh_compare(self):
        if not hasattr(self, "compare_text"):
            return
        agg = aggregate_methods(self.snap_store.load_all(), self.result_store.load_all())
        self.compare_text.delete("1.0", "end")
        for name in ("A", "B"):
            d = agg[name]
            self.compare_text.insert("end", f"{name}方式: 購入レース数={d['race_count']} 投資={d['investment']} 払戻={d['payout']} 損益={d['profit']} 回収率={format_rate(d['recovery_rate'])} TOP5捕捉率={format_rate(d['top5_capture_rate'])}（分母 {d['top5_recorded_races']}レース/{d['top5_denominator_slots']}頭枠） 除外={d.get('excluded_count', 0)}件\n")
        self.compare_text.insert("end", "C方式: 未算出（閾値未確定のため回収率・利益は計算しません）\n")
        self.compare_text.insert("end", f"対象外（新馬・未勝利）除外レース数: {agg['excluded_target_count']}\n")


def main():
    if tk is None:
        print("tkinter が利用できない環境です。core/storage の機能はテストで確認できます。")
        return
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    main()
