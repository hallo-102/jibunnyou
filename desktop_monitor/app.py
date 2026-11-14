"""5秒ポーリングのWindowsデスクトップ画面。NASのファイルは変更しない。"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import queue
import sys
import threading
import time

from PySide6.QtCore import QDate, QSettings, QTimer, Qt, QUrl
from PySide6.QtGui import QColor, QDesktopServices, QFont, QFontDatabase, QIcon, QPainter, QPixmap
from PySide6.QtWidgets import (QApplication, QDateEdit, QFrame, QHBoxLayout, QLabel,
    QLineEdit, QMainWindow, QPushButton, QScrollArea, QSplitter, QTabWidget,
    QTextEdit, QVBoxLayout, QWidget)

from .state import DEFAULT_ROOT, DRIVE_URL, JST, clock, collect, present

COLORS = {"done": ("#08795e", "#e7f4ee", "完了"), "active": ("#175fa7", "#e8f1fc", "進行中"),
          "wait": ("#5b6b7b", "#edf1f5", "待機"), "warn": ("#986014", "#fff3dc", "要確認"),
          "error": ("#b42d3d", "#ffebed", "エラー"), "skip": ("#727381", "#f0f0f4", "対象外")}


def label(text: str, css: str = "", wrap: bool = False) -> QLabel:
    widget = QLabel(text)
    # NAS内の文字列をHTMLとして解釈せず、そのまま表示する。
    widget.setTextFormat(Qt.TextFormat.PlainText)
    widget.setWordWrap(wrap)
    widget.setStyleSheet(css)
    return widget


class Stage(QFrame):
    def __init__(self, number: int):
        super().__init__()
        self.setObjectName("card")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(17, 12, 17, 12)
        row = QHBoxLayout()
        self.title = label(f"{number:02d}", "font-size:16px;font-weight:600")
        self.when = label("", "color:#66758a;font-size:12px")
        self.badge = label("待機")
        row.addWidget(self.title, 1)
        row.addWidget(self.when)
        row.addWidget(self.badge)
        layout.addLayout(row)
        self.detail = label("読込待ち", "color:#53637a;font-size:13px", True)
        layout.addWidget(self.detail)
        self.number = number
        self.setMinimumHeight(76)

    def update_stage(self, value: dict):
        color, background, word = COLORS[value["status"]]
        if self.number == 1 and value["status"] == "done":
            word = "稼働中"
        self.title.setText(f"{self.number:02d}   {value['title']}")
        self.when.setText(value["when"])
        self.badge.setText(word)
        self.badge.setStyleSheet(f"color:{color};background:{background};border-radius:7px;padding:4px 10px;font-size:12px;font-weight:600")
        self.detail.setText(str(value["detail"]))
        self.setStyleSheet(f"QFrame#card{{border:1px solid #dfe5ed;border-left:4px solid {color};background:white;border-radius:10px}}")


class Window(QMainWindow):
    def __init__(self, args):
        super().__init__()
        self.setWindowTitle("JRA改善ループ｜ライブモニター")
        self.resize(1330, 940)
        self.setMinimumSize(1000, 760)
        self.settings = QSettings("Okino", "JRALoopMonitor")
        self.results = queue.Queue()
        self.busy = False
        self.started = 0.0
        self.next_poll = 0.0
        self.snapshot = None
        self.snapshot_key = None
        self.problem = ""
        self.last_date = datetime.now(JST).date()
        self.args = args
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(24, 19, 24, 14)
        layout.setSpacing(12)
        heading = QHBoxLayout()
        titles = QVBoxLayout()
        titles.addWidget(label("JRA予想・回顧・改善ループ", "font-size:25px;font-weight:700;color:#142e4a"))
        titles.addWidget(label("LIVE MONITOR  /  NASの記録から工程を見える化", "font-size:12px;color:#66758a"))
        heading.addLayout(titles, 1)
        self.now_label = label("", "font-size:14px;color:#435570")
        heading.addWidget(self.now_label)
        layout.addLayout(heading)
        controls = QHBoxLayout()
        controls.addWidget(label("NAS共有"))
        self.source = QLineEdit(args.root or self.settings.value("root", DEFAULT_ROOT))
        self.source.setMinimumWidth(260)
        controls.addWidget(self.source, 1)
        controls.addWidget(label("表示日"))
        self.day = QDateEdit(QDate.currentDate())
        self.day.setCalendarPopup(True)
        self.day.setDisplayFormat("yyyy/MM/dd")
        self.day.dateChanged.connect(self.request_refresh)
        controls.addWidget(self.day)
        self.button(controls, "今日", self.today)
        self.button(controls, "今すぐ更新", self.request_refresh)
        self.source.returnPressed.connect(self.request_refresh)
        layout.addLayout(controls)
        self.banner = label("NASへ接続しています…", "font-size:20px;font-weight:700;padding:15px;border-radius:10px;background:#e8f1fc", True)
        layout.addWidget(self.banner)
        self.freshness = label("", "font-size:12px;color:#53637a", True)
        layout.addWidget(self.freshness)
        split = QSplitter(Qt.Orientation.Horizontal)
        left = QWidget()
        flow = QVBoxLayout(left)
        flow.setContentsMargins(0, 0, 8, 0)
        flow.setSpacing(8)
        self.cards = [Stage(i) for i in range(1, 8)]
        for card in self.cards:
            flow.addWidget(card)
        flow.addStretch()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(left)
        split.addWidget(scroll)
        right = QWidget()
        side = QVBoxLayout(right)
        side.setContentsMargins(10, 0, 0, 0)
        self.next_label = self.info_card(side, "次に動くこと", "状態を取得しています")
        self.optimizer = self.info_card(side, "改善候補の探索", "実績条件を確認しています")
        self.alerts = self.info_card(side, "確認が必要なこと", "未取得")
        self.model = self.info_card(side, "本番とShadow", "本番：20260730を維持\n初期Shadow：20260817\n採用承認まで本番は変更しません。\n※候補切替は詳細JSONを確認")
        self.tabs = QTabWidget()
        self.log = QTextEdit()
        self.scheduler = QTextEdit()
        self.raw = QTextEdit()
        for widget, title in ((self.log, "朝処理ログ"), (self.scheduler, "開始ログ"), (self.raw, "詳細JSON")):
            widget.setReadOnly(True)
            widget.setFont(QFont("Consolas", 9))
            self.tabs.addTab(widget, title)
        side.addWidget(self.tabs, 1)
        self.tabs.setMinimumHeight(150)
        split.addWidget(right)
        split.setSizes([720, 520])
        layout.addWidget(split, 1)
        footer = QHBoxLayout()
        self.button(footer, "NASの保存先を開く", self.open_folder)
        self.button(footer, "Google Driveを開く", lambda: QDesktopServices.openUrl(QUrl(DRIVE_URL)))
        footer.addStretch()
        footer.addWidget(label("5秒ごとに確認・NAS状態は約1分更新｜読み取り専用", "font-size:11px;color:#66758a"))
        layout.addLayout(footer)
        layout.addWidget(label("21時：新しい夕方工程なし  ／  22時：新規工程なし  ／  22:30：NAS停止予定  ／  未完了は翌稼働日に再開", "font-size:11px;color:#66758a", True))
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.pulse)
        self.timer.start(500)
        QTimer.singleShot(0, self.request_refresh)
        if args.screenshot:
            QTimer.singleShot(4500, self.capture)
        if args.quit_after:
            QTimer.singleShot(int(args.quit_after * 1000), self.close)

    def button(self, row, text, callback):
        button = QPushButton(text)
        button.clicked.connect(callback)
        row.addWidget(button)
        return button

    def info_card(self, layout, title, text):
        frame = QFrame()
        frame.setObjectName("info")
        inner = QVBoxLayout(frame)
        inner.setContentsMargins(14, 10, 14, 10)
        inner.addWidget(label(title, "font-weight:700;font-size:13px;color:#173954"))
        body = label(text, "font-size:13px;color:#53637a", True)
        inner.addWidget(body)
        layout.addWidget(frame)
        return body

    def key(self):
        return self.source.text().strip(), self.day.date().toString("yyyyMMdd")

    def today(self):
        self.day.setDate(QDate.currentDate())
        self.request_refresh()

    def request_refresh(self):
        if self.busy:
            return
        source, day = self.key()
        if not self.args.root:
            self.settings.setValue("root", source)
        self.busy = True
        self.started = time.monotonic()
        key = (source, day)

        def work():
            # SMBの応答待ちでGUIを止めず、同時読込は常に1本に制限する。
            try:
                result, error = collect(source, day), ""
            except Exception as exc:
                result, error = None, f"{type(exc).__name__}: {exc}"
            self.results.put((key, result, error))

        threading.Thread(target=work, daemon=True).start()

    def pulse(self):
        now = datetime.now(JST)
        self.now_label.setText(now.strftime("%Y/%m/%d  %H:%M:%S  JST"))
        if now.date() != self.last_date:
            # 今日を表示していた画面だけ、日付跨ぎで翌日へ追従する。
            if self.day.date().toString("yyyyMMdd") == self.last_date.strftime("%Y%m%d"):
                self.day.setDate(QDate.currentDate())
            self.last_date = now.date()
        try:
            key, result, error = self.results.get_nowait()
            self.busy = False
            self.next_poll = time.monotonic() + 5
            if key == self.key():
                self.problem = error
                if result is not None:
                    self.snapshot, self.snapshot_key = result, key
                    self.render_logs(result)
        except queue.Empty:
            pass
        if self.busy and time.monotonic() - self.started > 15:
            self.problem = "NASの応答待ちが15秒を超えています。取得値は最新ではありません。"
        valid = self.snapshot is not None and self.snapshot_key == self.key()
        if valid:
            view = present(self.snapshot, now)
            for card, stage in zip(self.cards, view["stages"]):
                card.update_stage(stage)
            self.next_label.setText(view["next"])
            opt = view["optimizer"]
            opt_state = {"WAITING_SAMPLE": "実績の蓄積待ち", "WAITING_NON_RACEDAY": "非開催日・実行時間帯待ち", "WAITING_NEXT_DAY": "次の探索日待ち", "FAILED": "探索エラー"}.get(opt.get("status"), opt.get("status", "状態未取得"))
            self.optimizer.setText(f"{opt_state}\n条件：8開催日・300レース・3連複50対象レース\n非開催日07〜12時に開始／本番の自動昇格なし")
            self.alerts.setText("\n".join(view["issues"])[:1800] or "工程のエラー記録はありません。\n待機は完了を意味しません。")
            color, background, _ = COLORS[view["tone"]]
            self.banner.setText(view["headline"])
            self.banner.setStyleSheet(f"font-size:20px;font-weight:700;padding:15px;border-radius:10px;color:{color};background:{background}")
            observed = self.snapshot.get("snapshot_published_at")
            self.freshness.setText(f"ループ状態更新：{view['heartbeat']}　｜　NAS観測：{clock(observed)}　｜　PC取得：{clock(self.snapshot['collected_at'])}")
        else:
            self.banner.setText("NASの状態を取得しています…")
            self.freshness.setText("接続先・表示日を切り替えた場合は、新しい取得が完了するまで状態を表示しません。")
            for card in self.cards:
                card.update_stage({"title": "取得待ち", "when": "", "status": "wait", "detail": "状態未取得"})
            for field in (self.next_label, self.optimizer, self.alerts):
                field.setText("状態未取得")
            for field in (self.log, self.scheduler, self.raw):
                field.clear()
        if self.problem:
            self.banner.setText("接続を確認してください ／ 前回の取得値を表示" if valid else "NASへ接続できません")
            self.banner.setStyleSheet("font-size:20px;font-weight:700;padding:15px;border-radius:10px;color:#986014;background:#fff3dc")
            self.alerts.setText(self.problem + "\nNAS電源・ネットワーク・共有フォルダへのアクセスを確認してください。SSHは使用しません。")
        if not self.busy and time.monotonic() >= self.next_poll:
            self.request_refresh()

    def render_logs(self, result):
        for widget, key in ((self.log, "daily_log"), (self.scheduler, "scheduler_log")):
            text = result.get(key, "")
            if widget.toPlainText() != text:
                widget.setPlainText(text)
                widget.verticalScrollBar().setValue(widget.verticalScrollBar().maximum())
        self.raw.setPlainText(json.dumps({k: v for k, v in result.items() if not k.endswith("_log")}, ensure_ascii=False, indent=2))

    def open_folder(self):
        # ファイルを開く操作だけを提供し、NASジョブの開始・停止は行わない。
        target = str(Path(self.source.text().strip()) / "data/output/continuous_improvement")
        QDesktopServices.openUrl(QUrl.fromLocalFile(target))

    def capture(self):
        self.grab().save(str(Path(self.args.screenshot).resolve()))


def main():
    parser = argparse.ArgumentParser(description="JRA改善ループのWindowsモニター")
    parser.add_argument("--root", help="NAS共有フォルダのパス")
    parser.add_argument("--screenshot", help="表示確認用PNGの保存先")
    parser.add_argument("--quit-after", type=float, help="表示確認時の終了秒数")
    args = parser.parse_args()
    app = QApplication(sys.argv[:1])
    # offscreen検証でも日本語を描画できるよう、Windowsの既存フォントを登録する。
    font_family = "Yu Gothic UI"
    if os.name == "nt":
        for font_path in ("C:/Windows/Fonts/YuGothR.ttc", "C:/Windows/Fonts/segoeui.ttf"):
            font_id = QFontDatabase.addApplicationFont(font_path)
            families = QFontDatabase.applicationFontFamilies(font_id)
            if families and "YuGoth" in font_path:
                font_family = families[0]
    app.setFont(QFont(font_family, 10))
    app.setStyle("Fusion")
    app.setStyleSheet("QMainWindow,QWidget{background:#f4f7fb;color:#243c55} QLabel{background:transparent} QLineEdit,QDateEdit{background:white;border:1px solid #d5dfe9;border-radius:6px;padding:7px} QPushButton{background:white;border:1px solid #cad8e6;border-radius:6px;padding:8px 12px} QPushButton:hover{background:#e6eef8} QFrame#info{background:white;border:1px solid #dfe5ed;border-radius:9px} QTextEdit{background:white;border:1px solid #dfe5ed;border-radius:6px} QTabBar::tab{padding:8px;background:#e8eef5} QTabBar::tab:selected{background:white}")
    icon = QPixmap(128, 128)
    icon.fill(QColor("#173954"))
    painter = QPainter(icon)
    painter.setPen(QColor("#73dfb2"))
    painter.setFont(QFont("Segoe UI", 38, QFont.Weight.Bold))
    painter.drawText(icon.rect(), Qt.AlignmentFlag.AlignCenter, "JRA")
    painter.end()
    app.setWindowIcon(QIcon(icon))
    window = Window(args)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
