# -*- coding: utf-8 -*-
"""
02_video_inspector.py

目的
----
JRAレース動画 MP4 をOpenCVで読み込み、

1. 動画情報を取得
2. video_info.json を保存
3. 5秒ごとの代表フレームをJPEG保存
4. frame_index.csv を保存
5. 各画像へ
   ・動画時刻
   ・フレーム番号
   ・FPS
   を描画
6. Windowsの日本語パスでもJPEG保存できるようにする

想定入力
--------
race_202602040601.mp4

想定出力
--------
analysis/
├─ video_info.json
├─ frame_index.csv
└─ preview_frames/
   ├─ t_000.000_frame_000000.jpg
   ├─ t_005.000_frame_000150.jpg
   ├─ t_010.000_frame_000300.jpg
   └─ ...

必要ライブラリ
--------------
pip install opencv-python pandas numpy

実行例
------
python -u 02_video_inspector.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import cv2
import numpy as np
import pandas as pd


# ============================================================
# 設定
# ============================================================

# ------------------------------------------------------------
# 対象race_id
# ------------------------------------------------------------

TARGET = "202602040601"


# ------------------------------------------------------------
# このPythonファイルが置かれているフォルダ
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------
# 入力MP4
#
# lap/
# ├─ 02_video_inspector.py
# └─ output/
#    └─ 202602040601/
#       └─ capture_v07/
#          └─ race_202602040601.mp4
# ------------------------------------------------------------

INPUT_VIDEO = (
    BASE_DIR
    / "output"
    / TARGET
    / "capture_v07"
    / f"race_{TARGET}.mp4"
)


# ------------------------------------------------------------
# 出力先
# ------------------------------------------------------------

OUTPUT_DIR = (
    BASE_DIR
    / "output"
    / TARGET
    / "analysis"
)

PREVIEW_DIR = (
    OUTPUT_DIR
    / "preview_frames"
)

VIDEO_INFO_JSON = (
    OUTPUT_DIR
    / "video_info.json"
)

FRAME_INDEX_CSV = (
    OUTPUT_DIR
    / "frame_index.csv"
)


# ------------------------------------------------------------
# 代表フレームの間隔
# ------------------------------------------------------------

PREVIEW_INTERVAL_SEC = 5.0


# ------------------------------------------------------------
# JPEG品質
# ------------------------------------------------------------

JPEG_QUALITY = 95


# ------------------------------------------------------------
# 画像へ情報文字を書き込む
# ------------------------------------------------------------

DRAW_OVERLAY = True


# ------------------------------------------------------------
# フレーム番号
# ------------------------------------------------------------

FRAME_NUMBER_START = 0


# ------------------------------------------------------------
# 実行前に古いpreview画像を削除する
#
# True:
# 前回画像を消してから作り直す
#
# False:
# 残したまま上書き
# ------------------------------------------------------------

CLEAR_OLD_PREVIEW_FILES = True


# ============================================================
# 共通関数
# ============================================================

def now_string() -> str:
    """
    現在時刻を文字列で返す。
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def prepare_directories() -> None:
    """
    出力フォルダを作成する。
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    PREVIEW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def clear_old_preview_files() -> None:
    """
    以前作成した代表画像を削除する。

    前回実行結果と今回実行結果が混ざるのを防ぐ。
    """

    if not CLEAR_OLD_PREVIEW_FILES:
        return

    if not PREVIEW_DIR.exists():
        return

    deleted = 0

    for path in PREVIEW_DIR.glob(
        "*.jpg"
    ):

        try:

            path.unlink()

            deleted += 1

        except Exception as exc:

            print(
                f"[WARN] 古いJPEG削除失敗: "
                f"{path}"
            )

            print(
                f"       {exc}"
            )

    print(
        f"[INFO] 古いpreview JPEG削除数="
        f"{deleted}"
    )


def format_hhmmss(
    seconds: float,
) -> str:
    """
    秒数を HH:MM:SS.mmm に変換する。

    例：
    65.123
    ↓
    00:01:05.123
    """

    if seconds < 0:
        seconds = 0.0

    hours = int(
        seconds // 3600
    )

    minutes = int(
        (seconds % 3600) // 60
    )

    secs = (
        seconds
        % 60
    )

    return (
        f"{hours:02d}:"
        f"{minutes:02d}:"
        f"{secs:06.3f}"
    )


# ============================================================
# Windows日本語パス対応画像保存
# ============================================================

def imwrite_unicode(
    output_path: Path,
    image: np.ndarray,
    jpeg_quality: int = 95,
) -> bool:
    """
    Windowsの日本語パスでも保存できるようにする。

    cv2.imwrite() は環境によっては

    C:\\Users\\...\\ドキュメント\\...

    のような日本語パスで失敗する。

    そのため、

    cv2.imencode()
    ↓
    numpy.tofile()

    を使用する。

    Returns
    -------
    True
        保存成功

    False
        保存失敗
    """

    try:

        # ----------------------------------------------------
        # 保存先フォルダ確認
        # ----------------------------------------------------

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        # ----------------------------------------------------
        # JPEGエンコード
        # ----------------------------------------------------

        extension = (
            output_path.suffix.lower()
        )

        if extension not in {
            ".jpg",
            ".jpeg",
        }:

            raise ValueError(
                f"JPEG以外の拡張子です: "
                f"{extension}"
            )

        success, encoded = (
            cv2.imencode(
                ".jpg",
                image,
                [
                    cv2.IMWRITE_JPEG_QUALITY,
                    int(
                        jpeg_quality
                    ),
                ],
            )
        )

        if not success:

            print(
                "[ERROR] "
                "cv2.imencode()に失敗しました。"
            )

            return False

        # ----------------------------------------------------
        # Unicodeパス対応保存
        # ----------------------------------------------------

        encoded.tofile(
            str(
                output_path
            )
        )

        # ----------------------------------------------------
        # 実ファイル確認
        # ----------------------------------------------------

        if not output_path.exists():

            print(
                "[ERROR] JPEG保存後に"
                "ファイルが存在しません。"
            )

            return False

        if output_path.stat().st_size <= 0:

            print(
                "[ERROR] JPEGファイルサイズが0です。"
            )

            return False

        return True

    except Exception as exc:

        print()
        print(
            "[ERROR] JPEG保存例外"
        )

        print(
            f"PATH={output_path}"
        )

        print(
            f"ERROR={exc}"
        )

        return False


# ============================================================
# 動画を開く
# ============================================================

def open_video(
    video_path: Path,
) -> cv2.VideoCapture:
    """
    OpenCVで動画を開く。
    """

    if not video_path.exists():

        raise FileNotFoundError(
            "\n"
            "入力MP4が見つかりません。\n"
            "\n"
            f"探した場所:\n"
            f"{video_path}\n"
        )

    capture = cv2.VideoCapture(
        str(
            video_path
        )
    )

    if not capture.isOpened():

        raise RuntimeError(
            "\n"
            "OpenCVで動画を開けませんでした。\n"
            "\n"
            f"動画:\n"
            f"{video_path}\n"
        )

    return capture


# ============================================================
# 動画情報取得
# ============================================================

def get_video_info(
    capture: cv2.VideoCapture,
    video_path: Path,
) -> dict:
    """
    動画の基本情報を取得する。
    """

    fps = float(
        capture.get(
            cv2.CAP_PROP_FPS
        )
    )

    frame_count_float = capture.get(
        cv2.CAP_PROP_FRAME_COUNT
    )

    frame_count = int(
        round(
            frame_count_float
        )
    )

    width = int(
        capture.get(
            cv2.CAP_PROP_FRAME_WIDTH
        )
    )

    height = int(
        capture.get(
            cv2.CAP_PROP_FRAME_HEIGHT
        )
    )

    fourcc_number = int(
        capture.get(
            cv2.CAP_PROP_FOURCC
        )
    )

    fourcc = "".join(
        chr(
            (
                fourcc_number
                >> (8 * i)
            )
            & 0xFF
        )
        for i in range(4)
    )

    if fps > 0:

        duration_sec = (
            frame_count
            / fps
        )

        frame_duration_sec = (
            1.0
            / fps
        )

    else:

        duration_sec = 0.0

        frame_duration_sec = 0.0

    file_size_bytes = (
        video_path.stat().st_size
    )

    info = {
        "target": TARGET,

        "input_video": str(
            video_path
        ),

        "created_at": (
            now_string()
        ),

        "fps": fps,

        "frame_duration_sec": (
            frame_duration_sec
        ),

        "frame_count": (
            frame_count
        ),

        "width": width,

        "height": height,

        "fourcc": fourcc,

        "duration_sec": (
            duration_sec
        ),

        "duration_hhmmss": (
            format_hhmmss(
                duration_sec
            )
        ),

        "file_size_bytes": (
            file_size_bytes
        ),

        "file_size_mb": (
            file_size_bytes
            / 1024
            / 1024
        ),

        "preview_interval_sec": (
            PREVIEW_INTERVAL_SEC
        ),

        "image_save_method": (
            "cv2.imencode + numpy.tofile"
        ),
    }

    return info


# ============================================================
# 動画情報表示
# ============================================================

def print_video_info(
    info: dict,
) -> None:
    """
    ターミナルへ動画情報を表示する。
    """

    print()
    print("=" * 70)
    print("[VIDEO INFO]")
    print("=" * 70)

    print(
        f"TARGET           : "
        f"{info['target']}"
    )

    print(
        f"INPUT            : "
        f"{info['input_video']}"
    )

    print(
        f"FPS              : "
        f"{info['fps']:.6f}"
    )

    print(
        f"1フレーム時間    : "
        f"{info['frame_duration_sec']:.6f} 秒"
    )

    print(
        f"総フレーム数      : "
        f"{info['frame_count']:,}"
    )

    print(
        f"解像度            : "
        f"{info['width']} x "
        f"{info['height']}"
    )

    print(
        f"FourCC           : "
        f"{info['fourcc']}"
    )

    print(
        f"動画時間          : "
        f"{info['duration_sec']:.3f} 秒"
    )

    print(
        f"動画時間          : "
        f"{info['duration_hhmmss']}"
    )

    print(
        f"ファイルサイズ    : "
        f"{info['file_size_mb']:.2f} MB"
    )

    print(
        f"代表フレーム間隔  : "
        f"{info['preview_interval_sec']} 秒"
    )

    print(
        f"JPEG保存方式      : "
        f"{info['image_save_method']}"
    )

    print("=" * 70)


# ============================================================
# JSON保存
# ============================================================

def save_video_info(
    info: dict,
) -> None:
    """
    video_info.json を保存する。
    """

    VIDEO_INFO_JSON.write_text(
        json.dumps(
            info,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        f"[SAVE] {VIDEO_INFO_JSON}"
    )


# ============================================================
# 抽出時刻作成
# ============================================================

def build_preview_times(
    duration_sec: float,
    interval_sec: float,
) -> list[float]:
    """
    代表フレームを抽出する時刻一覧を作る。
    """

    if duration_sec <= 0:

        return []

    if interval_sec <= 0:

        raise ValueError(
            "PREVIEW_INTERVAL_SECは"
            "0より大きくしてください。"
        )

    result: list[float] = []

    current = 0.0

    while (
        current
        < duration_sec
    ):

        result.append(
            round(
                current,
                6,
            )
        )

        current += (
            interval_sec
        )

    final_time = max(
        0.0,
        duration_sec
        - 0.001,
    )

    if result:

        if (
            final_time
            - result[-1]
            > 1.0
        ):

            result.append(
                final_time
            )

    else:

        result.append(
            final_time
        )

    return result


# ============================================================
# 文字描画
# ============================================================

def draw_frame_overlay(
    frame: np.ndarray,
    time_sec: float,
    frame_number: int,
    fps: float,
) -> None:
    """
    フレーム画像へ情報を書き込む。
    """

    if not DRAW_OVERLAY:
        return

    line1 = (
        f"TIME: "
        f"{time_sec:.3f} sec"
    )

    line2 = (
        f"FRAME: "
        f"{frame_number}"
    )

    line3 = (
        f"FPS: "
        f"{fps:.3f}"
    )

    line4 = (
        format_hhmmss(
            time_sec
        )
    )

    lines = [
        line1,
        line2,
        line3,
        line4,
    ]

    font = (
        cv2.FONT_HERSHEY_SIMPLEX
    )

    font_scale = 0.55

    thickness = 1

    text_color = (
        255,
        255,
        255,
    )

    background_color = (
        0,
        0,
        0,
    )

    x = 10

    y_start = 25

    line_height = 22

    box_width = 220

    box_height = (
        line_height
        * len(
            lines
        )
        + 10
    )

    overlay = (
        frame.copy()
    )

    cv2.rectangle(
        overlay,
        (
            0,
            0,
        ),
        (
            box_width,
            box_height,
        ),
        background_color,
        thickness=-1,
    )

    alpha = 0.65

    cv2.addWeighted(
        overlay,
        alpha,
        frame,
        1 - alpha,
        0,
        frame,
    )

    for index, text in enumerate(
        lines
    ):

        y = (
            y_start
            + index
            * line_height
        )

        cv2.putText(
            frame,
            text,
            (
                x,
                y,
            ),
            font,
            font_scale,
            text_color,
            thickness,
            cv2.LINE_AA,
        )


# ============================================================
# 時刻→フレーム
# ============================================================

def time_to_frame_number(
    time_sec: float,
    fps: float,
    frame_count: int,
) -> int:
    """
    秒数 → フレーム番号
    """

    if fps <= 0:

        raise ValueError(
            "FPSが0以下です。"
        )

    frame_number = int(
        round(
            time_sec
            * fps
        )
    )

    if frame_count > 0:

        frame_number = min(
            frame_number,
            frame_count - 1,
        )

    frame_number = max(
        FRAME_NUMBER_START,
        frame_number,
    )

    return frame_number


# ============================================================
# 1枚抽出
# ============================================================

def extract_single_frame(
    capture: cv2.VideoCapture,
    time_sec: float,
    fps: float,
    frame_count: int,
) -> dict:
    """
    指定時刻のフレームを1枚保存する。
    """

    target_frame = (
        time_to_frame_number(
            time_sec=time_sec,
            fps=fps,
            frame_count=frame_count,
        )
    )

    # --------------------------------------------------------
    # 指定フレームへ移動
    # --------------------------------------------------------

    seek_success = capture.set(
        cv2.CAP_PROP_POS_FRAMES,
        target_frame,
    )

    if not seek_success:

        print(
            f"[WARN] "
            f"CAP_PROP_POS_FRAMES seek結果=False "
            f"frame={target_frame}"
        )

    success, frame = (
        capture.read()
    )

    if not success:

        return {
            "success": False,

            "requested_time_sec": (
                time_sec
            ),

            "requested_frame": (
                target_frame
            ),

            "actual_time_sec": None,

            "actual_time_hhmmss": "",

            "actual_frame": None,

            "filename": "",

            "output_path": "",

            "file_size_bytes": 0,

            "error": (
                "capture.read() failed"
            ),
        }

    # --------------------------------------------------------
    # 読み込み画像チェック
    # --------------------------------------------------------

    if frame is None:

        return {
            "success": False,

            "requested_time_sec": (
                time_sec
            ),

            "requested_frame": (
                target_frame
            ),

            "actual_time_sec": None,

            "actual_time_hhmmss": "",

            "actual_frame": None,

            "filename": "",

            "output_path": "",

            "file_size_bytes": 0,

            "error": (
                "frame is None"
            ),
        }

    if frame.size == 0:

        return {
            "success": False,

            "requested_time_sec": (
                time_sec
            ),

            "requested_frame": (
                target_frame
            ),

            "actual_time_sec": None,

            "actual_time_hhmmss": "",

            "actual_frame": None,

            "filename": "",

            "output_path": "",

            "file_size_bytes": 0,

            "error": (
                "frame.size == 0"
            ),
        }

    # --------------------------------------------------------
    # OpenCVが実際に読んだフレーム
    # --------------------------------------------------------

    actual_frame_position = int(
        capture.get(
            cv2.CAP_PROP_POS_FRAMES
        )
    )

    actual_frame = max(
        0,
        actual_frame_position - 1,
    )

    if fps > 0:

        actual_time_sec = (
            actual_frame
            / fps
        )

    else:

        actual_time_sec = (
            time_sec
        )

    # --------------------------------------------------------
    # 画像へ情報描画
    # --------------------------------------------------------

    draw_frame_overlay(
        frame=frame,
        time_sec=actual_time_sec,
        frame_number=actual_frame,
        fps=fps,
    )

    # --------------------------------------------------------
    # ファイル名
    # --------------------------------------------------------

    filename = (
        f"t_{actual_time_sec:07.3f}"
        f"_frame_{actual_frame:06d}"
        f".jpg"
    )

    output_path = (
        PREVIEW_DIR
        / filename
    )

    # --------------------------------------------------------
    # ★重要修正
    #
    # cv2.imwrite()を使用しない。
    #
    # Windows日本語パス対応：
    # cv2.imencode + numpy.tofile
    # --------------------------------------------------------

    save_success = (
        imwrite_unicode(
            output_path=output_path,
            image=frame,
            jpeg_quality=JPEG_QUALITY,
        )
    )

    if not save_success:

        return {
            "success": False,

            "requested_time_sec": (
                time_sec
            ),

            "requested_frame": (
                target_frame
            ),

            "actual_time_sec": (
                actual_time_sec
            ),

            "actual_time_hhmmss": (
                format_hhmmss(
                    actual_time_sec
                )
            ),

            "actual_frame": (
                actual_frame
            ),

            "filename": (
                filename
            ),

            "output_path": str(
                output_path
            ),

            "file_size_bytes": 0,

            "error": (
                "imwrite_unicode() failed"
            ),
        }

    file_size_bytes = (
        output_path.stat().st_size
    )

    return {
        "success": True,

        "requested_time_sec": (
            time_sec
        ),

        "requested_frame": (
            target_frame
        ),

        "actual_time_sec": (
            actual_time_sec
        ),

        "actual_time_hhmmss": (
            format_hhmmss(
                actual_time_sec
            )
        ),

        "actual_frame": (
            actual_frame
        ),

        "filename": (
            filename
        ),

        "output_path": str(
            output_path
        ),

        "file_size_bytes": (
            file_size_bytes
        ),

        "error": "",
    }


# ============================================================
# 代表フレーム抽出
# ============================================================

def extract_preview_frames(
    capture: cv2.VideoCapture,
    info: dict,
) -> pd.DataFrame:
    """
    5秒ごとの代表フレームを抽出する。
    """

    fps = float(
        info["fps"]
    )

    frame_count = int(
        info["frame_count"]
    )

    duration_sec = float(
        info["duration_sec"]
    )

    preview_times = (
        build_preview_times(
            duration_sec=duration_sec,
            interval_sec=PREVIEW_INTERVAL_SEC,
        )
    )

    print()
    print("=" * 70)
    print("[PREVIEW FRAME EXTRACTION]")
    print("=" * 70)

    print(
        f"抽出予定枚数: "
        f"{len(preview_times)}"
    )

    print(
        f"間隔: "
        f"{PREVIEW_INTERVAL_SEC} 秒"
    )

    rows: list[dict] = []

    for index, time_sec in enumerate(
        preview_times,
        start=1,
    ):

        result = (
            extract_single_frame(
                capture=capture,
                time_sec=time_sec,
                fps=fps,
                frame_count=frame_count,
            )
        )

        rows.append(
            result
        )

        if result[
            "success"
        ]:

            print(
                f"[{index:03d}/"
                f"{len(preview_times):03d}] "
                f"{result['actual_time_sec']:8.3f} sec "
                f"frame="
                f"{result['actual_frame']:6d} "
                f"size="
                f"{result['file_size_bytes'] / 1024:7.1f} KB "
                f"OK"
            )

        else:

            print(
                f"[{index:03d}/"
                f"{len(preview_times):03d}] "
                f"{time_sec:8.3f} sec "
                f"FAILED: "
                f"{result.get('error')}"
            )

    dataframe = pd.DataFrame(
        rows
    )

    return dataframe


# ============================================================
# CSV保存
# ============================================================

def save_frame_index(
    dataframe: pd.DataFrame,
) -> None:
    """
    frame_index.csv 保存
    """

    dataframe.to_csv(
        FRAME_INDEX_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    print()
    print(
        f"[SAVE] {FRAME_INDEX_CSV}"
    )


# ============================================================
# 検証
# ============================================================

def validate_results(
    dataframe: pd.DataFrame,
    info: dict,
) -> bool:
    """
    出力結果を検査する。

    Returns
    -------
    True
        全画像保存成功

    False
        1件以上失敗
    """

    print()
    print("=" * 70)
    print("[VALIDATION]")
    print("=" * 70)

    expected_count = len(
        build_preview_times(
            duration_sec=float(
                info[
                    "duration_sec"
                ]
            ),
            interval_sec=(
                PREVIEW_INTERVAL_SEC
            ),
        )
    )

    if (
        "success"
        not in dataframe.columns
    ):

        print(
            "[ERROR] success列がありません。"
        )

        return False

    actual_success_count = int(
        dataframe[
            "success"
        ].sum()
    )

    actual_failure_count = (
        len(
            dataframe
        )
        - actual_success_count
    )

    jpg_files = list(
        PREVIEW_DIR.glob(
            "*.jpg"
        )
    )

    nonzero_jpg_files = [
        path
        for path in jpg_files
        if (
            path.exists()
            and path.stat().st_size > 0
        )
    ]

    print(
        f"予定抽出数       : "
        f"{expected_count}"
    )

    print(
        f"成功数           : "
        f"{actual_success_count}"
    )

    print(
        f"失敗数           : "
        f"{actual_failure_count}"
    )

    print(
        f"JPEG実ファイル数 : "
        f"{len(jpg_files)}"
    )

    print(
        f"正常JPEG数       : "
        f"{len(nonzero_jpg_files)}"
    )

    all_ok = (
        actual_success_count
        == expected_count
        and actual_failure_count
        == 0
        and len(
            nonzero_jpg_files
        )
        == expected_count
    )

    if all_ok:

        print()
        print(
            "[OK] "
            "29枚すべて正常に保存されました。"
        )

    else:

        print()
        print(
            "[FAILED] "
            "代表フレーム抽出に"
            "失敗があります。"
        )

    print("=" * 70)

    return all_ok


# ============================================================
# Main
# ============================================================

def main() -> int:
    """
    メイン処理。
    """

    print("=" * 70)
    print("JRA Video Inspector v0.2")
    print("=" * 70)

    print()
    print(
        "対象：2026年8月9日（日）"
    )

    print(
        "2回新潟6日 1R"
    )

    print(
        f"TARGET={TARGET}"
    )

    print()
    print(
        f"INPUT:\n"
        f"{INPUT_VIDEO}"
    )

    print()
    print(
        f"OUTPUT:\n"
        f"{OUTPUT_DIR}"
    )

    capture: (
        cv2.VideoCapture
        | None
    ) = None

    try:

        # ====================================================
        # STEP 0
        # ====================================================

        print()
        print(
            "[STEP 0] "
            "出力フォルダ準備"
        )

        prepare_directories()

        clear_old_preview_files()

        # ====================================================
        # STEP 1
        # ====================================================

        print()
        print(
            "[STEP 1] "
            "MP4を開きます。"
        )

        capture = open_video(
            INPUT_VIDEO
        )

        print(
            "[OK] MP4を開きました。"
        )

        # ====================================================
        # STEP 2
        # ====================================================

        print()
        print(
            "[STEP 2] "
            "動画情報を取得します。"
        )

        info = (
            get_video_info(
                capture=capture,
                video_path=INPUT_VIDEO,
            )
        )

        print_video_info(
            info
        )

        save_video_info(
            info
        )

        # ====================================================
        # 基本検査
        # ====================================================

        if info[
            "fps"
        ] <= 0:

            raise RuntimeError(
                "FPSを取得できませんでした。"
            )

        if info[
            "frame_count"
        ] <= 0:

            raise RuntimeError(
                "総フレーム数を取得できませんでした。"
            )

        if (
            info[
                "width"
            ] <= 0
            or info[
                "height"
            ] <= 0
        ):

            raise RuntimeError(
                "動画解像度を取得できませんでした。"
            )

        # ====================================================
        # STEP 3
        # ====================================================

        print()
        print(
            "[STEP 3] "
            "代表フレームを抽出します。"
        )

        dataframe = (
            extract_preview_frames(
                capture=capture,
                info=info,
            )
        )

        # ====================================================
        # STEP 4
        # ====================================================

        print()
        print(
            "[STEP 4] "
            "frame_index.csvを保存します。"
        )

        save_frame_index(
            dataframe
        )

        # ====================================================
        # STEP 5
        # ====================================================

        print()
        print(
            "[STEP 5] "
            "出力結果を確認します。"
        )

        validation_ok = (
            validate_results(
                dataframe=dataframe,
                info=info,
            )
        )

        # ====================================================
        # 失敗ならSUCCESSにしない
        # ====================================================

        if not validation_ok:

            print()
            print("=" * 70)
            print("[FAILED]")
            print("=" * 70)

            print()
            print(
                "動画情報取得は成功しましたが、"
            )

            print(
                "代表JPEGの保存に"
                "失敗があります。"
            )

            print()
            print(
                "frame_index.csvのerror列を"
                "確認してください。"
            )

            print("=" * 70)

            return 1

        # ====================================================
        # 完了
        # ====================================================

        print()
        print("=" * 70)
        print("[SUCCESS]")
        print("=" * 70)

        print()
        print(
            "動画基本解析と"
            "代表フレーム抽出が完了しました。"
        )

        print()
        print(
            "動画情報:"
        )

        print(
            VIDEO_INFO_JSON
        )

        print()
        print(
            "フレーム一覧:"
        )

        print(
            FRAME_INDEX_CSV
        )

        print()
        print(
            "代表画像:"
        )

        print(
            PREVIEW_DIR
        )

        print()
        print(
            "次にpreview_framesを"
            "大アイコン表示してください。"
        )

        print()
        print(
            "その画像から"
        )

        print(
            "・レース開始"
        )

        print(
            "・向正面"
        )

        print(
            "・600m付近"
        )

        print(
            "・400m付近"
        )

        print(
            "・200m付近"
        )

        print(
            "・ゴール"
        )

        print()
        print(
            "の動画時刻を絞ります。"
        )

        print("=" * 70)

        return 0

    except KeyboardInterrupt:

        print()
        print(
            "[STOP] ユーザー中断"
        )

        return 1

    except Exception as exc:

        print()
        print("=" * 70)
        print("[ERROR]")
        print("=" * 70)

        print(
            str(
                exc
            )
        )

        return 1

    finally:

        if capture is not None:

            capture.release()


if __name__ == "__main__":

    sys.exit(
        main()
    )