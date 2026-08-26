# -*- coding: utf-8 -*-
"""
03_extract_frame_range.py

目的
----
JRAレース動画から、指定した時間範囲だけを
1フレーム単位でJPEG画像として連続抽出する。

今回の想定
----------
対象：
2026年8月9日（日）
2回新潟6日 1R

race_id:
202602040601

動画：
race_202602040601.mp4

抽出範囲：
93.0秒 ～ 97.0秒

FPS：
30fps

想定フレーム：
93.000秒 = frame 2790
97.000秒 = frame 2910

約121枚を抽出する。


出力例
------
frame_range_093.000_097.000/
├─ frame_002790_t_093.000.jpg
├─ frame_002791_t_093.033.jpg
├─ frame_002792_t_093.067.jpg
├─ ...
└─ frame_002910_t_097.000.jpg

さらに、
frame_range_index.csv
も保存する。


必要ライブラリ
--------------
pip install opencv-python pandas numpy


実行
----
python -u 03_extract_frame_range.py
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
# このPythonファイルが置いてあるフォルダ
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------
# 入力動画
# ------------------------------------------------------------

INPUT_VIDEO = (
    BASE_DIR
    / "output"
    / TARGET
    / "capture_v07"
    / f"race_{TARGET}.mp4"
)


# ------------------------------------------------------------
# 抽出開始・終了時刻
#
# 今回は600m標識候補の93～97秒
# ------------------------------------------------------------

START_SEC = 93.0

END_SEC = 97.0


# ------------------------------------------------------------
# 出力先
# ------------------------------------------------------------

RANGE_DIR_NAME = (
    f"frame_range_"
    f"{START_SEC:07.3f}_"
    f"{END_SEC:07.3f}"
)

OUTPUT_DIR = (
    BASE_DIR
    / "output"
    / TARGET
    / "analysis"
    / RANGE_DIR_NAME
)

FRAME_INDEX_CSV = (
    OUTPUT_DIR
    / "frame_range_index.csv"
)

RANGE_INFO_JSON = (
    OUTPUT_DIR
    / "frame_range_info.json"
)


# ------------------------------------------------------------
# JPEG品質
# ------------------------------------------------------------

JPEG_QUALITY = 95


# ------------------------------------------------------------
# 画像に情報を表示する
# ------------------------------------------------------------

DRAW_OVERLAY = True


# ------------------------------------------------------------
# 古い画像を削除してから作る
# ------------------------------------------------------------

CLEAR_OLD_FILES = True


# ------------------------------------------------------------
# END_SECを含めるか
#
# True：
# 97.000秒のフレームも含む
#
# False：
# 97.000秒直前まで
# ------------------------------------------------------------

INCLUDE_END_FRAME = True


# ============================================================
# 共通
# ============================================================

def now_string() -> str:
    """
    現在日時。
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def format_hhmmss(
    seconds: float,
) -> str:
    """
    秒数を HH:MM:SS.mmm に変換。
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


def prepare_output_dir() -> None:
    """
    出力フォルダ作成。
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def clear_old_files() -> None:
    """
    前回のJPEGとCSV等を削除。
    """

    if not CLEAR_OLD_FILES:
        return

    if not OUTPUT_DIR.exists():
        return

    deleted = 0

    for path in OUTPUT_DIR.glob(
        "*.jpg"
    ):

        try:

            path.unlink()

            deleted += 1

        except Exception as exc:

            print(
                f"[WARN] 削除失敗: {path}"
            )

            print(
                f"       {exc}"
            )

    print(
        f"[INFO] 古いJPEG削除数={deleted}"
    )


# ============================================================
# 日本語パス対応JPEG保存
# ============================================================

def imwrite_unicode(
    output_path: Path,
    image: np.ndarray,
    jpeg_quality: int = 95,
) -> bool:
    """
    Windows日本語パス対応JPEG保存。

    cv2.imwrite()ではなく、
    cv2.imencode() + numpy.tofile()
    を使用する。
    """

    try:

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
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
                "[ERROR] cv2.imencode失敗"
            )

            return False

        encoded.tofile(
            str(
                output_path
            )
        )

        if not output_path.exists():

            print(
                "[ERROR] 保存後にJPEGが存在しません。"
            )

            return False

        if output_path.stat().st_size <= 0:

            print(
                "[ERROR] JPEGサイズが0です。"
            )

            return False

        return True

    except Exception as exc:

        print(
            "[ERROR] JPEG保存失敗"
        )

        print(
            f"PATH={output_path}"
        )

        print(
            f"ERROR={exc}"
        )

        return False


# ============================================================
# 動画
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
            "OpenCVで動画を開けません。\n"
            "\n"
            f"{video_path}\n"
        )

    return capture


def get_video_info(
    capture: cv2.VideoCapture,
) -> dict:
    """
    動画基本情報取得。
    """

    fps = float(
        capture.get(
            cv2.CAP_PROP_FPS
        )
    )

    frame_count = int(
        round(
            capture.get(
                cv2.CAP_PROP_FRAME_COUNT
            )
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

    if fps <= 0:

        raise RuntimeError(
            "FPSを取得できません。"
        )

    if frame_count <= 0:

        raise RuntimeError(
            "総フレーム数を取得できません。"
        )

    duration_sec = (
        frame_count
        / fps
    )

    return {
        "fps": fps,
        "frame_count": frame_count,
        "width": width,
        "height": height,
        "duration_sec": (
            duration_sec
        ),
        "frame_duration_sec": (
            1.0
            / fps
        ),
    }


# ============================================================
# 時刻とフレーム番号
# ============================================================

def sec_to_frame(
    seconds: float,
    fps: float,
) -> int:
    """
    秒 → フレーム番号。

    30fpsなら

    93秒
    ↓
    2790
    """

    return int(
        round(
            seconds
            * fps
        )
    )


def frame_to_sec(
    frame_number: int,
    fps: float,
) -> float:
    """
    フレーム番号 → 秒。
    """

    return (
        frame_number
        / fps
    )


# ============================================================
# 抽出範囲チェック
# ============================================================

def validate_range(
    info: dict,
) -> tuple[
    int,
    int,
]:
    """
    START_SECとEND_SECを検査し、
    開始・終了フレームを返す。
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

    if START_SEC < 0:

        raise ValueError(
            "START_SECは0以上にしてください。"
        )

    if END_SEC <= START_SEC:

        raise ValueError(
            "END_SECはSTART_SECより"
            "大きくしてください。"
        )

    if START_SEC >= duration_sec:

        raise ValueError(
            f"START_SEC={START_SEC}秒は"
            f"動画長={duration_sec:.3f}秒を"
            "超えています。"
        )

    if END_SEC > duration_sec:

        print()
        print(
            "[WARN] END_SECが動画長を超えているため、"
        )

        print(
            f"{END_SEC:.3f}秒"
        )

        print(
            "↓"
        )

        print(
            f"{duration_sec:.3f}秒"
        )

        print(
            "へ補正します。"
        )

        end_sec_effective = (
            duration_sec
        )

    else:

        end_sec_effective = (
            END_SEC
        )

    start_frame = (
        sec_to_frame(
            START_SEC,
            fps,
        )
    )

    end_frame = (
        sec_to_frame(
            end_sec_effective,
            fps,
        )
    )

    start_frame = max(
        0,
        start_frame,
    )

    end_frame = min(
        frame_count - 1,
        end_frame,
    )

    if not INCLUDE_END_FRAME:

        end_frame -= 1

    if end_frame < start_frame:

        raise RuntimeError(
            "抽出範囲のフレーム計算に失敗しました。"
        )

    return (
        start_frame,
        end_frame,
    )


# ============================================================
# Overlay
# ============================================================

def draw_overlay(
    frame: np.ndarray,
    frame_number: int,
    fps: float,
) -> None:
    """
    画像左上に情報表示。
    """

    if not DRAW_OVERLAY:
        return

    time_sec = (
        frame_to_sec(
            frame_number,
            fps,
        )
    )

    lines = [
        f"TIME: {time_sec:.3f} sec",
        f"FRAME: {frame_number}",
        f"FPS: {fps:.3f}",
        format_hhmmss(
            time_sec
        ),
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
        len(
            lines
        )
        * line_height
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

    cv2.addWeighted(
        overlay,
        0.65,
        frame,
        0.35,
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
# 連続フレーム抽出
# ============================================================

def extract_frame_range(
    capture: cv2.VideoCapture,
    info: dict,
    start_frame: int,
    end_frame: int,
) -> pd.DataFrame:
    """
    指定範囲の全フレームを連続抽出する。
    """

    fps = float(
        info["fps"]
    )

    expected_count = (
        end_frame
        - start_frame
        + 1
    )

    print()
    print("=" * 70)
    print("[FRAME RANGE EXTRACTION]")
    print("=" * 70)

    print(
        f"START_SEC   = "
        f"{START_SEC:.3f}"
    )

    print(
        f"END_SEC     = "
        f"{END_SEC:.3f}"
    )

    print(
        f"START_FRAME = "
        f"{start_frame}"
    )

    print(
        f"END_FRAME   = "
        f"{end_frame}"
    )

    print(
        f"抽出予定枚数 = "
        f"{expected_count}"
    )

    print(
        f"FPS         = "
        f"{fps:.3f}"
    )

    print(
        f"1フレーム   = "
        f"{1.0 / fps:.6f}秒"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # 開始フレームへ移動
    # --------------------------------------------------------

    seek_success = capture.set(
        cv2.CAP_PROP_POS_FRAMES,
        start_frame,
    )

    if not seek_success:

        print(
            "[WARN] "
            "開始フレームへのseek結果=False"
        )

    rows: list[dict] = []

    # --------------------------------------------------------
    # start_frame ～ end_frameまで順番に読む
    #
    # 1枚ずつseekするより、
    # 連続read()の方が高速で安定。
    # --------------------------------------------------------

    for index, expected_frame in enumerate(
        range(
            start_frame,
            end_frame + 1,
        ),
        start=1,
    ):

        success, frame = (
            capture.read()
        )

        if not success:

            rows.append(
                {
                    "success": False,
                    "frame_number": (
                        expected_frame
                    ),
                    "time_sec": (
                        frame_to_sec(
                            expected_frame,
                            fps,
                        )
                    ),
                    "time_hhmmss": (
                        format_hhmmss(
                            frame_to_sec(
                                expected_frame,
                                fps,
                            )
                        )
                    ),
                    "filename": "",
                    "output_path": "",
                    "file_size_bytes": 0,
                    "error": (
                        "capture.read() failed"
                    ),
                }
            )

            print(
                f"[{index:03d}/"
                f"{expected_count:03d}] "
                f"frame={expected_frame} "
                f"FAILED"
            )

            continue

        if frame is None:

            rows.append(
                {
                    "success": False,
                    "frame_number": (
                        expected_frame
                    ),
                    "time_sec": (
                        frame_to_sec(
                            expected_frame,
                            fps,
                        )
                    ),
                    "time_hhmmss": (
                        format_hhmmss(
                            frame_to_sec(
                                expected_frame,
                                fps,
                            )
                        )
                    ),
                    "filename": "",
                    "output_path": "",
                    "file_size_bytes": 0,
                    "error": (
                        "frame is None"
                    ),
                }
            )

            continue

        # ----------------------------------------------------
        # 時刻
        # ----------------------------------------------------

        time_sec = (
            frame_to_sec(
                expected_frame,
                fps,
            )
        )

        # ----------------------------------------------------
        # Overlay
        # ----------------------------------------------------

        draw_overlay(
            frame=frame,
            frame_number=expected_frame,
            fps=fps,
        )

        # ----------------------------------------------------
        # ファイル名
        # ----------------------------------------------------

        filename = (
            f"frame_"
            f"{expected_frame:06d}"
            f"_t_"
            f"{time_sec:07.3f}"
            f".jpg"
        )

        output_path = (
            OUTPUT_DIR
            / filename
        )

        # ----------------------------------------------------
        # 保存
        # ----------------------------------------------------

        save_success = (
            imwrite_unicode(
                output_path=output_path,
                image=frame,
                jpeg_quality=JPEG_QUALITY,
            )
        )

        if save_success:

            file_size_bytes = (
                output_path.stat().st_size
            )

            rows.append(
                {
                    "success": True,
                    "frame_number": (
                        expected_frame
                    ),
                    "time_sec": (
                        time_sec
                    ),
                    "time_hhmmss": (
                        format_hhmmss(
                            time_sec
                        )
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
            )

            print(
                f"[{index:03d}/"
                f"{expected_count:03d}] "
                f"frame="
                f"{expected_frame:06d} "
                f"time="
                f"{time_sec:7.3f} "
                f"size="
                f"{file_size_bytes / 1024:6.1f}KB "
                f"OK"
            )

        else:

            rows.append(
                {
                    "success": False,
                    "frame_number": (
                        expected_frame
                    ),
                    "time_sec": (
                        time_sec
                    ),
                    "time_hhmmss": (
                        format_hhmmss(
                            time_sec
                        )
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
            )

    return pd.DataFrame(
        rows
    )


# ============================================================
# CSV
# ============================================================

def save_frame_index(
    dataframe: pd.DataFrame,
) -> None:
    """
    CSV保存。
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
# JSON
# ============================================================

def save_range_info(
    info: dict,
    start_frame: int,
    end_frame: int,
    dataframe: pd.DataFrame,
) -> None:
    """
    抽出条件をJSON保存。
    """

    success_count = int(
        dataframe[
            "success"
        ].sum()
    )

    failure_count = (
        len(
            dataframe
        )
        - success_count
    )

    data = {
        "target": TARGET,

        "created_at": (
            now_string()
        ),

        "input_video": str(
            INPUT_VIDEO
        ),

        "output_dir": str(
            OUTPUT_DIR
        ),

        "start_sec": (
            START_SEC
        ),

        "end_sec": (
            END_SEC
        ),

        "fps": (
            info["fps"]
        ),

        "frame_duration_sec": (
            info[
                "frame_duration_sec"
            ]
        ),

        "start_frame": (
            start_frame
        ),

        "end_frame": (
            end_frame
        ),

        "expected_frame_count": (
            end_frame
            - start_frame
            + 1
        ),

        "success_count": (
            success_count
        ),

        "failure_count": (
            failure_count
        ),

        "width": (
            info["width"]
        ),

        "height": (
            info["height"]
        ),

        "video_duration_sec": (
            info["duration_sec"]
        ),
    }

    RANGE_INFO_JSON.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"[SAVE] {RANGE_INFO_JSON}"
    )


# ============================================================
# 検証
# ============================================================

def validate_results(
    dataframe: pd.DataFrame,
    start_frame: int,
    end_frame: int,
) -> bool:
    """
    抽出結果検証。
    """

    expected_count = (
        end_frame
        - start_frame
        + 1
    )

    success_count = int(
        dataframe[
            "success"
        ].sum()
    )

    failure_count = (
        len(
            dataframe
        )
        - success_count
    )

    jpg_files = list(
        OUTPUT_DIR.glob(
            "*.jpg"
        )
    )

    valid_jpg_files = [
        path
        for path in jpg_files
        if (
            path.exists()
            and path.stat().st_size > 0
        )
    ]

    print()
    print("=" * 70)
    print("[VALIDATION]")
    print("=" * 70)

    print(
        f"予定枚数        : "
        f"{expected_count}"
    )

    print(
        f"成功数          : "
        f"{success_count}"
    )

    print(
        f"失敗数          : "
        f"{failure_count}"
    )

    print(
        f"JPEG実ファイル数: "
        f"{len(jpg_files)}"
    )

    print(
        f"正常JPEG数      : "
        f"{len(valid_jpg_files)}"
    )

    all_ok = (
        expected_count
        == success_count
        == len(
            valid_jpg_files
        )
        and failure_count == 0
    )

    if all_ok:

        print()
        print(
            "[OK] "
            "全フレーム正常抽出"
        )

    else:

        print()
        print(
            "[FAILED] "
            "一部フレームに失敗があります。"
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
    print("JRA Frame Range Extractor v0.1")
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
        "抽出範囲:"
    )

    print(
        f"{START_SEC:.3f}秒"
    )

    print(
        "～"
    )

    print(
        f"{END_SEC:.3f}秒"
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

        prepare_output_dir()

        clear_old_files()

        # ====================================================
        # STEP 1
        # ====================================================

        print()
        print(
            "[STEP 1] "
            "MP4を開く"
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
            "動画情報取得"
        )

        info = get_video_info(
            capture
        )

        print()
        print("=" * 70)
        print("[VIDEO INFO]")
        print("=" * 70)

        print(
            f"FPS              : "
            f"{info['fps']:.6f}"
        )

        print(
            f"1フレーム        : "
            f"{info['frame_duration_sec']:.6f}秒"
        )

        print(
            f"総フレーム数      : "
            f"{info['frame_count']:,}"
        )

        print(
            f"動画時間          : "
            f"{info['duration_sec']:.3f}秒"
        )

        print(
            f"解像度            : "
            f"{info['width']}x"
            f"{info['height']}"
        )

        print("=" * 70)

        # ====================================================
        # STEP 3
        # ====================================================

        print()
        print(
            "[STEP 3] "
            "抽出範囲を計算"
        )

        (
            start_frame,
            end_frame,
        ) = validate_range(
            info
        )

        print()
        print(
            f"START_FRAME="
            f"{start_frame}"
        )

        print(
            f"END_FRAME="
            f"{end_frame}"
        )

        print(
            f"FRAME_COUNT="
            f"{end_frame - start_frame + 1}"
        )

        # ====================================================
        # STEP 4
        # ====================================================

        print()
        print(
            "[STEP 4] "
            "全フレーム抽出"
        )

        dataframe = (
            extract_frame_range(
                capture=capture,
                info=info,
                start_frame=start_frame,
                end_frame=end_frame,
            )
        )

        # ====================================================
        # STEP 5
        # ====================================================

        print()
        print(
            "[STEP 5] "
            "CSV保存"
        )

        save_frame_index(
            dataframe
        )

        # ====================================================
        # STEP 6
        # ====================================================

        print()
        print(
            "[STEP 6] "
            "JSON保存"
        )

        save_range_info(
            info=info,
            start_frame=start_frame,
            end_frame=end_frame,
            dataframe=dataframe,
        )

        # ====================================================
        # STEP 7
        # ====================================================

        print()
        print(
            "[STEP 7] "
            "結果検証"
        )

        validation_ok = (
            validate_results(
                dataframe=dataframe,
                start_frame=start_frame,
                end_frame=end_frame,
            )
        )

        if not validation_ok:

            print()
            print("=" * 70)
            print("[FAILED]")
            print("=" * 70)

            print(
                "フレーム抽出に"
                "失敗があります。"
            )

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
            "93～97秒の全フレームを"
            "1フレーム単位で抽出しました。"
        )

        print()
        print(
            f"保存先:\n"
            f"{OUTPUT_DIR}"
        )

        print()
        print(
            f"CSV:\n"
            f"{FRAME_INDEX_CSV}"
        )

        print()
        print(
            "次の確認:"
        )

        print()
        print(
            "Windowsエクスプローラーで"
            "大アイコン表示にして、"
        )

        print(
            "600m標識と先頭馬の位置を"
            "連続画像で確認します。"
        )

        print()
        print(
            "特に見る範囲:"
        )

        print(
            "frame 2820～2880"
        )

        print(
            "94.000～96.000秒"
        )

        print()
        print(
            "この中から"
        )

        print(
            "・600m標識が最も明瞭なフレーム"
        )

        print(
            "・先頭馬が標識へ到達する直前"
        )

        print(
            "・先頭馬が標識を通過した直後"
        )

        print()
        print(
            "を探します。"
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