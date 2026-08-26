# -*- coding: utf-8 -*-
"""
04_mark_600m_line.py

目的
----
600m標識が見えている連続フレームについて、

1. 600m標識の位置を人間がクリック
2. 先頭5番の「鼻先」をクリック
3. 600m基準線を画像へ描画
4. 鼻先と基準線の左右関係を計算
5. annotated画像を保存
6. CSVへ判定結果を保存

するPoCコードです。


今回の対象
----------
2026年8月9日（日）
2回新潟6日 1R

race_id:
202602040601

対象フレーム：
2828 ～ 2835

時刻：
94.267秒 ～ 94.500秒


重要
----
今回のレース映像では馬は概ね

右 → 左

へ走っています。

したがって、

鼻先X > 600m線X
    → まだ600m地点の手前

鼻先X ≒ 600m線X
    → 600m地点付近

鼻先X < 600m線X
    → 600m地点を通過済み

としてPoC判定します。


ただしこれは「画面上の簡易判定」です。

最終的にはカメラ角度や透視投影を考慮した
基準線補正が必要です。


操作方法
--------
画像が1枚ずつ表示されます。

① 600m標識の中央を左クリック

② 先頭5番の鼻先を左クリック

③ Enterキー
   → 保存して次のフレームへ

Rキー
   → クリック位置をリセット

Sキー
   → そのフレームをスキップ

ESCキー
   → 処理終了


必要ライブラリ
--------------
pip install opencv-python pandas numpy


実行
----
python -u 04_mark_600m_line.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import cv2
import numpy as np
import pandas as pd


# ============================================================
# 設定
# ============================================================

# ------------------------------------------------------------
# race_id
# ------------------------------------------------------------

TARGET = "202602040601"


# ------------------------------------------------------------
# このPythonファイルの場所
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent


# ------------------------------------------------------------
# 入力フレームフォルダ
#
# 03_extract_frame_range.py が作成したフォルダ
# ------------------------------------------------------------

INPUT_DIR = (
    BASE_DIR
    / "output"
    / TARGET
    / "analysis"
    / "frame_range_093.000_097.000"
)


# ------------------------------------------------------------
# 今回見るフレーム
# ------------------------------------------------------------

START_FRAME = 2828

END_FRAME = 2835


# ------------------------------------------------------------
# 出力
# ------------------------------------------------------------

OUTPUT_DIR = (
    BASE_DIR
    / "output"
    / TARGET
    / "analysis"
    / "mark_600m"
)

ANNOTATED_DIR = (
    OUTPUT_DIR
    / "annotated"
)

RESULT_CSV = (
    OUTPUT_DIR
    / "600m_mark_result.csv"
)

RESULT_JSON = (
    OUTPUT_DIR
    / "600m_mark_result.json"
)


# ------------------------------------------------------------
# 走行方向
#
# 今回：
# 右 → 左
# ------------------------------------------------------------

RUN_DIRECTION = "right_to_left"


# ------------------------------------------------------------
# 「線上」と判定する許容ピクセル
#
# 鼻先と標識Xの差が±3px以内なら
# AT_LINE
# ------------------------------------------------------------

LINE_TOLERANCE_PX = 3


# ------------------------------------------------------------
# JPEG品質
# ------------------------------------------------------------

JPEG_QUALITY = 95


# ------------------------------------------------------------
# UI表示倍率
#
# 640x360なので通常は1.5程度が見やすい
# ------------------------------------------------------------

DISPLAY_SCALE = 1.5


# ------------------------------------------------------------
# 過去の結果を削除して新規実行
# ------------------------------------------------------------

CLEAR_OLD_ANNOTATED = True


# ============================================================
# グローバルクリック状態
# ============================================================

current_pole_point: Optional[tuple[int, int]] = None

current_nose_point: Optional[tuple[int, int]] = None


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


def prepare_directories() -> None:
    """
    出力フォルダ作成。
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    ANNOTATED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def clear_old_files() -> None:
    """
    古いannotated画像を削除。
    """

    if not CLEAR_OLD_ANNOTATED:
        return

    deleted = 0

    for path in ANNOTATED_DIR.glob(
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
        f"[INFO] 古いannotated画像削除数={deleted}"
    )


# ============================================================
# 日本語パス対応
# ============================================================

def imread_unicode(
    image_path: Path,
) -> Optional[np.ndarray]:
    """
    日本語を含むWindowsパスでも画像を読み込む。
    """

    try:

        data = np.fromfile(
            str(
                image_path
            ),
            dtype=np.uint8,
        )

        image = cv2.imdecode(
            data,
            cv2.IMREAD_COLOR,
        )

        return image

    except Exception as exc:

        print(
            f"[ERROR] 画像読込失敗: {image_path}"
        )

        print(
            exc
        )

        return None


def imwrite_unicode(
    output_path: Path,
    image: np.ndarray,
    jpeg_quality: int = 95,
) -> bool:
    """
    日本語を含むWindowsパスでもJPEG保存する。
    """

    try:

        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        success, encoded = cv2.imencode(
            ".jpg",
            image,
            [
                cv2.IMWRITE_JPEG_QUALITY,
                jpeg_quality,
            ],
        )

        if not success:

            return False

        encoded.tofile(
            str(
                output_path
            )
        )

        return (
            output_path.exists()
            and output_path.stat().st_size > 0
        )

    except Exception as exc:

        print(
            f"[ERROR] JPEG保存失敗: {output_path}"
        )

        print(
            exc
        )

        return False


# ============================================================
# フレームファイル検索
# ============================================================

def find_frame_file(
    frame_number: int,
) -> Optional[Path]:
    """
    指定フレーム番号のJPEGを探す。

    例：

    frame_002828_t_094.267.jpg
    """

    pattern = (
        f"frame_{frame_number:06d}_t_*.jpg"
    )

    files = list(
        INPUT_DIR.glob(
            pattern
        )
    )

    if not files:

        return None

    return sorted(
        files
    )[0]


def build_frame_list() -> list[Path]:
    """
    START_FRAME～END_FRAMEの画像を取得。
    """

    result: list[Path] = []

    for frame_number in range(
        START_FRAME,
        END_FRAME + 1,
    ):

        path = find_frame_file(
            frame_number
        )

        if path is None:

            print(
                f"[WARN] frame {frame_number} が見つかりません。"
            )

            continue

        result.append(
            path
        )

    return result


# ============================================================
# ファイル名解析
# ============================================================

def parse_frame_info(
    image_path: Path,
) -> tuple[
    int,
    float,
]:
    """
    ファイル名から

    frame番号
    時刻

    を取得。

    frame_002828_t_094.267.jpg
    """

    stem = image_path.stem

    parts = stem.split(
        "_"
    )

    # frame_002828_t_094.267

    frame_number = int(
        parts[1]
    )

    time_sec = float(
        parts[3]
    )

    return (
        frame_number,
        time_sec,
    )


# ============================================================
# マウス操作
# ============================================================

def mouse_callback(
    event,
    x,
    y,
    flags,
    param,
) -> None:
    """
    左クリック処理。

    1回目
    → 600m標識

    2回目
    → 5番鼻先
    """

    global current_pole_point
    global current_nose_point

    if event != cv2.EVENT_LBUTTONDOWN:
        return

    # 表示倍率を元座標へ戻す
    original_x = int(
        round(
            x
            / DISPLAY_SCALE
        )
    )

    original_y = int(
        round(
            y
            / DISPLAY_SCALE
        )
    )

    # --------------------------------------------------------
    # 1回目
    # --------------------------------------------------------

    if current_pole_point is None:

        current_pole_point = (
            original_x,
            original_y,
        )

        print(
            f"[CLICK] 600m標識 "
            f"x={original_x}, "
            f"y={original_y}"
        )

        return

    # --------------------------------------------------------
    # 2回目
    # --------------------------------------------------------

    if current_nose_point is None:

        current_nose_point = (
            original_x,
            original_y,
        )

        print(
            f"[CLICK] 5番鼻先 "
            f"x={original_x}, "
            f"y={original_y}"
        )

        return


# ============================================================
# 通過判定
# ============================================================

def judge_crossing(
    pole_x: int,
    nose_x: int,
) -> tuple[
    str,
    int,
]:
    """
    600m通過判定。

    Returns
    -------
    status
    difference_px
    """

    difference = (
        nose_x
        - pole_x
    )

    # --------------------------------------------------------
    # 線上
    # --------------------------------------------------------

    if abs(
        difference
    ) <= LINE_TOLERANCE_PX:

        return (
            "AT_LINE",
            difference,
        )

    # --------------------------------------------------------
    # 右→左
    # --------------------------------------------------------

    if RUN_DIRECTION == "right_to_left":

        if nose_x > pole_x:

            return (
                "BEFORE",
                difference,
            )

        return (
            "PASSED",
            difference,
        )

    # --------------------------------------------------------
    # 左→右
    # --------------------------------------------------------

    if RUN_DIRECTION == "left_to_right":

        if nose_x < pole_x:

            return (
                "BEFORE",
                difference,
            )

        return (
            "PASSED",
            difference,
        )

    raise ValueError(
        f"未対応RUN_DIRECTION: "
        f"{RUN_DIRECTION}"
    )


# ============================================================
# 注釈描画
# ============================================================

def make_annotated_image(
    image: np.ndarray,
    frame_number: int,
    time_sec: float,
    pole_point: tuple[int, int],
    nose_point: tuple[int, int],
    status: str,
    difference_px: int,
) -> np.ndarray:
    """
    クリック結果を画像へ描画。
    """

    result = image.copy()

    height, width = (
        result.shape[:2]
    )

    pole_x, pole_y = (
        pole_point
    )

    nose_x, nose_y = (
        nose_point
    )

    # --------------------------------------------------------
    # 600m基準線
    # --------------------------------------------------------

    cv2.line(
        result,
        (
            pole_x,
            0,
        ),
        (
            pole_x,
            height - 1,
        ),
        (
            0,
            255,
            255,
        ),
        2,
    )

    # --------------------------------------------------------
    # 標識クリック点
    # --------------------------------------------------------

    cv2.circle(
        result,
        pole_point,
        6,
        (
            0,
            255,
            255,
        ),
        -1,
    )

    # --------------------------------------------------------
    # 馬鼻先
    # --------------------------------------------------------

    cv2.circle(
        result,
        nose_point,
        7,
        (
            255,
            0,
            255,
        ),
        -1,
    )

    # --------------------------------------------------------
    # 2点を線で接続
    # --------------------------------------------------------

    cv2.line(
        result,
        pole_point,
        nose_point,
        (
            255,
            255,
            255,
        ),
        1,
    )

    # --------------------------------------------------------
    # テキスト
    # --------------------------------------------------------

    texts = [
        f"FRAME: {frame_number}",
        f"TIME: {time_sec:.3f} sec",
        f"600m X: {pole_x}",
        f"HORSE5 NOSE X: {nose_x}",
        f"DIFF: {difference_px}px",
        f"STATUS: {status}",
    ]

    font = cv2.FONT_HERSHEY_SIMPLEX

    font_scale = 0.55

    thickness = 1

    x = 10

    y = 115

    for text in texts:

        cv2.putText(
            result,
            text,
            (
                x,
                y,
            ),
            font,
            font_scale,
            (
                255,
                255,
                255,
            ),
            thickness,
            cv2.LINE_AA,
        )

        y += 22

    # --------------------------------------------------------
    # 画面下にも状態表示
    # --------------------------------------------------------

    cv2.putText(
        result,
        status,
        (
            10,
            height - 15,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.9,
        (
            255,
            255,
            255,
        ),
        2,
        cv2.LINE_AA,
    )

    return result


# ============================================================
# UI用画像
# ============================================================

def make_display_image(
    image: np.ndarray,
    frame_number: int,
    time_sec: float,
) -> np.ndarray:
    """
    クリック中の情報を画面表示。
    """

    display = (
        image.copy()
    )

    # --------------------------------------------------------
    # 600m点
    # --------------------------------------------------------

    if current_pole_point is not None:

        pole_x, pole_y = (
            current_pole_point
        )

        cv2.line(
            display,
            (
                pole_x,
                0,
            ),
            (
                pole_x,
                display.shape[0] - 1,
            ),
            (
                0,
                255,
                255,
            ),
            2,
        )

        cv2.circle(
            display,
            current_pole_point,
            6,
            (
                0,
                255,
                255,
            ),
            -1,
        )

    # --------------------------------------------------------
    # 鼻先
    # --------------------------------------------------------

    if current_nose_point is not None:

        cv2.circle(
            display,
            current_nose_point,
            7,
            (
                255,
                0,
                255,
            ),
            -1,
        )

    # --------------------------------------------------------
    # 操作説明
    # --------------------------------------------------------

    instruction = ""

    if current_pole_point is None:

        instruction = (
            "CLICK 1: 600m pole center"
        )

    elif current_nose_point is None:

        instruction = (
            "CLICK 2: horse #5 nose"
        )

    else:

        instruction = (
            "ENTER=save / R=reset / S=skip / ESC=quit"
        )

    cv2.putText(
        display,
        instruction,
        (
            10,
            display.shape[0] - 15,
        ),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.55,
        (
            255,
            255,
            255,
        ),
        1,
        cv2.LINE_AA,
    )

    # --------------------------------------------------------
    # 拡大
    # --------------------------------------------------------

    display = cv2.resize(
        display,
        None,
        fx=DISPLAY_SCALE,
        fy=DISPLAY_SCALE,
        interpolation=cv2.INTER_LINEAR,
    )

    return display


# ============================================================
# 1フレーム処理
# ============================================================

def process_single_frame(
    image_path: Path,
) -> Optional[dict]:
    """
    1枚を手動マーク。
    """

    global current_pole_point
    global current_nose_point

    current_pole_point = None

    current_nose_point = None

    image = imread_unicode(
        image_path
    )

    if image is None:

        return {
            "success": False,
            "filename": image_path.name,
            "error": (
                "image load failed"
            ),
        }

    (
        frame_number,
        time_sec,
    ) = parse_frame_info(
        image_path
    )

    window_name = (
        f"600m Marker - "
        f"frame {frame_number}"
    )

    cv2.namedWindow(
        window_name,
        cv2.WINDOW_NORMAL,
    )

    cv2.setMouseCallback(
        window_name,
        mouse_callback,
    )

    print()
    print("=" * 70)

    print(
        f"FRAME={frame_number}"
    )

    print(
        f"TIME={time_sec:.3f}"
    )

    print(
        image_path.name
    )

    print("=" * 70)

    print(
        "① 600m標識をクリック"
    )

    print(
        "② 5番の鼻先をクリック"
    )

    print(
        "③ Enterで保存"
    )

    print(
        "R=やり直し"
    )

    print(
        "S=スキップ"
    )

    print(
        "ESC=終了"
    )

    # --------------------------------------------------------
    # UIループ
    # --------------------------------------------------------

    while True:

        display = make_display_image(
            image=image,
            frame_number=frame_number,
            time_sec=time_sec,
        )

        cv2.imshow(
            window_name,
            display,
        )

        key = cv2.waitKey(
            30
        ) & 0xFF

        # ----------------------------------------------------
        # ESC
        # ----------------------------------------------------

        if key == 27:

            cv2.destroyWindow(
                window_name
            )

            return None

        # ----------------------------------------------------
        # R
        # ----------------------------------------------------

        if key in (
            ord("r"),
            ord("R"),
        ):

            current_pole_point = None

            current_nose_point = None

            print(
                "[RESET]"
            )

            continue

        # ----------------------------------------------------
        # S
        # ----------------------------------------------------

        if key in (
            ord("s"),
            ord("S"),
        ):

            cv2.destroyWindow(
                window_name
            )

            return {
                "success": False,

                "frame_number": (
                    frame_number
                ),

                "time_sec": (
                    time_sec
                ),

                "filename": (
                    image_path.name
                ),

                "pole_x": None,

                "pole_y": None,

                "nose_x": None,

                "nose_y": None,

                "difference_px": None,

                "status": "SKIPPED",

                "annotated_file": "",

                "error": "",
            }

        # ----------------------------------------------------
        # Enter
        # ----------------------------------------------------

        if key in (
            13,
            10,
        ):

            if (
                current_pole_point is None
                or current_nose_point is None
            ):

                print(
                    "[WARN] 2点をクリックしてください。"
                )

                continue

            pole_x, pole_y = (
                current_pole_point
            )

            nose_x, nose_y = (
                current_nose_point
            )

            (
                status,
                difference_px,
            ) = judge_crossing(
                pole_x=pole_x,
                nose_x=nose_x,
            )

            annotated = (
                make_annotated_image(
                    image=image,
                    frame_number=frame_number,
                    time_sec=time_sec,
                    pole_point=(
                        pole_x,
                        pole_y,
                    ),
                    nose_point=(
                        nose_x,
                        nose_y,
                    ),
                    status=status,
                    difference_px=difference_px,
                )
            )

            output_name = (
                f"frame_{frame_number:06d}"
                f"_t_{time_sec:07.3f}"
                f"_marked.jpg"
            )

            output_path = (
                ANNOTATED_DIR
                / output_name
            )

            save_ok = (
                imwrite_unicode(
                    output_path=output_path,
                    image=annotated,
                    jpeg_quality=JPEG_QUALITY,
                )
            )

            cv2.destroyWindow(
                window_name
            )

            return {
                "success": (
                    save_ok
                ),

                "frame_number": (
                    frame_number
                ),

                "time_sec": (
                    time_sec
                ),

                "filename": (
                    image_path.name
                ),

                "pole_x": (
                    pole_x
                ),

                "pole_y": (
                    pole_y
                ),

                "nose_x": (
                    nose_x
                ),

                "nose_y": (
                    nose_y
                ),

                "difference_px": (
                    difference_px
                ),

                "status": (
                    status
                ),

                "annotated_file": str(
                    output_path
                ),

                "error": (
                    ""
                    if save_ok
                    else "annotated save failed"
                ),
            }


# ============================================================
# 結果分析
# ============================================================

def analyze_crossing(
    dataframe: pd.DataFrame,
) -> None:
    """
    BEFORE → PASSED の切り替わりを探す。
    """

    print()
    print("=" * 70)
    print("[CROSSING ANALYSIS]")
    print("=" * 70)

    usable = dataframe[
        dataframe["success"] == True
    ].copy()

    if usable.empty:

        print(
            "[WARN] 有効データなし"
        )

        return

    usable = usable.sort_values(
        "frame_number"
    )

    # --------------------------------------------------------
    # 全結果表示
    # --------------------------------------------------------

    for _, row in usable.iterrows():

        print(
            f"frame="
            f"{int(row['frame_number'])} "
            f"time="
            f"{float(row['time_sec']):.3f} "
            f"pole_x="
            f"{int(row['pole_x'])} "
            f"nose_x="
            f"{int(row['nose_x'])} "
            f"diff="
            f"{int(row['difference_px']):+d}px "
            f"{row['status']}"
        )

    # --------------------------------------------------------
    # 初回PASSED
    # --------------------------------------------------------

    passed = usable[
        usable[
            "status"
        ] == "PASSED"
    ]

    at_line = usable[
        usable[
            "status"
        ] == "AT_LINE"
    ]

    print()

    if not at_line.empty:

        first = at_line.iloc[0]

        print(
            "[候補] AT_LINEを検出"
        )

        print(
            f"frame="
            f"{int(first['frame_number'])}"
        )

        print(
            f"time="
            f"{float(first['time_sec']):.3f}秒"
        )

    if not passed.empty:

        first_passed = (
            passed.iloc[0]
        )

        first_passed_frame = int(
            first_passed[
                "frame_number"
            ]
        )

        print()
        print(
            "[候補] 最初のPASSED"
        )

        print(
            f"frame="
            f"{first_passed_frame}"
        )

        print(
            f"time="
            f"{float(first_passed['time_sec']):.3f}秒"
        )

        previous = usable[
            usable[
                "frame_number"
            ] < first_passed_frame
        ]

        if not previous.empty:

            previous_row = (
                previous.iloc[-1]
            )

            print()
            print(
                "直前フレーム:"
            )

            print(
                f"frame="
                f"{int(previous_row['frame_number'])}"
            )

            print(
                f"time="
                f"{float(previous_row['time_sec']):.3f}秒"
            )

            print(
                f"status="
                f"{previous_row['status']}"
            )

            print()
            print(
                "したがって600m通過時刻は"
            )

            print(
                f"{float(previous_row['time_sec']):.3f}"
                " ～ "
                f"{float(first_passed['time_sec']):.3f}"
                " 秒"
            )

    else:

        print(
            "[INFO] PASSED判定はありません。"
        )

    print("=" * 70)


# ============================================================
# 保存
# ============================================================

def save_results(
    dataframe: pd.DataFrame,
) -> None:
    """
    CSV / JSON保存。
    """

    dataframe.to_csv(
        RESULT_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    records = (
        dataframe
        .where(
            pd.notna(
                dataframe
            ),
            None,
        )
        .to_dict(
            orient="records"
        )
    )

    data = {
        "target": TARGET,

        "created_at": (
            now_string()
        ),

        "start_frame": (
            START_FRAME
        ),

        "end_frame": (
            END_FRAME
        ),

        "run_direction": (
            RUN_DIRECTION
        ),

        "line_tolerance_px": (
            LINE_TOLERANCE_PX
        ),

        "records": records,
    }

    RESULT_JSON.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print()
    print(
        f"[SAVE] {RESULT_CSV}"
    )

    print(
        f"[SAVE] {RESULT_JSON}"
    )


# ============================================================
# Main
# ============================================================

def main() -> int:

    print("=" * 70)
    print("JRA 600m Manual Marker PoC v0.1")
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
        f"対象frame="
        f"{START_FRAME}～{END_FRAME}"
    )

    print()
    print(
        f"INPUT:\n"
        f"{INPUT_DIR}"
    )

    print()
    print(
        f"OUTPUT:\n"
        f"{OUTPUT_DIR}"
    )

    try:

        # ====================================================
        # STEP 1
        # ====================================================

        print()
        print(
            "[STEP 1] 出力フォルダ準備"
        )

        prepare_directories()

        clear_old_files()

        # ====================================================
        # STEP 2
        # ====================================================

        print()
        print(
            "[STEP 2] フレーム検索"
        )

        frame_files = (
            build_frame_list()
        )

        print(
            f"[INFO] 対象画像数="
            f"{len(frame_files)}"
        )

        if not frame_files:

            raise RuntimeError(
                "対象フレームが見つかりません。"
            )

        # ====================================================
        # STEP 3
        # ====================================================

        print()
        print(
            "[STEP 3] 600m標識と5番鼻先を手動指定"
        )

        results: list[dict] = []

        for image_path in frame_files:

            result = (
                process_single_frame(
                    image_path
                )
            )

            # ESC
            if result is None:

                print()
                print(
                    "[STOP] ユーザー終了"
                )

                break

            results.append(
                result
            )

        cv2.destroyAllWindows()

        # ====================================================
        # STEP 4
        # ====================================================

        if not results:

            raise RuntimeError(
                "結果が1件もありません。"
            )

        dataframe = (
            pd.DataFrame(
                results
            )
        )

        # ====================================================
        # STEP 5
        # ====================================================

        print()
        print(
            "[STEP 4] CSV / JSON保存"
        )

        save_results(
            dataframe
        )

        # ====================================================
        # STEP 6
        # ====================================================

        print()
        print(
            "[STEP 5] 600m通過候補解析"
        )

        analyze_crossing(
            dataframe
        )

        # ====================================================
        # 完了
        # ====================================================

        print()
        print("=" * 70)
        print("[SUCCESS]")
        print("=" * 70)

        print()
        print(
            "600m標識・5番鼻先の"
            "手動マーキングが完了しました。"
        )

        print()
        print(
            "注釈画像:"
        )

        print(
            ANNOTATED_DIR
        )

        print()
        print(
            "判定CSV:"
        )

        print(
            RESULT_CSV
        )

        print()
        print(
            "次に見るポイント:"
        )

        print()
        print(
            "BEFORE"
        )

        print(
            "↓"
        )

        print(
            "AT_LINE または PASSED"
        )

        print()
        print(
            "へ初めて変化したフレームが"
            "600m通過フレーム候補です。"
        )

        print("=" * 70)

        return 0

    except KeyboardInterrupt:

        cv2.destroyAllWindows()

        print()
        print(
            "[STOP] ユーザー中断"
        )

        return 1

    except Exception as exc:

        cv2.destroyAllWindows()

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


if __name__ == "__main__":

    sys.exit(
        main()
    )