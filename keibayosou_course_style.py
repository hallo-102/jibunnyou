# -*- coding: utf-8 -*-
"""今回コース専用の脚質有利不利を計算するモジュール。"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import pandas as pd


RUNNING_STYLES = ("逃げ", "先行", "差し", "追込")
STYLE_TO_CODE = {"逃げ": 4.0, "先行": 3.0, "差し": 2.0, "追込": 1.0}


def _style_scores(nige: float, senko: float, sashi: float, oikomi: float) -> Dict[str, float]:
    """脚質別スコアを辞書へ変換する。"""
    return {
        "逃げ": float(nige),
        "先行": float(senko),
        "差し": float(sashi),
        "追込": float(oikomi),
    }


@dataclass(frozen=True)
class CourseStyleRule:
    """競馬場・芝ダ・距離・内外回りごとの脚質スコアルール。"""

    place: str
    surface: str
    dist_min: Optional[int]
    dist_max: Optional[int]
    variant: str
    good_scores: Dict[str, float]
    bad_scores: Dict[str, float]

    def matches(self, place: str, surface: str, distance: Optional[int], variant: str) -> bool:
        """今回条件がこのルールに該当するかを返す。"""
        if self.place != place or self.surface != surface:
            return False
        if self.variant and self.variant != variant:
            return False
        if distance is None:
            return False
        if self.dist_min is not None and distance < self.dist_min:
            return False
        if self.dist_max is not None and distance > self.dist_max:
            return False
        return True


def _rule(
    place: str,
    surface: str,
    dist_min: Optional[int],
    dist_max: Optional[int],
    good: Tuple[float, float, float, float],
    bad: Tuple[float, float, float, float],
    variant: str = "",
) -> CourseStyleRule:
    """ルール定義の記述量を減らすための補助関数。"""
    return CourseStyleRule(
        place=place,
        surface=surface,
        dist_min=dist_min,
        dist_max=dist_max,
        variant=variant,
        good_scores=_style_scores(*good),
        bad_scores=_style_scores(*bad),
    )


COURSE_STYLE_RULES: Tuple[CourseStyleRule, ...] = (
    # 東京
    _rule("東京", "芝", 1400, 1400, (1, 2, 2, 0), (2, 2, 1, -1)),
    _rule("東京", "芝", 1600, 1600, (0, 1, 3, 1), (1, 2, 2, 0)),
    _rule("東京", "芝", 1800, 1800, (0, 1, 3, 1), (1, 2, 2, 0)),
    _rule("東京", "芝", 2000, 2000, (1, 2, 2, 0), (2, 2, 1, -1)),
    _rule("東京", "芝", 2400, None, (0, 2, 2, 0), (1, 2, 1, -1)),
    _rule("東京", "ダ", 1400, 1400, (2, 2, 1, 0), (3, 3, 1, -1)),
    _rule("東京", "ダ", 1600, 1600, (2, 2, 1, 0), (3, 3, 1, 0)),
    _rule("東京", "ダ", 2100, 2100, (1, 2, 2, 0), (2, 2, 1, -1)),
    # 中山
    _rule("中山", "芝", 1200, 1200, (3, 3, 0, -2), (4, 3, -1, -3)),
    _rule("中山", "芝", 1600, 1600, (2, 3, 1, -2), (3, 3, 0, -3)),
    _rule("中山", "芝", 1800, 1800, (2, 3, 1, -2), (3, 3, 0, -3)),
    _rule("中山", "芝", 2000, 2000, (2, 3, 1, -2), (3, 4, 0, -3)),
    _rule("中山", "芝", 2200, 2200, (1, 3, 1, -2), (2, 4, 0, -3)),
    _rule("中山", "芝", 2500, None, (1, 3, 1, -2), (2, 4, 0, -3)),
    _rule("中山", "ダ", 1200, 1200, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("中山", "ダ", 1700, 1900, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 阪神
    _rule("阪神", "芝", 1200, 1200, (3, 3, 0, -2), (4, 3, -1, -3)),
    _rule("阪神", "芝", 1400, 1400, (2, 3, 1, -1), (3, 3, 0, -2)),
    _rule("阪神", "芝", 1600, 1600, (0, 1, 3, 1), (1, 2, 2, 0), variant="outer"),
    _rule("阪神", "芝", 1800, 1800, (0, 1, 3, 1), (1, 2, 2, 0), variant="outer"),
    _rule("阪神", "芝", 2000, 2000, (2, 3, 1, -2), (4, 4, 0, -3), variant="inner"),
    _rule("阪神", "芝", 2200, 2200, (2, 3, 1, -2), (4, 4, 0, -3), variant="inner"),
    _rule("阪神", "芝", 2400, 2400, (0, 1, 3, 1), (1, 2, 2, 0), variant="outer"),
    _rule("阪神", "ダ", 1200, 1400, (3, 3, 0, -2), (4, 4, -1, -3)),
    _rule("阪神", "ダ", 1700, 1900, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 京都
    _rule("京都", "芝", 1200, 1200, (3, 3, 0, -2), (4, 3, -1, -3)),
    _rule("京都", "芝", 1400, 1400, (2, 3, 1, -1), (3, 3, 0, -2), variant="inner"),
    _rule("京都", "芝", 1400, 1400, (1, 2, 2, 0), (2, 2, 1, -1), variant="outer"),
    _rule("京都", "芝", 1600, 1600, (2, 3, 1, -1), (3, 3, 0, -2), variant="inner"),
    _rule("京都", "芝", 1600, 1600, (1, 2, 2, 0), (2, 2, 1, -1), variant="outer"),
    _rule("京都", "芝", 1800, 1800, (1, 2, 2, 0), (2, 2, 1, -1), variant="outer"),
    _rule("京都", "芝", 2000, 2000, (2, 3, 1, -1), (3, 3, 0, -2), variant="inner"),
    _rule("京都", "芝", 2200, 2400, (1, 2, 2, 0), (2, 2, 1, -1), variant="outer"),
    _rule("京都", "芝", 3000, None, (0, 2, 2, 0), (1, 2, 1, -1)),
    _rule("京都", "ダ", 1200, 1400, (3, 3, 0, -2), (4, 4, -1, -3)),
    _rule("京都", "ダ", 1800, 1900, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 中京
    _rule("中京", "芝", 1200, 1200, (2, 3, 1, -1), (3, 3, 0, -2)),
    _rule("中京", "芝", 1400, 1400, (1, 2, 2, 0), (2, 3, 1, -1)),
    _rule("中京", "芝", 1600, 1600, (0, 2, 3, 1), (1, 2, 2, 0)),
    _rule("中京", "芝", 2000, 2000, (1, 2, 2, 0), (2, 3, 1, -1)),
    _rule("中京", "芝", 2200, None, (0, 2, 2, 0), (1, 3, 1, -1)),
    _rule("中京", "ダ", 1200, 1400, (3, 3, 0, -2), (4, 4, -1, -3)),
    _rule("中京", "ダ", 1800, 1900, (2, 3, 1, -1), (3, 3, 0, -2)),
    # 新潟
    _rule("新潟", "芝", 1000, 1000, (3, 3, 1, -2), (3, 3, 0, -2), variant="straight"),
    _rule("新潟", "芝", 1200, 1200, (3, 3, 0, -2), (4, 3, -1, -3), variant="inner"),
    _rule("新潟", "芝", 1400, 1400, (2, 3, 1, -1), (3, 3, 0, -2), variant="inner"),
    _rule("新潟", "芝", 1600, 2000, (-1, 1, 4, 2), (0, 2, 3, 1), variant="outer"),
    _rule("新潟", "芝", 2000, None, (1, 2, 2, 0), (2, 3, 1, -1), variant="inner"),
    _rule("新潟", "ダ", 1200, 1200, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("新潟", "ダ", 1800, 1800, (2, 3, 1, -1), (3, 3, 0, -2)),
    # 福島
    _rule("福島", "芝", 1200, 1200, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("福島", "芝", 1800, 2000, (2, 3, 1, -2), (3, 4, 0, -3)),
    _rule("福島", "芝", 2600, 2600, (1, 3, 1, -2), (2, 4, 0, -3)),
    _rule("福島", "ダ", 1150, 1150, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("福島", "ダ", 1700, 1700, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 小倉
    _rule("小倉", "芝", 1200, 1200, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("小倉", "芝", 1800, 2000, (2, 3, 1, -2), (3, 4, 0, -3)),
    _rule("小倉", "芝", 2600, 2600, (1, 3, 1, -2), (2, 4, 0, -3)),
    _rule("小倉", "ダ", 1000, 1000, (4, 4, -2, -3), (4, 4, -2, -3)),
    _rule("小倉", "ダ", 1700, 1700, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 札幌
    _rule("札幌", "芝", 1200, 1200, (3, 3, 0, -2), (4, 3, -1, -3)),
    _rule("札幌", "芝", 1500, 1500, (2, 3, 1, -1), (3, 3, 0, -2)),
    _rule("札幌", "芝", 1800, 2000, (2, 3, 1, -1), (3, 4, 0, -2)),
    _rule("札幌", "芝", 2600, 2600, (1, 3, 1, -1), (2, 4, 0, -2)),
    _rule("札幌", "ダ", 1000, 1000, (4, 4, -2, -3), (4, 4, -2, -3)),
    _rule("札幌", "ダ", 1700, 1700, (3, 3, 0, -2), (4, 3, -1, -3)),
    # 函館
    _rule("函館", "芝", 1200, 1200, (4, 3, -1, -3), (4, 4, -1, -3)),
    _rule("函館", "芝", 1800, 2000, (2, 3, 1, -2), (3, 4, 0, -3)),
    _rule("函館", "芝", 2600, 2600, (1, 3, 1, -2), (2, 4, 0, -3)),
    _rule("函館", "ダ", 1000, 1000, (4, 4, -2, -3), (4, 4, -2, -3)),
    _rule("函館", "ダ", 1700, 1700, (3, 3, 0, -2), (4, 3, -1, -3)),
)


def _is_missing(value: Any) -> bool:
    """None / NaN / 空文字を欠損として扱う。"""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip() == ""


def _normalize_text(value: Any) -> str:
    """表記ゆれを減らすためにNFKC正規化して前後空白を落とす。"""
    if _is_missing(value):
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def normalize_place(value: Any) -> str:
    """競馬場名を「東京」「中山」のような表記へ寄せる。"""
    text = _normalize_text(value)
    text = text.replace("競馬場", "")
    return re.sub(r"\s+", "", text)


def normalize_surface(value: Any) -> str:
    """芝/ダート/障害の表記を正規化する。"""
    text = _normalize_text(value)
    if "芝" in text:
        return "芝"
    if "ダ" in text:
        return "ダ"
    if "障" in text:
        return "障害"
    return text


def parse_distance(value: Any) -> Optional[int]:
    """「芝1600」「1600m」のような値から距離mを取り出す。"""
    if _is_missing(value):
        return None
    match = re.search(r"(\d{3,4})", _normalize_text(value))
    if not match:
        return None
    return int(match.group(1))


def normalize_track_condition(value: Any) -> str:
    """馬場状態を 良/稍重/重/不良 に寄せる。"""
    text = _normalize_text(value)
    if "不" in text:
        return "不良"
    if "稍" in text or "やや" in text:
        return "稍重"
    if "重" in text:
        return "重"
    if "良" in text:
        return "良"
    return "良"


def normalize_pace(value: Any) -> str:
    """想定ペースを slow/mid/fast/very_fast に寄せる。"""
    text = _normalize_text(value).lower()
    if text in {"", "nan", "<na>"}:
        return ""
    if "超" in text and ("ハイ" in text or "fast" in text):
        return "very_fast"
    if text in {"very_fast", "veryfast", "super_fast", "超ハイ", "超ハイペース"}:
        return "very_fast"
    if text in {"slow", "スロー", "スローペース"}:
        return "slow"
    if text in {"mid", "middle", "ミドル", "平均", "標準", "ミドルペース"}:
        return "mid"
    if text in {"fast", "high", "ハイ", "ハイペース"}:
        return "fast"
    return ""


def infer_course_variant(place: Any, surface: Any, distance: Any, course_text: Any = "") -> str:
    """内回り/外回り/直線を、明示表記とJRA10場の一般的な距離設定から推定する。"""
    p = normalize_place(place)
    s = normalize_surface(surface)
    d = parse_distance(distance)
    text = _normalize_text(course_text)

    if "直" in text:
        return "straight"
    if "外" in text:
        return "outer"
    if "内" in text:
        return "inner"
    if s != "芝" or d is None:
        return ""

    if p == "新潟":
        if d == 1000:
            return "straight"
        if d in {1600, 1800, 2000}:
            return "outer"
        return "inner"
    if p == "阪神":
        if d in {1600, 1800, 2400}:
            return "outer"
        return "inner"
    if p == "京都":
        if d == 2000:
            return "inner"
        if d in {1400, 1600, 1800, 2200, 2400} or d >= 3000:
            return "outer"
    return ""


def normalize_running_style(value: Any) -> str:
    """脚質表記を 逃げ/先行/差し/追込 に寄せる。"""
    text = _normalize_text(value).lower()
    if text in {"逃", "逃げ", "nige", "leader"}:
        return "逃げ"
    if text in {"先", "先行", "senko", "front", "pace"} or "先行" in text:
        return "先行"
    if text in {"差", "差し", "sashi", "mid", "middle"} or "差" in text:
        return "差し"
    if text in {"追", "追込", "追い込み", "oikomi", "rear", "closer"} or "追" in text:
        return "追込"
    return ""


def infer_running_style_from_pass(pass_value: Any, field_size: Optional[int]) -> str:
    """通過順から4分類の脚質を推定する。"""
    if _is_missing(pass_value):
        return ""
    nums = [int(x) for x in re.findall(r"\d+", _normalize_text(pass_value))]
    if not nums:
        return ""

    first_pos = nums[0]
    last_pos = nums[-1]
    if field_size and field_size > 0:
        field = max(int(field_size), 1)
        frac = float(last_pos) / float(field)
        escape_border = max(1, int(math.ceil(field * 0.16)))
        if last_pos <= escape_border or (first_pos == 1 and frac <= 0.35):
            return "逃げ"
        if frac <= 0.38:
            return "先行"
        if frac <= 0.72:
            return "差し"
        return "追込"

    if last_pos <= 1 or (first_pos == 1 and last_pos <= 3):
        return "逃げ"
    if last_pos <= 4:
        return "先行"
    if last_pos <= 9:
        return "差し"
    return "追込"


def dominant_running_style_from_pass_series(
    pass_s: pd.Series,
    field_size: Optional[int],
) -> Tuple[str, float, int]:
    """過去走の通過順から主脚質・信頼度・判定数を返す。"""
    if pass_s is None or pass_s.empty:
        return "", 0.0, 0

    styles = []
    for value in pass_s.dropna().tolist():
        style = infer_running_style_from_pass(value, field_size)
        if style:
            styles.append(style)

    if not styles:
        return "", 0.0, 0

    counts = pd.Series(styles).value_counts()
    dominant = str(counts.index[0])
    total = int(counts.sum())
    confidence = float(counts.iloc[0] / total) if total > 0 else 0.0
    return dominant, confidence, total


def running_style_to_code(value: Any) -> float:
    """脚質を数値コードに変換する。"""
    return STYLE_TO_CODE.get(normalize_running_style(value), 0.0)


def _is_bad_track(track_condition: str) -> bool:
    """良以外を道悪として扱う。"""
    return track_condition in {"稍重", "重", "不良"}


def _find_rule(place: str, surface: str, distance: Optional[int], variant: str) -> Optional[CourseStyleRule]:
    """今回条件に最初に合うルールを探す。"""
    for rule in COURSE_STYLE_RULES:
        if rule.matches(place=place, surface=surface, distance=distance, variant=variant):
            return rule
    return None


def _fallback_scores(place: str, surface: str, distance: Optional[int], variant: str) -> Dict[str, float]:
    """ルール表に無い距離を、競馬場傾向と距離帯から保守的に補完する。"""
    d = distance or 0
    local_small = place in {"函館", "札幌", "福島", "小倉", "中山"}
    long_straight = _is_long_straight_late_course(place, surface, variant)

    if surface == "ダ":
        if d and d <= 1200:
            return _style_scores(4, 3, -1, -3)
        if d and d <= 1400:
            return _style_scores(3, 3, 0, -2)
        if place in {"東京", "中京"}:
            return _style_scores(2, 3, 1, -1)
        return _style_scores(3, 3, 0, -2)

    if surface == "芝":
        if d and d <= 1200:
            return _style_scores(4, 3, -1, -3) if local_small else _style_scores(3, 3, 0, -2)
        if long_straight and d and 1400 <= d <= 2400:
            return _style_scores(0, 1, 3, 1)
        if local_small:
            if d and d >= 2400:
                return _style_scores(1, 3, 1, -2)
            return _style_scores(2, 3, 1, -2)
        return _style_scores(1, 2, 2, 0)

    return _style_scores(0, 0, 0, 0)


def _track_adjustment_scores(place: str, surface: str, track_condition: str, variant: str) -> Dict[str, float]:
    """馬場状態による追加補正を返す。"""
    if track_condition == "良":
        return _style_scores(0, 0, 0, 0)

    if surface == "芝":
        if place == "東京" or (place == "新潟" and variant == "outer"):
            table = {
                "稍重": _style_scores(0.5, 1.0, 0.0, -0.5),
                "重": _style_scores(0.5, 1.0, 0.0, -0.5),
                "不良": _style_scores(0.5, 1.0, 0.0, -0.5),
            }
            return table.get(track_condition, _style_scores(0, 0, 0, 0))
        table = {
            "稍重": _style_scores(0.5, 0.5, 0.0, -0.5),
            "重": _style_scores(1.0, 1.0, -0.5, -1.0),
            "不良": _style_scores(1.5, 1.5, -1.0, -1.5),
        }
        return table.get(track_condition, _style_scores(0, 0, 0, 0))

    if surface == "ダ":
        table = {
            "稍重": _style_scores(0.5, 0.5, 0.0, -0.5),
            "重": _style_scores(1.0, 1.0, -0.5, -1.0),
            "不良": _style_scores(1.5, 1.5, -0.5, -1.0),
        }
        return table.get(track_condition, _style_scores(0, 0, 0, 0))

    return _style_scores(0, 0, 0, 0)


def _is_short_front_resistant(surface: str, distance: Optional[int]) -> bool:
    """ハイペースでも前を下げすぎない短距離条件かを返す。"""
    if distance is None:
        return False
    return (surface == "芝" and distance == 1200) or (surface == "ダ" and distance <= 1400)


def _pace_adjustment_scores(surface: str, distance: Optional[int], pace: str) -> Dict[str, float]:
    """想定ペースによる追加補正を返す。"""
    if pace == "slow":
        return _style_scores(1.0, 1.0, -0.5, -1.0)
    if pace in {"", "mid"}:
        return _style_scores(0, 0, 0, 0)
    if pace == "fast":
        if _is_short_front_resistant(surface, distance):
            return _style_scores(-0.5, 0.0, 1.0, 0.0)
        return _style_scores(-1.0, -0.5, 1.0, 0.5)
    if pace == "very_fast":
        if _is_short_front_resistant(surface, distance):
            return _style_scores(-1.0, -0.5, 1.5, 0.5)
        return _style_scores(-2.0, -1.0, 1.5, 1.0)
    return _style_scores(0, 0, 0, 0)


def _is_local_small_front_course(place: str, surface: str, variant: str) -> bool:
    """小回り・内回りで前目を評価したい条件かを返す。"""
    if surface not in {"芝", "ダ"}:
        return False
    if place in {"函館", "札幌", "福島", "小倉", "中山"}:
        return True
    return place == "阪神" and variant == "inner"


def _local_small_front_bonus(place: str, surface: str, variant: str) -> Dict[str, float]:
    """ローカル小回り・内回りの逃げ先行ボーナスを返す。"""
    if _is_local_small_front_course(place, surface, variant):
        return _style_scores(0.8, 0.6, 0.0, 0.0)
    return _style_scores(0, 0, 0, 0)


def _is_long_straight_late_course(place: str, surface: str, variant: str) -> bool:
    """長い直線で差し脚を評価したい条件かを返す。"""
    if surface not in {"芝", "ダ"}:
        return False
    if place in {"東京", "中京"}:
        return True
    if place in {"新潟", "阪神"} and variant == "outer":
        return True
    return False


def _long_straight_late_bonus(place: str, surface: str, variant: str) -> Dict[str, float]:
    """長い直線コースの差し追込ボーナスを返す。"""
    if _is_long_straight_late_course(place, surface, variant):
        return _style_scores(0.0, 0.0, 0.8, 0.4)
    return _style_scores(0, 0, 0, 0)


def _clip_course_style_score(value: float) -> float:
    """メモの目安に合わせ、-3〜+4へ丸める。"""
    return float(max(-3.0, min(4.0, value)))


def calc_course_style_features(
    place: Any,
    surface: Any,
    distance: Any,
    track_condition: Any,
    running_style: Any,
    course_text: Any = "",
    pace: Any = "",
) -> Dict[str, float]:
    """
    開催場所×芝/ダート×距離×馬場状態×脚質で、今回脚質の有利不利を数値化する。

    course_style_fit は初版ルール表を軸に、道悪と想定ペースだけを足した総合値としてscoreへ使う。
    小回り前有利・長直線差し有利は、表の背景傾向を確認するための診断列として出力する。
    """
    p = normalize_place(place)
    s = normalize_surface(surface or course_text)
    d = parse_distance(distance if distance is not None else course_text)
    baba = normalize_track_condition(track_condition)
    style = normalize_running_style(running_style)
    pace_norm = normalize_pace(pace)
    variant = infer_course_variant(p, s, d, course_text)

    if not style or not p or s not in {"芝", "ダ"} or d is None:
        return {
            "course_style_fit": 0.0,
            "bad_track_style_fit": 0.0,
            "pace_adjusted_course_style_fit": 0.0,
            "local_small_course_front_bonus": 0.0,
            "long_straight_late_bonus": 0.0,
            "running_style_code": running_style_to_code(style),
        }

    rule = _find_rule(p, s, d, variant)
    if rule is not None:
        base_scores = rule.bad_scores if _is_bad_track(baba) else rule.good_scores
    else:
        base_scores = _fallback_scores(p, s, d, variant)

    base_fit = float(base_scores.get(style, 0.0))
    bad_track_fit = float(_track_adjustment_scores(p, s, baba, variant).get(style, 0.0))
    pace_fit = float(_pace_adjustment_scores(s, d, pace_norm).get(style, 0.0))
    local_front_fit = float(_local_small_front_bonus(p, s, variant).get(style, 0.0))
    long_straight_fit = float(_long_straight_late_bonus(p, s, variant).get(style, 0.0))

    total_fit = _clip_course_style_score(base_fit + bad_track_fit + pace_fit)

    return {
        "course_style_fit": round(total_fit, 4),
        "bad_track_style_fit": round(bad_track_fit, 4),
        "pace_adjusted_course_style_fit": round(pace_fit, 4),
        "local_small_course_front_bonus": round(local_front_fit, 4),
        "long_straight_late_bonus": round(long_straight_fit, 4),
        "running_style_code": running_style_to_code(style),
    }
