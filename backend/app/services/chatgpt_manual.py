from __future__ import annotations

import json
import logging
import webbrowser
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.models import (
    ChatgptManualPrediction,
    HorsePastPerformance,
    PredictionResult,
    PredictionRun,
    Race,
    RaceEntry,
)


LOGGER = logging.getLogger(__name__)
SOURCE = "chatgpt_manual"


class ChatgptManualError(RuntimeError):
    """Represent a safe Japanese message for a manual ChatGPT operation."""


def generate_chatgpt_prompt(
    db: Session,
    race_id: str,
    *,
    settings: Settings | None = None,
) -> ChatgptManualPrediction:
    """Generate and persist an editable prompt without calling an external API."""

    resolved = settings or get_settings()
    if not resolved.chatgpt_manual_prediction_enabled:
        raise ChatgptManualError("ChatGPT手動予想機能は現在無効です")

    race = db.get(Race, race_id)
    if race is None:
        raise ChatgptManualError("対象レースの情報が見つかりません")
    entries = list(
        db.scalars(
            select(RaceEntry)
            .where(RaceEntry.race_id == race_id)
            .order_by(RaceEntry.horse_no)
        )
    )
    if not entries:
        raise ChatgptManualError("対象レースの出走馬情報が見つかりません")

    prediction_run = db.scalar(
        select(PredictionRun)
        .where(
            PredictionRun.race_id == race_id,
            PredictionRun.status.in_(("completed", "succeeded")),
        )
        .order_by(PredictionRun.created_at.desc())
        .limit(1)
    )
    if prediction_run is None:
        raise ChatgptManualError("Python予想が未実行です。先にPython予想を実行してください")
    predictions = {
        item.horse_no: item
        for item in db.scalars(
            select(PredictionResult)
            .where(
                PredictionResult.race_id == race_id,
                PredictionResult.prediction_run_id == prediction_run.id,
            )
            .order_by(PredictionResult.horse_no)
        )
    }
    if not predictions:
        raise ChatgptManualError("Python予想結果が見つかりません。Python予想を再実行してください")

    prompt_text = _build_prompt(
        db,
        race=race,
        entries=entries,
        predictions=predictions,
        recent_races_per_horse=resolved.chatgpt_recent_races_per_horse,
    )
    record = ChatgptManualPrediction(
        race_id=race_id,
        source=SOURCE,
        prompt_text=prompt_text,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def save_chatgpt_response(
    db: Session,
    *,
    race_id: str,
    prompt_text: str,
    response_text: str,
    history_id: str | None = None,
) -> ChatgptManualPrediction:
    """Save a manually pasted ChatGPT response and its actual edited prompt."""

    cleaned_prompt = prompt_text.strip()
    cleaned_response = response_text.strip()
    if not cleaned_prompt:
        raise ChatgptManualError("保存するプロンプトが空です")
    if not cleaned_response:
        raise ChatgptManualError("ChatGPTの回答が空欄です")
    if db.get(Race, race_id) is None:
        raise ChatgptManualError("対象レースの情報が見つかりません")

    record = db.get(ChatgptManualPrediction, history_id) if history_id else None
    if record is not None and record.race_id != race_id:
        raise ChatgptManualError("選択レースと保存履歴のレースが一致しません")
    if record is None:
        record = ChatgptManualPrediction(race_id=race_id, source=SOURCE, prompt_text=cleaned_prompt)
        db.add(record)
    record.prompt_text = cleaned_prompt
    record.response_text = cleaned_response
    db.commit()
    db.refresh(record)
    return record


def list_chatgpt_history(
    db: Session,
    race_id: str,
    *,
    limit: int = 20,
) -> list[ChatgptManualPrediction]:
    """Return newest manual ChatGPT records for one race."""

    return list(
        db.scalars(
            select(ChatgptManualPrediction)
            .where(ChatgptManualPrediction.race_id == race_id)
            .order_by(ChatgptManualPrediction.created_at.desc())
            .limit(limit)
        )
    )


def copy_prompt_to_clipboard(prompt_text: str) -> None:
    """Copy text through the Python standard UI toolkit when used outside the web UI."""

    if not prompt_text.strip():
        raise ChatgptManualError("コピーするプロンプトが空です")
    try:
        import tkinter

        root = tkinter.Tk()
        root.withdraw()
        root.clipboard_clear()
        root.clipboard_append(prompt_text)
        root.update()
        root.destroy()
    except Exception as exc:  # pragma: no cover - GUI環境依存のため単体テストではモックする。
        LOGGER.exception("ChatGPT prompt clipboard copy failed: %s", exc.__class__.__name__)
        raise ChatgptManualError(
            "クリップボードへコピーできませんでした。確認欄から手動でコピーしてください"
        ) from exc


def open_chatgpt_browser(
    *,
    url: str | None = None,
    settings: Settings | None = None,
) -> None:
    """Open ChatGPT in the OS default browser without automating the page."""

    target = url or (settings or get_settings()).chatgpt_url
    try:
        if not webbrowser.open(target):
            raise RuntimeError("webbrowser.open returned false")
    except Exception as exc:
        LOGGER.exception("ChatGPT browser open failed: %s", exc.__class__.__name__)
        raise ChatgptManualError(
            f"ChatGPTをブラウザで開けませんでした。次のURLを手動で開いてください。\n\n{target}"
        ) from exc


def _build_prompt(
    db: Session,
    *,
    race: Race,
    entries: list[RaceEntry],
    predictions: dict[int, PredictionResult],
    recent_races_per_horse: int,
) -> str:
    race_raw = race.raw if isinstance(race.raw, dict) else {}
    generated_at = datetime.now(ZoneInfo("Asia/Tokyo")).isoformat(timespec="seconds")
    race_items = [
        ("レースID", race.race_id),
        ("開催日", race.race_date),
        ("競馬場", race.venue),
        ("レース番号", f"{race.race_number}R" if race.race_number else None),
        ("レース名", race.name),
        ("コース", race.course),
        ("距離", _pick(race_raw, "距離", "distance")),
        ("芝・ダート", _pick(race_raw, "芝ダ", "surface", "コース種別")),
        ("馬場状態", race.track_condition),
        ("天候", _pick(race_raw, "天候", "weather")),
        ("頭数", race.headcount or len(entries)),
        ("クラス", race.race_class),
        ("レース種別", race.race_type),
        ("ペース予想", _pick(race_raw, "ペース予想", "pace_prediction")),
        ("レース質", _pick(race_raw, "レース質", "race_quality")),
        ("展開予想", _pick(race_raw, "展開予想", "race_development")),
    ]
    lines = [
        "# ChatGPTへの競馬予想・最新情報調査依頼",
        "",
        "## 1. あなたの役割（最重要）",
        "あなたの最重要任務は、Python予想の説明や言い換えではありません。",
        "Web検索を使ってPythonが取得できない直前情報・定性情報を調査し、Python予想を"
        "見る前に独立評価を作成したうえで、Python予想と比較し、必要なら明確に反対・"
        "降格・昇格・順位変更を行うことです。",
        "Pythonは構造化された数値データの処理を担当し、あなたは最新情報の調査、文脈判断、"
        "情報の矛盾確認、独立見解、Python予想への批判、最終統合判断を担当してください。",
        "Pythonの順位を正解として扱わず、根拠があれば同意し、問題があれば遠慮なく否定してください。",
        "",
        "## 2. 対象レース",
    ]
    lines.extend(f"- {label}: {_format(value)}" for label, value in race_items if _has_value(value))

    lines.extend(
        [
            "",
            "## 3. プロンプト生成日時",
            f"- 日本時間: {generated_at}",
            "- 対象レースの開催日とレースIDをWeb検索結果と照合し、別年・別レース・同名馬を混同しないでください。",
            "",
            "## 4. Web調査指示",
            "最初にWeb検索を実行し、全出走馬について予想を変え得る最新情報を優先して調査してください。",
            "特に、最終追い切り・1週前追い切り、厩舎・騎手コメント、馬体重と気配、輸送、"
            "当日の天候・馬場変化・トラックバイアス、血統、ローテーションと出走意図、"
            "厩舎の勝負度、騎手変更、気性、前走の不利・展開利、前走やメンバーのレースレベル、"
            "枠順、同型馬とペース衝突、人気・オッズの過熱や盲点を確認してください。",
            "情報量を増やすこと自体を目的にせず、結論や順位が変わり得る情報を優先してください。",
            "Web検索を利用できない場合は、その事実を冒頭で明記し、未確認情報を推測で補わないでください。",
            "",
            "## 5. 情報の信頼性と調査ルール",
            "- 情報源の優先順位: JRA等の主催者・競馬場公式 > 厩舎・騎手・馬主・生産者公式 > "
            "信頼できる競馬専門メディア > 競馬データサイト > 一般ニュース > SNS・掲示板。",
            "- SNS・掲示板は補助情報に限り、重要な判断は可能な限り複数ソースで照合してください。",
            "- 枠順確定前後、最終追い切り前後、記事の公開日と取材日を区別し、最新情報を優先してください。",
            "- 事実・評価・推測を明確に分け、確認できない事項は必ず「確認できない」と書いてください。",
            "- 使用した情報にはページ名、公開日または確認日、URLを付けてください。",
            "",
            "## 6. Pythonによる先入観を抑える必須分析手順",
            "以下の4段階を厳守してください。",
            "1. 最新情報調査: Web検索で対象レースと全出走馬の直前情報を収集する。",
            "2. AI独立評価: 下記のPython予想結果を参照せず、Web調査とレース条件から全馬の"
            "独立順位・100点満点スコア・評価理由を確定する。",
            "3. Python比較: 独立評価を確定した後に初めてPython予想を読み、一致点・相違点と"
            "Pythonの過大評価・過小評価を検証する。",
            "4. 統合最終予想: Python数値とWeb調査結果を統合し、最終順位・印・買い目・見送り判断を出す。",
            "",
            "## 7. Python予想結果（第3段階まで参照禁止）",
            "注意: 独立順位と独立スコアを書き終えるまで、この節の順位・スコアを根拠にしないでください。",
        ]
    )
    for entry in entries:
        prediction = predictions.get(entry.horse_no)
        python_items = [
            ("馬番・馬名", f"{entry.horse_no}番 {entry.horse_name}"),
            ("Python予想順位", prediction.prediction_rank if prediction else None),
            ("Python予想スコア", prediction.prediction_score if prediction else None),
            ("推定馬券内率", prediction.estimated_in3_rate if prediction else None),
            ("期待値", prediction.expected_value if prediction else None),
            ("危険馬判定", prediction.risk_reason if prediction and prediction.risk_flag else None),
            ("Python評価理由", _limited(prediction.evaluation_reason, 400) if prediction else None),
        ]
        lines.append(
            "- " + " / ".join(f"{label}: {_format(value)}" for label, value in python_items if _has_value(value))
        )

    lines.extend(["", "## 8. 全出走馬の基礎・数値データ"])
    for entry in entries:
        prediction = predictions.get(entry.horse_no)
        raw = entry.raw if isinstance(entry.raw, dict) else {}
        lines.extend(["", f"### {entry.horse_no}番 {entry.horse_name}"])
        horse_items = [
            ("枠番", entry.frame_no),
            ("人気", prediction.popularity if prediction and prediction.popularity is not None else entry.popularity),
            ("単勝オッズ", prediction.win_odds if prediction and prediction.win_odds is not None else entry.win_odds),
            ("複勝オッズ", prediction.place_odds if prediction and prediction.place_odds is not None else entry.place_odds),
            ("性齢", _pick(raw, "性齢", "sex_age") or (f"{entry.age}歳" if entry.age else None)),
            ("斤量", entry.carried_weight),
            ("騎手", entry.jockey),
            ("調教師", entry.trainer),
            ("脚質", _pick(raw, "脚質", "running_style")),
            ("調教評価", _limited(_pick(raw, "調教評価", "training_summary", "調教"), 400)),
            ("関係者コメント", _limited(_pick(raw, "関係者コメント", "trainer_comment", "コメント"), 400)),
            ("血統情報", _limited(_pick(raw, "血統情報", "bloodline_summary", "血統"), 400)),
        ]
        lines.extend(f"- {label}: {_format(value)}" for label, value in horse_items if _has_value(value))

        past_rows = list(
            db.scalars(
                select(HorsePastPerformance)
                .where(
                    HorsePastPerformance.target_race_id == race.race_id,
                    HorsePastPerformance.horse_name == entry.horse_name,
                )
                .order_by(HorsePastPerformance.race_date.desc())
                .limit(recent_races_per_horse)
            )
        )
        if past_rows:
            lines.append(f"- 過去走（直近{len(past_rows)}走）:")
            for row in past_rows:
                values = [
                    _format(row.race_date),
                    row.race_name,
                    f"{row.finish_position}着" if row.finish_position else None,
                    f"{row.popularity}人気" if row.popularity else None,
                    f"単勝{_format(row.odds)}" if row.odds is not None else None,
                    row.distance,
                    row.jockey,
                ]
                lines.append("  - " + " / ".join(str(value) for value in values if _has_value(value)))

    lines.extend(
        [
            "",
            "## 9. Keiba AI Studioのペース・レース質・展開予想",
            f"- ペース予想: {_format(_pick(race_raw, 'ペース予想', 'pace_prediction') or 'データなし')}",
            f"- レース質: {_format(_pick(race_raw, 'レース質', 'race_quality') or 'データなし')}",
            f"- 展開予想: {_format(_pick(race_raw, '展開予想', 'race_development') or 'データなし')}",
            "",
            "## 10. Pythonが使用した主な特徴量",
        ]
    )
    for entry in entries:
        prediction = predictions.get(entry.horse_no)
        features = (
            _useful_features(prediction.feature_summary)
            if prediction and isinstance(prediction.feature_summary, dict)
            else []
        )
        feature_text = " / ".join(f"{key}={_format(value)}" for key, value in features) or "データなし"
        lines.append(f"- {entry.horse_no}番 {entry.horse_name}: {feature_text}")

    lines.extend(
        [
            "",
            "## 11. Python予想との比較指示",
            "- Python上位1〜3頭と上位5頭は、Web調査で判明した不安材料から降格の要否を必ず検討してください。",
            "- Python6位以下は、最新状態・展開・馬場・人気との比較から昇格候補を漏れなく検討してください。",
            "- Pythonで最も過大評価された馬と最も過小評価された馬を各1頭挙げ、理由を示してください。",
            "- Python1位馬は、状態、追い切り、展開、馬場、距離、枠、騎手、血統、相手関係、"
            "人気との釣り合いを深掘りし、信頼度をS/A/B/C/Dで判定してください。",
            "- Python6位以下から穴馬を探索してください。ただし根拠が弱ければ「該当なし」とし、無理に選ばないでください。",
            "- 独立順位とPython順位の差を全馬について示し、最終的な修正順位を提示してください。",
            "",
            "## 12. 最終判断・買い目",
            "的中可能性だけでなく、想定オッズに対する期待値、資金配分、組み合わせの重複、"
            "不確実性を考慮してください。期待値が低い、情報不足、直前変動が大きい場合は、"
            "無理に買い目を作らず明確に「見送り」としてください。",
            "",
            "## 13. 必須回答形式",
            "以下の14見出しをこの順番で、省略せず日本語で回答してください。",
            "1. 対象レース確認",
            "2. Web調査で確認した最新情報（全馬。事実／評価／推測、日付、URLを表で整理）",
            "3. 当日の天候・馬場状態・トラックバイアス",
            "4. Python予想を参照する前のAI独立評価（全馬の独立順位と100点満点スコア）",
            "5. 全出走馬評価（状態、追い切り、展開、馬場、コース、距離、枠、騎手、血統、"
            "強み、弱みを全馬分記載）",
            "6. AI独立評価とPython予想の比較（全馬の順位差を記載）",
            "7. Python予想への明確な意見（同意、反対、降格、昇格、最も過大評価、最も過小評価）",
            "8. Python1位馬の深掘り監査（信頼度S/A/B/C/D）",
            "9. Python6位以下の穴馬監査（該当なし可）",
            "10. 最終印（◎○▲△☆および危険な人気馬）",
            "11. 統合最終ランキング（全馬、100点満点スコア、Python順位からの変更理由）",
            "12. 期待値を考慮した推奨買い目・資金配分、または見送り",
            "13. 最終結論と予想信頼度",
            "14. 参照情報源一覧（ページ名、公開日または確認日、URL）",
            "",
            "最後にもう一度確認します。Python予想の説明を目的にせず、必ずWeb検索を先に行い、"
            "独立評価を確定してからPythonと比較してください。確認できない情報は「確認できない」と明記してください。",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _pick(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if _has_value(value):
            return value
    return None


def _useful_features(payload: dict[str, Any]) -> list[tuple[str, Any]]:
    seen: set[str] = set()
    selected: list[tuple[str, Any]] = []
    for key, value in payload.items():
        normalized = str(key).strip()
        if (
            not normalized
            or normalized.lower().endswith("id")
            or normalized in seen
            or isinstance(value, (dict, list))
            or not _has_value(value)
        ):
            continue
        seen.add(normalized)
        selected.append((normalized, value))
        if len(selected) >= 12:
            break
    return selected


def _limited(value: Any, max_length: int) -> str | None:
    if not _has_value(value):
        return None
    text = str(value).strip()
    return text if len(text) <= max_length else text[:max_length] + "…"


def _has_value(value: Any) -> bool:
    return value is not None and value != "" and value != [] and value != {}


def _format(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)
