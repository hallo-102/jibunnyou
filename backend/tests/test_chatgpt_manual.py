from datetime import date
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.api.v1.deps import get_db
from app.core.config import Settings
from app.db.base import Base
from app.db.models import (
    ChatgptManualPrediction,
    HorsePastPerformance,
    PredictionResult,
    PredictionRun,
    Race,
    RaceEntry,
)
from app.main import app
from app.services.chatgpt_manual import (
    ChatgptManualError,
    copy_prompt_to_clipboard,
    generate_chatgpt_prompt,
    list_chatgpt_history,
    open_chatgpt_browser,
    save_chatgpt_response,
)


def _session(tmp_path) -> Session:
    """Create an isolated database for manual ChatGPT tests."""

    engine = create_engine(f"sqlite:///{tmp_path / 'chatgpt.db'}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed_race(db: Session) -> None:
    """Seed one race with Python predictions and more past races than the prompt limit."""

    db.add(
        Race(
            race_id="202607230101",
            race_date=date(2026, 7, 23),
            race_number=1,
            venue="東京",
            name="テスト特別",
            course="芝1600m",
            track_condition="良",
            race_class="3勝クラス",
            headcount=2,
            raw={"天候": "晴", "距離": "1600m", "芝ダ": "芝", "ペース予想": "ミドル"},
        )
    )
    db.add_all(
        [
            RaceEntry(
                race_id="202607230101",
                horse_no=1,
                frame_no=1,
                horse_name="テストホースA",
                age=4,
                carried_weight=57,
                jockey="騎手A",
                trainer="調教師A",
                popularity=1,
                win_odds=2.5,
                raw={
                    "性齢": "牡4",
                    "脚質": "差し",
                    "調教評価": "A",
                    "関係者コメント": "順調です。" * 100,
                    "血統情報": "父テスト",
                },
            ),
            RaceEntry(
                race_id="202607230101",
                horse_no=2,
                frame_no=2,
                horse_name="テストホースB",
                age=5,
                carried_weight=56,
                jockey="騎手B",
                trainer=None,
                popularity=None,
                win_odds=None,
                raw={},
            ),
        ]
    )
    db.add(
        PredictionRun(
            id="python-run-001",
            status="completed",
            race_date=date(2026, 7, 23),
            race_id="202607230101",
            prediction_version="test",
            feature_version="test",
            weight_version="test",
            model_version="python-test",
        )
    )
    db.add_all(
        [
            PredictionResult(
                prediction_run_id="python-run-001",
                race_id="202607230101",
                horse_no=1,
                horse_name="テストホースA",
                prediction_rank=1,
                prediction_score=80.5,
                risk_flag=False,
                feature_summary={"speed": 1.2, "horse_id": 999, "missing": None},
            ),
            PredictionResult(
                prediction_run_id="python-run-001",
                race_id="202607230101",
                horse_no=2,
                horse_name="テストホースB",
                prediction_rank=2,
                prediction_score=70.1,
                risk_flag=True,
                risk_reason="人気先行",
            ),
        ]
    )
    for index in range(7):
        db.add(
            HorsePastPerformance(
                source_file="test.xlsx",
                source_sheet="過去走",
                target_race_id="202607230101",
                horse_name="テストホースA",
                race_date=date(2026, 7, 20 - index),
                race_name=f"過去レース{index}",
                finish_position=index + 1,
            )
        )
    db.commit()


def _settings(tmp_path, **overrides) -> Settings:
    """Build explicit manual ChatGPT settings without API credentials."""

    return Settings(
        database_url=f"sqlite:///{tmp_path / 'unused.db'}",
        chatgpt_manual_prediction_enabled=True,
        chatgpt_recent_races_per_horse=5,
        chatgpt_prompt_length_warning=1000,
        **overrides,
    )


def test_prompt_generation_handles_missing_values_limits_history_and_avoids_internal_ids(tmp_path):
    db = _session(tmp_path)
    try:
        _seed_race(db)
        record = generate_chatgpt_prompt(db, "202607230101", settings=_settings(tmp_path))

        assert record.source == "chatgpt_manual"
        assert "東京" in record.prompt_text
        assert "テストホースA" in record.prompt_text
        assert "Python予想順位: 1" in record.prompt_text
        assert "speed=1.2" in record.prompt_text
        assert "horse_id" not in record.prompt_text
        assert record.prompt_text.count("過去レース") == 5
        assert "調教師: データなし" not in record.prompt_text
        assert "Python予想の説明や言い換えではありません" in record.prompt_text
        assert "見送り" in record.prompt_text
        assert len(record.prompt_text) < 50000
    finally:
        db.close()


def test_prompt_requires_web_first_independent_evaluation_and_complete_output_order(tmp_path):
    db = _session(tmp_path)
    try:
        _seed_race(db)
        prompt = generate_chatgpt_prompt(
            db,
            "202607230101",
            settings=_settings(tmp_path),
        ).prompt_text

        required_instructions = [
            "最初にWeb検索を実行",
            "独立順位・100点満点スコア",
            "第3段階まで参照禁止",
            "Python上位1〜3頭",
            "Python6位以下",
            "最も過大評価された馬",
            "最も過小評価された馬",
            "信頼度をS/A/B/C/D",
            "確認できない",
            "公開日または確認日、URL",
            "期待値",
            "見送り",
        ]
        for instruction in required_instructions:
            assert instruction in prompt

        assert prompt.index("## 1. あなたの役割") < prompt.index("## 2. 対象レース")
        assert prompt.index("## 2. 対象レース") < prompt.index("## 3. プロンプト生成日時")
        assert prompt.index("## 4. Web調査指示") < prompt.index("## 6. Pythonによる先入観")
        assert prompt.index("2. AI独立評価") < prompt.index("## 7. Python予想結果")
        assert prompt.index("## 7. Python予想結果") < prompt.index("## 8. 全出走馬")
        assert prompt.index("## 12. 最終判断") < prompt.index("## 13. 必須回答形式")
        assert "- レースID: 202607230101" in prompt
        assert "- 開催日: 2026-07-23" in prompt
        for section_number in range(1, 15):
            assert f"{section_number}. " in prompt
    finally:
        db.close()


def test_prompt_generation_requires_race_entries_and_python_prediction(tmp_path):
    db = _session(tmp_path)
    try:
        with pytest.raises(ChatgptManualError, match="対象レース"):
            generate_chatgpt_prompt(db, "missing", settings=_settings(tmp_path))

        db.add(Race(race_id="race-no-entry", race_date=date(2026, 7, 23)))
        db.commit()
        with pytest.raises(ChatgptManualError, match="出走馬情報"):
            generate_chatgpt_prompt(db, "race-no-entry", settings=_settings(tmp_path))

        db.add(Race(race_id="race-no-python", race_date=date(2026, 7, 23)))
        db.add(RaceEntry(race_id="race-no-python", horse_no=1, horse_name="未予想馬"))
        db.commit()
        with pytest.raises(ChatgptManualError, match="Python予想が未実行"):
            generate_chatgpt_prompt(db, "race-no-python", settings=_settings(tmp_path))
    finally:
        db.close()


def test_manual_response_save_rejects_empty_and_history_is_readable(tmp_path):
    db = _session(tmp_path)
    try:
        _seed_race(db)
        prompt = generate_chatgpt_prompt(db, "202607230101", settings=_settings(tmp_path))
        with pytest.raises(ChatgptManualError, match="回答が空欄"):
            save_chatgpt_response(
                db,
                race_id="202607230101",
                history_id=prompt.id,
                prompt_text=prompt.prompt_text,
                response_text="   ",
            )

        saved = save_chatgpt_response(
            db,
            race_id="202607230101",
            history_id=prompt.id,
            prompt_text=prompt.prompt_text + "\n利用者編集",
            response_text="本命は1番です。",
        )
        history = list_chatgpt_history(db, "202607230101")

        assert saved.response_text == "本命は1番です。"
        assert saved.prompt_text.endswith("利用者編集")
        assert history[0].id == saved.id
        assert db.scalar(select(ChatgptManualPrediction).where(ChatgptManualPrediction.id == saved.id))
    finally:
        db.close()


def test_clipboard_and_browser_helpers_use_expected_text_and_url(tmp_path, monkeypatch):
    clipboard: list[str] = []

    class FakeRoot:
        def withdraw(self):
            return None

        def clipboard_clear(self):
            clipboard.clear()

        def clipboard_append(self, text):
            clipboard.append(text)

        def update(self):
            return None

        def destroy(self):
            return None

    monkeypatch.setitem(__import__("sys").modules, "tkinter", SimpleNamespace(Tk=FakeRoot))
    opened: list[str] = []
    monkeypatch.setattr(
        "app.services.chatgpt_manual.webbrowser.open",
        lambda url: opened.append(url) or True,
    )

    copy_prompt_to_clipboard("日本語プロンプト")
    open_chatgpt_browser(settings=_settings(tmp_path))

    assert clipboard == ["日本語プロンプト"]
    assert opened == ["https://chatgpt.com/"]


def test_prompt_response_api_round_trip(tmp_path):
    db = _session(tmp_path)
    _seed_race(db)

    def override_db():
        try:
            yield db
        finally:
            pass

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        generated = client.post(
            "/api/v1/chatgpt/prompts",
            json={"race_id": "202607230101"},
        )
        assert generated.status_code == 201
        prompt_payload = generated.json()
        assert prompt_payload["chatgpt_url"] == "https://chatgpt.com/"
        assert prompt_payload["prompt_length"] == len(prompt_payload["prompt_text"])

        saved = client.post(
            "/api/v1/chatgpt/responses",
            json={
                "race_id": "202607230101",
                "history_id": prompt_payload["history_id"],
                "prompt_text": prompt_payload["prompt_text"],
                "response_text": "対抗は2番です。",
            },
        )
        assert saved.status_code == 201

        history = client.get("/api/v1/races/202607230101/chatgpt-predictions")
        assert history.status_code == 200
        assert history.json()[0]["response_text"] == "対抗は2番です。"
    finally:
        app.dependency_overrides.clear()
        db.close()
