import json
from datetime import date
from importlib import import_module
from pathlib import Path


def _first_existing_directory(*candidates: Path) -> Path:
    """Return the first input directory available on host or in Compose."""

    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    raise AssertionError(f"test input directory was not found: {candidates}")


def test_phase2_imports_existing_excel_and_ozzu(monkeypatch, tmp_path):
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "keiba_ai_studio_test.db"
    excel_input_dir = _first_existing_directory(
        repo_root / "data" / "input",
        repo_root / "data" / "raw" / "excel",
    )
    odds_input_dir = _first_existing_directory(
        repo_root / "data" / "ozzu_csv",
        repo_root / "data" / "raw" / "odds",
    )
    legacy_output_dir = _first_existing_directory(
        repo_root / "data" / "output",
        repo_root / "data" / "raw" / "legacy_output",
    )
    legacy_root = repo_root / "legacy" if (repo_root / "legacy").is_dir() else repo_root

    monkeypatch.setenv("KEIBA_DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("KEIBA_EXCEL_INPUT_DIR", str(excel_input_dir))
    monkeypatch.setenv("KEIBA_ODDS_INPUT_DIR", str(odds_input_dir))
    monkeypatch.setenv("KEIBA_LEGACY_OUTPUT_DIR", str(legacy_output_dir))
    monkeypatch.setenv("KEIBA_STAGING_DIR", str(tmp_path / "staging"))
    monkeypatch.setenv("KEIBA_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("KEIBA_LOGS_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("KEIBA_LEGACY_ROOT", str(legacy_root))
    monkeypatch.setenv("KEIBA_LEGACY_RUNNER_MODE", "dry_run")

    config = import_module("app.core.config")
    config.get_settings.cache_clear()

    init_db = import_module("app.db.init_db").init_db
    session_module = import_module("app.db.session")
    models = import_module("app.db.models")
    excel_importer = import_module("app.legacy_bridge.excel_importer")
    odds_importer = import_module("app.legacy_bridge.odds_importer")
    jobs_endpoint = import_module("app.api.v1.endpoints.jobs")
    schemas = import_module("app.schemas.api")
    betting_service = import_module("app.services.betting")
    data_quality_service = import_module("app.services.data_quality")

    init_db()

    with session_module.SessionLocal() as db:
        workbook = excel_input_dir / "馬の競走成績_20260705.xlsx"
        feature_workbook = legacy_output_dir / "馬の競走成績_with_feat_20260705.xlsx"
        odds_csv = odds_input_dir / "OZZU_20260705.csv"

        workbook_summary = excel_importer.import_race_workbook(db, workbook)
        feature_summary = excel_importer.import_race_workbook(db, feature_workbook)
        odds_summary = odds_importer.import_odds_csv(db, odds_csv)

        assert workbook_summary.races > 0
        assert workbook_summary.entries > 0
        assert workbook_summary.past_performances > 0
        assert feature_summary.entries > 0
        assert odds_summary.odds > 0
        assert db.query(models.Race).count() > 0
        assert db.query(models.RaceEntry).count() > 0
        assert db.query(models.HorsePastPerformance).count() > 0
        assert db.query(models.OddsSnapshot).count() > 0

        race = db.get(models.Race, "202602010801")
        assert race.venue == "函館"
        assert race.start_time == "09:50"
        assert race.name == "2歳未勝利"
        assert race.course == "芝1200"

        entry = (
            db.query(models.RaceEntry)
            .filter(models.RaceEntry.race_id == "202602010801", models.RaceEntry.horse_no == 4)
            .one()
        )
        assert entry.prediction_rank == 1
        assert entry.prediction_score is not None

        quality_summary = data_quality_service.run_data_quality_checks(
            db,
            race_date=workbook_summary.race_date,
        )
        db.commit()
        assert quality_summary.checked_races > 0
        race_quality = (
            db.query(models.RaceQualityStatus)
            .filter(models.RaceQualityStatus.race_id == "202602010801")
            .one()
        )
        assert race_quality.status in {"GREEN", "YELLOW"}

        entry_ai_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="ai.second_opinion",
                race_date=workbook_summary.race_date,
                race_id="202602010811",
                force=True,
            ),
            db,
        )
        entry_ai_message = json.loads(entry_ai_job.message)
        assert entry_ai_job.status == "completed"
        assert entry_ai_message["runs"] == 1
        assert entry_ai_message["evaluations"] > 0
        assert entry_ai_message["final_predictions"] > 0

        entry_prediction_run = db.get(models.PredictionRun, "entry-202602010811")
        assert entry_prediction_run is not None
        assert entry_prediction_run.model_version == "race-entry-import"
        assert (
            db.query(models.PredictionResult)
            .filter(
                models.PredictionResult.prediction_run_id == entry_prediction_run.id,
                models.PredictionResult.race_id == "202602010811",
            )
            .count()
            > 0
        )
        assert (
            db.query(models.AiHorseEvaluation)
            .filter(models.AiHorseEvaluation.race_id == "202602010811")
            .count()
            > 0
        )

        broken_race = models.Race(
            race_id="202699999999",
            race_date=date(2026, 7, 5),
            headcount=2,
        )
        db.add(broken_race)
        db.add(
            models.RaceEntry(
                race_id=broken_race.race_id,
                horse_no=1,
                horse_name="品質チェック用テスト馬",
                win_odds=0,
            )
        )
        db.commit()

        broken_summary = data_quality_service.run_data_quality_checks(
            db,
            race_id=broken_race.race_id,
        )
        db.commit()
        assert broken_summary.red == 1

        job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="collection.race_info",
                race_date=workbook_summary.race_date,
                force=True,
            ),
            db,
        )
        message = json.loads(job.message)
        assert job.status == "completed"
        assert message["legacy"]["mode"] == "dry_run"
        assert message["import"]["races"] > 0
        assert (tmp_path / "exports" / "runs" / job.id / "output_manifest_v1.json").exists()
        assert (tmp_path / "logs" / "runs" / job.id / "stdout.log").exists()
        db.refresh(entry)
        assert entry.prediction_rank == 1
        assert entry.prediction_score is not None

        result_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="collection.results",
                race_date=workbook_summary.race_date,
                force=True,
            ),
            db,
        )
        result_message = json.loads(result_job.message)
        assert result_job.status == "completed"
        assert result_message["legacy"]["mode"] == "dry_run"
        assert result_message["import"]["results"] > 0
        assert (
            db.query(models.RawFileRecord)
            .filter(models.RawFileRecord.file_type == "result_workbook")
            .count()
            > 0
        )
        assert db.query(models.RaceResult).count() > 0

        prediction_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="prediction.run",
                race_date=workbook_summary.race_date,
                race_id="202602010801",
                force=True,
            ),
            db,
        )
        prediction_message = json.loads(prediction_job.message)
        assert prediction_job.status == "completed"
        assert prediction_message["legacy"]["mode"] == "dry_run"
        assert prediction_message["prediction"]["results"] > 0

        prediction_run = db.get(models.PredictionRun, prediction_job.id)
        assert prediction_run is not None
        assert prediction_run.result_count > 0
        assert prediction_run.mismatch_count == 0
        assert prediction_run.input_manifest_sha256 is not None
        assert len(prediction_run.input_manifest_sha256) == 64
        assert (
            db.query(models.ArtifactFile)
            .filter(models.ArtifactFile.prediction_run_id == prediction_job.id)
            .count()
            == 6
        )
        assert (
            db.query(models.JobLog)
            .filter(models.JobLog.job_run_id == prediction_job.id)
            .count()
            >= 2
        )
        assert (
            db.query(models.AuditLog)
            .filter(
                models.AuditLog.entity_type == "job_runs",
                models.AuditLog.entity_id == prediction_job.id,
            )
            .count()
            == 1
        )

        prediction_result = (
            db.query(models.PredictionResult)
            .filter(
                models.PredictionResult.prediction_run_id == prediction_job.id,
                models.PredictionResult.race_id == "202602010801",
                models.PredictionResult.horse_no == 4,
            )
            .one()
        )
        assert prediction_result.prediction_rank == 1
        assert prediction_result.prediction_score is not None
        assert prediction_result.risk_reason
        assert prediction_result.evaluation_reason
        assert (tmp_path / "exports" / "runs" / prediction_job.id / "engine_prediction_v1.json").exists()

        ai_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="ai.second_opinion",
                race_date=workbook_summary.race_date,
                race_id="202602010801",
                force=True,
            ),
            db,
        )
        ai_message = json.loads(ai_job.message)
        assert ai_job.status == "completed"
        assert ai_message["runs"] == 1
        assert ai_message["evaluations"] > 0
        assert ai_message["final_predictions"] > 0

        ai_evaluations = (
            db.query(models.AiHorseEvaluation)
            .filter(models.AiHorseEvaluation.race_id == "202602010801")
            .all()
        )
        assert any(evaluation.ai_adjust_score > 0 for evaluation in ai_evaluations)
        assert any(evaluation.ai_adjust_score < 0 for evaluation in ai_evaluations)
        assert all(evaluation.ai_reason for evaluation in ai_evaluations)
        assert all(evaluation.ai_bet_role for evaluation in ai_evaluations)

        final_prediction = (
            db.query(models.FinalPrediction)
            .filter(models.FinalPrediction.race_id == "202602010801")
            .order_by(models.FinalPrediction.final_rank)
            .first()
        )
        assert final_prediction is not None
        assert final_prediction.final_score is not None
        assert final_prediction.final_rank == 1

        bet_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="bet.generate",
                race_date=workbook_summary.race_date,
                race_id="202602010801",
                force=True,
            ),
            db,
        )
        bet_message = json.loads(bet_job.message)
        assert bet_job.status == "completed"
        assert bet_message["generated"] == 1

        candidate = (
            db.query(models.BetCandidate)
            .filter(models.BetCandidate.race_id == "202602010801")
            .one()
        )
        assert candidate.rank in {"S", "A", "B"}
        assert candidate.status == "candidate"
        assert candidate.bet_type == "3連複"
        assert candidate.points > 0
        assert candidate.total_amount <= candidate.max_race_amount
        assert candidate.combinations

        result = betting_service.upsert_race_result(
            db,
            schemas.RaceResultCreate(
                race_id="202602010801",
                race_date=workbook_summary.race_date,
                finish_order=candidate.combinations[0],
                payout_amount=8600,
            ),
        )
        assert result.payout_amount == 8600

        settlement_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="result.settlement",
                race_id="202602010801",
                force=True,
            ),
            db,
        )
        settlement_message = json.loads(settlement_job.message)
        assert settlement_job.status == "completed"
        assert settlement_message["settled"] == 1
        assert settlement_message["hits"] == 1

        settlement = (
            db.query(models.BetSettlement)
            .filter(models.BetSettlement.bet_candidate_id == candidate.id)
            .one()
        )
        assert settlement.is_hit is True
        assert settlement.profit_loss == 8600 - candidate.total_amount
        assert settlement.roi > 100

        analytics = betting_service.analytics_summary(db, race_date=workbook_summary.race_date)
        assert analytics.settled_bets == 1
        assert analytics.hits == 1
        assert analytics.profit_loss == settlement.profit_loss
        assert db.query(models.ReviewNote).filter(models.ReviewNote.race_id == "202602010801").count() == 1

        ai_bet_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="ai.bet_correction",
                race_date=workbook_summary.race_date,
                race_id="202602010801",
                force=True,
            ),
            db,
        )
        ai_bet_message = json.loads(ai_bet_job.message)
        assert ai_bet_job.status == "completed"
        assert ai_bet_message["strategies"] == 1
        assert db.query(models.AiBetStrategy).filter(models.AiBetStrategy.race_id == "202602010801").count() == 1
        assert (
            db.query(models.BetCandidate)
            .filter(
                models.BetCandidate.race_id == "202602010801",
                models.BetCandidate.strategy.like("AI補正%"),
            )
            .count()
            == 1
        )

        blocked_job = jobs_endpoint.create_job(
            schemas.JobCreate(
                job_type="prediction.run",
                race_date=workbook_summary.race_date,
                force=True,
            ),
            db,
        )
        assert blocked_job.status == "failed"
        assert "RED" in (blocked_job.message or "")
