from pathlib import Path

from keiba_v2.tracking import tracking_run, log_metrics, log_params


def test_sqlite_tracking_from_another_directory(tmp_path, monkeypatch):
    import mlflow

    # 本番と同じSQLite設定を隔離したルートで使用する。
    root = tmp_path / "日本語プロジェクト"
    monkeypatch.chdir(tmp_path)
    cfg = {"tracking_uri": "sqlite:///data/runtime/mlflow.db", "experiment_name": "regression"}
    with tracking_run(cfg, root, "morning_probe") as run:
        run_id = run.info.run_id
        log_metrics({"races": 24})
        log_params({"mode": "SHADOW"})

    # 実際の保存先・正常終了・パラメータと指標の読み戻しを確認する。
    assert (root / "data/runtime/mlflow.db").is_file()
    assert not (tmp_path / "data/runtime/mlflow.db").exists()
    saved = mlflow.get_run(run_id)
    assert saved.info.status == "FINISHED"
    assert saved.data.metrics["races"] == 24
    assert saved.data.params["mode"] == "SHADOW"
