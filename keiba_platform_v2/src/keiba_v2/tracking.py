from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def tracking_run(cfg: dict, project_root: Path, run_name: str) -> Iterator[object | None]:
    if not bool(cfg.get("enabled", True)):
        yield None
        return
    try:
        import mlflow
    except ImportError:
        yield None
        return

    uri = str(cfg.get("tracking_uri", "data/runtime/mlruns"))
    if "://" not in uri:
        uri = str((project_root / uri).resolve())
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment(str(cfg.get("experiment_name", "keiba_platform_v2")))
    with mlflow.start_run(run_name=run_name) as run:
        yield run


def log_metrics(metrics: dict[str, float | int]) -> None:
    try:
        import mlflow
    except ImportError:
        return
    active = mlflow.active_run()
    if active is None:
        return
    clean = {str(k): float(v) for k, v in metrics.items() if isinstance(v, (int, float))}
    if clean:
        mlflow.log_metrics(clean)


def log_params(params: dict[str, object]) -> None:
    try:
        import mlflow
    except ImportError:
        return
    if mlflow.active_run() is None:
        return
    clean = {str(k): str(v)[:500] for k, v in params.items()}
    if clean:
        mlflow.log_params(clean)
