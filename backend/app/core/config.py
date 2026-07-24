from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_prefix="KEIBA_", extra="ignore")

    app_name: str = "Keiba AI Studio"
    app_version: str = "1.1.0"
    environment: str = "local"
    git_commit: str = "unknown"
    database_url: str = "sqlite:///./data/keiba_ai_studio.db"
    redis_url: str = "redis://redis:6379/0"

    data_root: Path = Field(default=Path("data"))
    excel_input_dir: Path = Field(default=Path("data/input"))
    odds_input_dir: Path = Field(default=Path("data/ozzu_csv"))
    legacy_output_dir: Path = Field(default=Path("data/output"))
    raw_snapshots_dir: Path = Field(default=Path("data/raw_snapshots"))
    normalized_dir: Path = Field(default=Path("data/normalized"))
    snapshots_dir: Path = Field(default=Path("data/snapshots"))
    staging_dir: Path = Field(default=Path("data/staging"))
    exports_dir: Path = Field(default=Path("data/exports"))
    logs_dir: Path = Field(default=Path("data/logs"))
    legacy_root: Path = Field(default=Path("."))
    legacy_runner_mode: str = "dry_run"
    prediction_runner_mode: Literal["dry_run", "execute"] = "dry_run"
    legacy_timeout_seconds: int = 900
    prediction_timeout_seconds: int = Field(default=1800, ge=60, le=7200)
    # 旧API方式は過去履歴の読取互換性と旧テスト用だけに残し、正式機能では無効にする。
    # "openai"は旧.envを読み込んでも起動を壊さない互換値で、Provider生成時は常に無効扱い。
    ai_provider: Literal["disabled", "openai", "mock"] = "disabled"
    ai_model: str = "gpt-5.4-mini-2026-03-17"
    ai_reasoning_effort: Literal["none", "low", "medium", "high", "xhigh"] = "low"
    ai_timeout_seconds: int = Field(default=120, ge=10, le=900)
    ai_max_output_tokens: int = Field(default=8000, ge=1000, le=32000)
    ai_max_retries: int = Field(default=2, ge=0, le=3)
    ai_retry_delays_seconds: list[int] = [2, 10]
    chatgpt_manual_prediction_enabled: bool = True
    chatgpt_url: str = "https://chatgpt.com/"
    chatgpt_recent_races_per_horse: int = Field(default=5, ge=1, le=10)
    chatgpt_prompt_length_warning: int = Field(default=50000, ge=1000, le=200000)
    collector_max_retries: int = Field(default=3, ge=0, le=3)
    collector_retry_delays_seconds: list[int] = [10, 60, 300]
    collector_min_interval_seconds: int = Field(default=60, ge=0, le=86400)
    job_execution_mode: Literal["inline", "queue"] = "inline"
    odds_freshness_warning_minutes: int = Field(default=30, ge=1, le=1440)
    odds_freshness_critical_minutes: int = Field(default=120, ge=1, le=10080)
    collector_approved_sources: list[str] = []

    cors_origins: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:18080",
        "http://127.0.0.1:18080",
    ]

    @property
    def writable_runtime_dirs(self) -> tuple[Path, ...]:
        """Return directories that the API and workers must be able to write."""

        return (
            self.raw_snapshots_dir,
            self.normalized_dir,
            self.snapshots_dir,
            self.staging_dir,
            self.exports_dir,
            self.logs_dir,
        )

    def ensure_runtime_directories(self) -> None:
        """Create writable runtime directories before serving requests."""

        for directory in self.writable_runtime_dirs:
            directory.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    """Return cached settings for the running process."""

    return Settings()
