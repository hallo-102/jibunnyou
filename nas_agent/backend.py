"""ローカル固定ハンドラー。Issue値をコマンドやDocker引数へ展開しない。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

from .audit import audit_args, audit_result, verify_fixture
from .comparison import MAX_JSON_BYTES, comparison_result, stage_fixture
from .files import atomic_json, beneath, isolated, spool_json
from .schema import AUDIT_SHA, COMPARISON_SHA, REPOSITORY, Rejected

AGENT_CODE = Path(__file__).resolve().parents[1]


def clean_environment(home):
    env = {key: os.environ[key] for key in ("PATH", "SystemRoot", "WINDIR", "TEMP", "TMP") if key in os.environ}
    env.update(HOME=str(home), USERPROFILE=str(home), PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1",
               GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
    return env


class Commands:
    def run(self, args, *, cwd, env, control, log=None, check=True):
        stream = subprocess.DEVNULL if log is None else open(log, "ab")
        process = None
        try:
            options = {"umask": 0o022} if os.name == "posix" else {}
            process = subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                       start_new_session=os.name != "nt", **options)
            while process.poll() is None:
                control()
                time.sleep(0.2)
            if check and process.returncode:
                raise Rejected("FIXED_COMMAND_FAILED")
            return process.returncode
        finally:
            if process is not None and process.poll() is None:
                if os.name != "nt":
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    if os.name != "nt":
                        os.killpg(process.pid, signal.SIGKILL)
                    else:
                        process.kill()
                    process.wait()
            if log is not None:
                stream.close()


class Backend:
    def __init__(self, config, read_token=None, commands=None):
        self.config, self.read_token = config, read_token
        self.commands = commands or Commands()

    def checkout(self, sha, job_dir, control, *, checkout_root=None, full_history=False):
        root = Path(self.config["checkout_root"]) if checkout_root is None else checkout_root
        source = beneath(root, root / sha)
        env = clean_environment(job_dir / "home")
        if not source.exists():
            source.mkdir(parents=True)
            self.commands.run(["git", "init", str(source)], cwd=job_dir, env=env, control=control)
            self.commands.run(["git", "-C", str(source), "config", "core.hooksPath", str(job_dir / "no-hooks")], cwd=job_dir, env=env, control=control)
            self.commands.run(["git", "-C", str(source), "remote", "add", "origin", f"https://github.com/{REPOSITORY}.git"], cwd=job_dir, env=env, control=control)
            fetch_env = dict(env)
            # private repo用read tokenはGitプロセスの環境だけ。Gitログは記録しない。
            if self.read_token:
                import base64
                header = base64.b64encode(("x-access-token:" + self.read_token).encode()).decode()
                fetch_env.update(GIT_CONFIG_COUNT="3", GIT_CONFIG_KEY_0="credential.helper", GIT_CONFIG_VALUE_0="",
                                 GIT_CONFIG_KEY_1="http.https://github.com/.extraheader", GIT_CONFIG_VALUE_1="Authorization: Basic " + header,
                                 GIT_CONFIG_KEY_2="http.followRedirects", GIT_CONFIG_VALUE_2="false")
            depth = [] if full_history else ["--depth=1"]
            self.commands.run(["git", "-C", str(source), "-c", "credential.helper=", "-c", "http.followRedirects=false", "fetch", *depth, "origin", sha], cwd=job_dir, env=fetch_env, control=control)
            self.commands.run(["git", "-C", str(source), "checkout", "--detach", "FETCH_HEAD"], cwd=job_dir, env=env, control=control)
        self.verify_checkout(source, sha, env)
        return source

    @staticmethod
    def verify_checkout(source, sha, env):
        def git(*args):
            return subprocess.check_output(["git", "-C", str(source), *args], env=env, stderr=subprocess.DEVNULL, text=True).strip()
        if git("rev-parse", "HEAD") != sha or git("status", "--porcelain", "--untracked-files=all") or git("ls-files", "--others", "--ignored", "--exclude-standard") or git("remote", "get-url", "origin") != f"https://github.com/{REPOSITORY}.git":
            raise Rejected("CHECKOUT_NOT_FIXED_AND_CLEAN")

    def stop_job_containers(self, project):
        # 予約済みの、このagent instance/jobのprojectだけを停止対象にする。
        env = clean_environment(Path(self.config["state_dir"]) / "docker-home")
        try:
            rows = subprocess.check_output(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}",
                                            "--filter", f"label=io.keiba.nas_agent.instance={self.config['instance_id']}"], env=env, stderr=subprocess.DEVNULL, timeout=20, text=True).split()
            if rows:
                subprocess.run(["docker", "stop", "--time", "10", *rows], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=True)
        except (OSError, subprocess.SubprocessError):
            pass  # 結果にかかわらず台帳のrecovery_requiredを管理者が解除するまで次を実行しない。

    def no_orphans(self):
        result = subprocess.run(["docker", "ps", "-aq", "--filter", f"label=io.keiba.nas_agent.instance={self.config['instance_id']}"],
                                capture_output=True, text=True, timeout=20, env=clean_environment(Path(self.config["state_dir"]) / "docker-home"))
        return result.returncode == 0 and not result.stdout.strip()

    def run(self, job, policy, control):
        control()
        if job["job_type"] == "healthcheck":
            architecture = platform.machine()
            if architecture not in {"aarch64", "arm64", "AMD64", "x86_64"}:
                architecture = "unknown"
            return {"summary": {"architecture": architecture, "python_version": platform.python_version()}, "artifacts_sha256": {}}
        if job["job_type"] == "full_feature_audit":
            return self.run_audit(job, policy, control)
        if job["job_type"] == "ab_comparison":
            return self.run_comparison(job, policy, control)
        if job["job_type"] != "issue16_acceptance":
            raise Rejected("UNSUPPORTED_HANDLER")
        job_dir = beneath(self.config["state_dir"], Path(self.config["state_dir"]) / "jobs" / job["job_id"])
        job_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        (job_dir / "home").mkdir()
        project = "nasagent-" + self.config["instance_id"][:8] + "-" + hashlib.sha256(job["job_id"].encode()).hexdigest()[:12]
        finished = False
        try:
            if not self.no_orphans():
                raise Rejected("ORPHAN_REQUIRES_ADMIN")
            source = self.checkout(job["commit_sha"], job_dir, control)
            spec = {"job": job, "policy": policy, "source": str(source), "workspace": str(Path(self.config["validation_root"]) / job["job_id"]),
                    "instance_id": self.config["instance_id"], "project": project, "result_path": str(job_dir / "result.json")}
            atomic_json(job_dir / "spec.json", spec)
            # ホスト専用の固定helper。GitHub/PATの環境・ファイルはコンテナへ渡さない。
            self.commands.run([self.config["validation_python"], "-m", "nas_agent.acceptance_host", str(job_dir / "spec.json")],
                              cwd=AGENT_CODE, env=clean_environment(job_dir / "home"), control=control, log=job_dir / "execution.log")
            control()
            self.verify_checkout(source, job["commit_sha"], clean_environment(job_dir / "home"))
            result = json.loads((job_dir / "result.json").read_text(encoding="utf-8"))
            finished = True
            return result
        finally:
            if not finished:
                self.stop_job_containers(project)

    def run_audit(self, job, policy, control):
        if job["commit_sha"] != AUDIT_SHA:
            raise Rejected("INVALID_AUDIT_COMMIT")
        entry = policy["inputs"][job["input_id"]]
        fixture = verify_fixture(entry)
        root = Path(self.config["validation_root"])
        workspace = beneath(root, root / job["job_id"])
        # policyは起動後にも更新できるため、実行時の入力と出力を照合する。
        isolated(workspace, [fixture, policy["production_root"], AGENT_CODE, self.config["checkout_root"]])
        workspace.mkdir(mode=0o700, exist_ok=False)
        home = workspace / "home"
        home.mkdir()
        output = workspace / "output"
        output.mkdir()
        source = self.checkout(job["commit_sha"], workspace, control, checkout_root=workspace / "checkout")
        env = clean_environment(home)
        env.update(TMPDIR=str(home), TMP=str(home), TEMP=str(home))
        started = time.monotonic()
        control()
        code = self.commands.run(audit_args(self.config["validation_python"], source, fixture, output, job["raceday"]),
                                 cwd=source, env=env, control=control, log=workspace / "execution.log", check=False)
        elapsed = round(time.monotonic() - started, 3)
        control()
        self.verify_checkout(source, job["commit_sha"], env)
        verify_fixture(entry)
        return audit_result(output, code, elapsed)

    def run_comparison(self, job, policy, control):
        if job["commit_sha"] != COMPARISON_SHA:
            raise Rejected("INVALID_COMPARISON_COMMIT")
        entry = policy["inputs"][job["input_id"]]
        root = Path(self.config["validation_root"])
        workspace = beneath(root, root / job["job_id"])
        isolated(workspace, [entry["root"], policy["production_root"], AGENT_CODE, self.config["checkout_root"]])
        workspace.mkdir(mode=0o700, exist_ok=False)
        home = workspace / "home"
        home.mkdir()
        fixture = workspace / "input"
        hashes = stage_fixture(entry, job["raceday"], fixture, control)
        settings = spool_json(fixture, fixture / "run.json", MAX_JSON_BYTES)
        evaluation_mode = settings.get("evaluation_mode", "formal")
        source = self.checkout(job["commit_sha"], workspace, control, checkout_root=workspace / "checkout",
                               full_history=True)
        output = workspace / "output"
        if evaluation_mode == "reference":
            output = output / "reference"
        output.mkdir(parents=True)
        env = clean_environment(home)
        env.update(TMPDIR=str(home), TMP=str(home), TEMP=str(home), KEIBA_SCORING_MODEL_VERSION="legacy")
        control()
        code = self.commands.run([self.config["validation_python"], str(AGENT_CODE / "nas_agent/comparison_ab_only.py"),
                                  str(fixture / "run.json"), str(output)],
                                 cwd=source, env=env, control=control, log=workspace / "execution.log", check=False)
        control()
        self.verify_checkout(source, job["commit_sha"], env)
        return comparison_result(output, job["commit_sha"], hashes, code, evaluation_mode)
