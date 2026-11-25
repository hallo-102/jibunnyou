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

from .files import atomic_json, beneath
from .schema import REPOSITORY, Rejected

AGENT_CODE = Path(__file__).resolve().parents[1]


def clean_environment(home):
    env = {key: os.environ[key] for key in ("PATH", "SystemRoot", "WINDIR", "TEMP", "TMP") if key in os.environ}
    env.update(HOME=str(home), USERPROFILE=str(home), PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1",
               GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
    return env


class Commands:
    def run(self, args, *, cwd, env, control, log=None):
        stream = subprocess.DEVNULL if log is None else open(log, "ab")
        process = None
        try:
            options = {"umask": 0o022} if os.name == "posix" else {}
            process = subprocess.Popen(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
                                       start_new_session=os.name != "nt", **options)
            while process.poll() is None:
                control()
                time.sleep(0.2)
            if process.returncode:
                raise Rejected("FIXED_COMMAND_FAILED")
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

    def checkout(self, sha, job_dir, control):
        source = beneath(self.config["checkout_root"], Path(self.config["checkout_root"]) / sha)
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
            self.commands.run(["git", "-C", str(source), "-c", "credential.helper=", "-c", "http.followRedirects=false", "fetch", "--depth=1", "origin", sha], cwd=job_dir, env=fetch_env, control=control)
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
