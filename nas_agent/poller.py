"""Docker権限を持たない受付/結果報告プロセス。実行ファイルは呼ばない。"""
from __future__ import annotations

import json
from pathlib import Path
import time

from .files import atomic_json, beneath, single_worker, spool_json
from .report import comment
from .schema import Rejected, digest, request, utcnow
from .state import State


class Poller:
    def __init__(self, config, github):
        self.config, self.github = config, github
        self.spool = Path(config["spool_dir"])
        self.state = State(Path(config["state_dir"]) / "poller.sqlite")

    def cycle(self):
        with single_worker(Path(self.config["state_dir"]) / "poller.lock"):
            for issue in self.github.issues():
                try:
                    job, request_hash = request(issue)
                except Rejected as exc:
                    self.state.meta("rejected_issue:" + str(issue.get("number", 0)), str(exc))
                    continue
                hint = {"issue_number": issue["number"], "request_hash": request_hash}
                hint_path = beneath(self.spool / "inbox", self.spool / "inbox" / f"{issue['number']}.json")
                saved = self.spool / "outbox" / f"{issue['number']}.json"
                if saved.is_file() and not saved.is_symlink():
                    result = spool_json(self.spool / "outbox", saved, 65536)
                    if result.get("request_hash") == request_hash and result.get("status") in {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}:
                        hint_path.unlink(missing_ok=True)
                        continue
                atomic_json(hint_path, hint)
            self.state.meta("last_poll_at", utcnow().isoformat())
            for path in sorted((self.spool / "outbox").glob("*.json"))[:1000]:
                if path.is_symlink() or not path.stem.isdigit() or path.stat().st_size > 65536:
                    continue
                payload = spool_json(self.spool / "outbox", path, 65536)
                if payload["instance_id"] != self.config["instance_id"] or payload["issue_number"] != int(path.stem):
                    continue
                marker, body = comment(payload)
                key = digest(marker)
                previous = json.loads(self.state.meta("report:" + key) or "{}")
                revision = digest(payload)
                if previous.get("revision") == revision:
                    continue
                if payload["status"] == "RUNNING" and previous.get("status") == "RUNNING" and time.time() - previous.get("sent_at", 0) < 300:
                    continue
                comment_id = self.github.report(payload["issue_number"], marker, body, previous.get("comment_id"))
                self.state.meta("report:" + key, json.dumps({"comment_id": comment_id, "revision": revision, "status": payload["status"], "sent_at": time.time()}))
