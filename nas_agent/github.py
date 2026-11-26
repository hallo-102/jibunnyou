"""固定GitHub APIだけに接続する。トークン・HTTP本文をエラーへ出さない。"""
from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request

from .schema import REPOSITORY, Rejected


class Unavailable(RuntimeError):
    def __init__(self, code="GITHUB_UNAVAILABLE", retry_at=None, *, http_status=None, rate_limited=False, transport=None):
        super().__init__(code)
        self.code = code
        self.retry_at = retry_at or time.time() + 60
        self.http_status = http_status
        self.rate_limited = rate_limited
        self.transport = transport

    def diagnostic(self, phase):
        # HTTP本文、URL、認証ヘッダー、例外の原文は保存しない。
        return {"phase": phase, "code": self.code, "http_status": self.http_status,
                "rate_limited": self.rate_limited, "transport": self.transport, "retry_at": self.retry_at}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Unavailable("GITHUB_REDIRECT_REFUSED", http_status=code)


class GitHub:
    def __init__(self, token_file=None):
        self.token = None
        if token_file:
            import os
            path = Path(token_file)
            if path.is_symlink() or (os.name == "posix" and (path.stat().st_mode & 0o077 or path.stat().st_uid not in {0, os.geteuid()})):
                raise Rejected("UNSAFE_TOKEN_PERMISSIONS")
            self.token = path.read_text(encoding="utf-8").strip()
            if not self.token or any(c.isspace() for c in self.token):
                raise Rejected("INVALID_TOKEN_FILE")
        self.opener = urllib.request.build_opener(NoRedirect())
        self.etags = {}
        self.cache = {}
        self.retry_at = 0
        self.failures = 0

    def api(self, path, *, method="GET", payload=None, conditional=False):
        if not path.startswith((f"/repos/{REPOSITORY}", "/user")) or ".." in path:
            raise Rejected("UNSAFE_API_PATH")
        if time.time() < self.retry_at:
            raise Unavailable("GITHUB_BACKOFF", self.retry_at)
        headers = {"Accept": "application/vnd.github+json", "User-Agent": "keiba-nas-agent/1", "X-GitHub-Api-Version": "2022-11-28"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if conditional and path in self.etags:
            headers["If-None-Match"] = self.etags[path]
        data = None if payload is None else json.dumps(payload).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request("https://api.github.com" + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=20) as response:
                body = response.read(4 * 1024 * 1024 + 1)
                if len(body) > 4 * 1024 * 1024:
                    raise Unavailable("GITHUB_RESPONSE_TOO_LARGE")
                result = json.loads(body) if body else None
                if conditional and response.headers.get("ETag"):
                    self.etags[path] = response.headers["ETag"]
                    self.cache[path] = result
                self.failures = 0
                return result
        except urllib.error.HTTPError as exc:
            if exc.code == 304 and path in self.cache:
                return self.cache[path]
            self.failures += 1
            delay = min(3600, 60 * 2 ** min(self.failures - 1, 6))
            retry = exc.headers.get("Retry-After")
            if retry:
                try:
                    delay = max(delay, float(retry))
                except ValueError:
                    try:
                        delay = max(delay, parsedate_to_datetime(retry).timestamp() - time.time())
                    except (TypeError, ValueError):
                        pass
            exhausted = exc.headers.get("X-RateLimit-Remaining") == "0"
            if exhausted:
                try:
                    delay = max(delay, float(exc.headers.get("X-RateLimit-Reset", time.time() + delay)) - time.time() + 1)
                except (TypeError, ValueError):
                    pass
            self.retry_at = time.time() + delay
            limited = exc.code == 429 or (exc.code == 403 and (exhausted or bool(retry)))
            if limited:
                code = "GITHUB_RATE_LIMIT"
            else:
                code = "GITHUB_AUTH_OR_PERMISSION" if exc.code in {401, 403, 404} else "GITHUB_HTTP_ERROR"
            raise Unavailable(code, self.retry_at, http_status=exc.code, rate_limited=limited) from None
        except (OSError, ValueError) as exc:
            self.retry_at = time.time() + 60
            reason = exc.reason if isinstance(exc, urllib.error.URLError) else exc
            if isinstance(reason, TimeoutError):
                transport = "timeout"
            elif isinstance(reason, socket.gaierror):
                transport = "dns"
            elif isinstance(reason, ssl.SSLError):
                transport = "tls"
            elif isinstance(reason, ValueError):
                transport = "invalid_response"
            else:
                transport = "io"
            raise Unavailable(retry_at=self.retry_at, transport=transport) from None

    def issue(self, number):
        return self.api(f"/repos/{REPOSITORY}/issues/{int(number)}")

    def issues(self):
        # 全ページを走査し、停止中の依頼やclosedによるキャンセルを取りこぼさない。
        for page in range(1, 101):
            rows = self.api(f"/repos/{REPOSITORY}/issues?state=all&per_page=100&page={page}", conditional=True)
            yield from rows
            if len(rows) < 100:
                return
        raise Unavailable("GITHUB_BACKLOG_LIMIT")

    def identity_allowed(self, issue, requester_ids):
        author = issue["user"]
        if author["id"] not in requester_ids:
            return False
        repo = self.api(f"/repos/{REPOSITORY}")
        if repo["full_name"].lower() != REPOSITORY.lower():
            return False
        if repo["owner"]["id"] == author["id"]:
            return True
        permission = self.api(f"/repos/{REPOSITORY}/collaborators/{urllib.parse.quote(author['login'], safe='')}/permission")
        return permission.get("user", {}).get("id") == author["id"] and permission.get("permission") in {"admin", "write"}

    def report(self, issue_number, marker, body, comment_id=None):
        # POSTの応答喪失後は自分のmarkerコメントを探す。実行の再試行とは独立。
        me = self.api("/user")["id"]
        if comment_id is None:
            for page in range(1, 101):
                rows = self.api(f"/repos/{REPOSITORY}/issues/{issue_number}/comments?per_page=100&page={page}")
                found = [r for r in rows if r["user"]["id"] == me and r.get("body", "").startswith(marker + "\n")]
                if found:
                    comment_id = found[0]["id"]
                    break
                if len(rows) < 100:
                    break
            else:
                raise Unavailable("GITHUB_COMMENT_BACKLOG_LIMIT")
        content = marker + "\n" + body
        if comment_id is not None:
            result = self.api(f"/repos/{REPOSITORY}/issues/comments/{int(comment_id)}", method="PATCH", payload={"body": content})
        else:
            result = self.api(f"/repos/{REPOSITORY}/issues/{issue_number}/comments", method="POST", payload={"body": content})
        return result["id"]
