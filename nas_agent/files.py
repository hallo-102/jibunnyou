"""新規専用ルート・私有状態・共有spoolを扱う。"""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import tempfile

from .schema import Rejected

POSIX = os.name == "posix"


def validate_admin_file(path, *, for_write=False):
    """管理者(root)所有の承認ファイルと全親を検査する。workerは更新できない。"""
    if not POSIX:
        raise Rejected("ADMIN_POLICY_REQUIRES_POSIX")
    if for_write and os.geteuid() != 0:
        raise Rejected("ADMIN_APPROVAL_REQUIRED")
    path = Path(path).absolute()
    for item in (path, *path.parents):
        info = item.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise Rejected("UNTRUSTED_ADMIN_POLICY_PERMISSIONS")
        if item == path and not stat.S_ISREG(info.st_mode):
            raise Rejected("UNTRUSTED_ADMIN_POLICY_PERMISSIONS")


def admin_json(path):
    validate_admin_file(path)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def private_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise Rejected("UNTRUSTED_CONFIG")
    if os.name == "posix":
        info = path.stat()
        if info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022:
            raise Rejected("UNTRUSTED_CONFIG_PERMISSIONS")
    return json.loads(path.read_text(encoding="utf-8"))


def atomic_json(path, payload, *, mode=0o640, gid=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise Rejected("UNSAFE_SPOOL_PATH")
    descriptor, temporary = tempfile.mkstemp(prefix=".nas-agent-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        if gid is not None:
            os.chown(temporary, -1, gid)  # 管理者の承認更新でも読取専用groupを維持する。
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def isolated(path, protected):
    root = Path(path).resolve()
    for item in protected:
        other = Path(item).resolve()
        if root.is_relative_to(other) or other.is_relative_to(root):
            raise Rejected("UNSAFE_AGENT_ROOT")
    return root


def beneath(root, path):
    root, path = Path(root).resolve(), Path(path)
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        raise Rejected("UNSAFE_SPOOL_PATH")
    return path.resolve()


def spool_json(root, path, limit):
    path = beneath(root, path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise Rejected("UNSAFE_SPOOL_FILE")
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise Rejected("UNSAFE_SPOOL_FILE")
    return json.loads(data.decode("utf-8"))


def spool_page(root, state, key, limit=1000):
    """履歴を残し、永続cursorの次から一巡する。失敗したファイルにも次回を独占させない。"""
    paths = sorted(Path(root).glob("*.json"))
    cursor = state.meta(key) or ""
    following = [path for path in paths if path.name > cursor]
    preceding = [path for path in paths if path.name <= cursor]
    for path in (following + preceding)[:limit]:
        state.meta(key, path.name)
        yield path


@contextmanager
def single_worker(path):
    """OSロックは電源断/異常終了で解放される。PIDファイルの強制削除はしない。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise Rejected("UNSAFE_LOCK")
    with path.open("a+b") as handle:
        try:
            handle.seek(0)
            if not handle.read(1):
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise Rejected("WORKER_ALREADY_RUNNING") from exc
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)
