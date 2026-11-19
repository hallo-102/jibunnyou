"""Append-only JSONL storage.

Snapshots are immutable by design: this module intentionally provides NO
update or delete functions for snapshots. Duplicate race_id + SHA256 is an
error. Results go to a separate file and never overwrite snapshots.
"""
from __future__ import annotations

import hashlib
import json
import os


def compute_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


class DuplicateSnapshotError(Exception):
    pass


def _ensure_parent(path):
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def _load_jsonl(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _append_jsonl(path, record):
    _ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


class SnapshotStore:
    """Append-only store for pre-race snapshots."""

    def __init__(self, path):
        self.path = path

    def load_all(self):
        return _load_jsonl(self.path)

    def append_snapshot(self, record):
        race_id = record.get("race_id")
        sha = record.get("sha256")
        for existing in self.load_all():
            if str(existing.get("race_id")) == str(race_id) and existing.get("sha256") == sha:
                raise DuplicateSnapshotError(
                    f"同一 race_id + 同一 SHA256 のスナップショットは既に保存されています: race_id={race_id}"
                )
        _append_jsonl(self.path, record)
        return record

    # NOTE: intentionally no update/delete methods.


class ResultStore:
    """Append-only store for post-race results (separate file)."""

    def __init__(self, path):
        self.path = path

    def load_all(self):
        return _load_jsonl(self.path)

    def append_result(self, record):
        _append_jsonl(self.path, record)
        return record


def default_data_paths(base_dir=None):
    if base_dir is None:
        base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(base_dir, exist_ok=True)
    return {
        "snapshots": os.path.join(base_dir, "snapshots.jsonl"),
        "results": os.path.join(base_dir, "results.jsonl"),
    }
