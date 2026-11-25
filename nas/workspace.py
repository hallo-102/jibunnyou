"""候補専用ルートの初期化・出力境界・新規JSON公開を共通化する。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import uuid

CODE_ROOT = Path(__file__).resolve().parents[1]
MARKER = ".nas-candidate-workspace.json"
CATEGORIES = {"runs", "legacy", "comparisons", "snapshots"}


def overlaps(left: Path, right: Path) -> bool:
    """resolveでsymlink/junctionを辿ってから包含を判定する。"""
    left, right = left.resolve(), right.resolve()
    return left.is_relative_to(right) or right.is_relative_to(left)


def protected_paths(extra=()) -> list[Path]:
    # 取得済み本番相当コピーと既知NASパスも、指定忘れで書込先にしない。
    return [CODE_ROOT, CODE_ROOT.parent / "keiba_yosou_2026_nas", CODE_ROOT.parent / "keiba_yosou_2026-2",
            Path("/volume1/docker/keiba_yosou_2026_nas"), *map(Path, extra)]


def initialize(root: Path, production_root: Path, *, extra_protected=()) -> Path:
    """既存ディレクトリへの後付け許可をせず、新規ルートだけを作る。"""
    root, production_root = root.resolve(), production_root.resolve(strict=True)
    protected = protected_paths([production_root, *extra_protected])
    if any(overlaps(root, path) for path in protected):
        raise ValueError("候補作業ルートは本番・ソース・固定入力から独立させてください")
    root.mkdir(parents=True, exist_ok=False)
    payload = {"schema_version": 1, "scope": "offline", "workspace_id": str(uuid.uuid4()),
               "host_root": str(root), "production_root": str(production_root),
               "protected_roots": [str(path.resolve()) for path in protected]}
    with (root / MARKER).open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return root


class Workspace:
    """マーカーで明示されたルートの定義済みカテゴリだけを許可する。"""
    def __init__(self, root: Path):
        self.root = root.resolve(strict=True)
        self.marker = self.root / MARKER
        if self.marker.is_symlink() or not self.marker.is_file():
            raise ValueError("候補専用workspaceの初期化が必要です")
        self.policy = json.loads(self.marker.read_text(encoding="utf-8"))
        if self.policy.get("schema_version") != 1 or self.policy.get("scope") != "offline":
            raise ValueError("workspace形式が不正です")
        uuid.UUID(self.policy["workspace_id"])
        self.protected = protected_paths(self.policy["protected_roots"])
        if any(overlaps(self.root, path) for path in self.protected):
            raise ValueError("workspaceが本番・コードの保護領域と重複しています")

    def output(self, path: Path, category: str, *, readonly=(), new=False) -> Path:
        """固定入力/既存成果物への参照を含む出力を、書込前に拒否する。"""
        if category not in CATEGORIES:
            raise ValueError("未知の出力カテゴリ")
        path = path.absolute()
        # 解決後だけでなく、途中のsymlink/junctionも拒否する。
        for parent in [path, *path.parents]:
            if parent == self.root:
                break
            if parent.is_symlink() or (parent.exists() and parent.resolve() != parent):
                raise ValueError("出力先にsymlink/junctionを使用できません")
        resolved = path.resolve()
        category_root = self.root / category
        if not resolved.is_relative_to(category_root) or not category_root.resolve().is_relative_to(self.root):
            raise ValueError("出力は候補workspaceの専用カテゴリ内に限定します")
        if any(overlaps(resolved, item) for item in [*self.protected, *map(Path, readonly)]):
            raise ValueError("出力が本番・コード・固定入力と重複しています")
        if new and (path.exists() or path.is_symlink()):
            raise FileExistsError(f"既存成果物は上書きしません: {path}")
        return resolved

    def new_json(self, path: Path, payload: dict, *, readonly=()) -> None:
        """予測可能な.tmpやhardlinkを使わず、JSONを排他的に新規作成する。"""
        path = self.output(path, "comparisons", readonly=readonly, new=True)
        # 先に全体を直列化し、形式不正で空の成果物を作らない。
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8")
        path.parent.mkdir(parents=True, exist_ok=True)
        # xbはOSのO_EXCLを使う。同時作成でも既存targetを置換しない。
        # I/O故障で新規ファイルが不完全になれば例外とし、成功結果として扱わない。
        with path.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--protect", type=Path, action="append", default=[])
    args = parser.parse_args()
    print(initialize(args.root, args.production_root, extra_protected=args.protect))
