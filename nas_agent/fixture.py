"""Prepare a comparison fixture from a locally pinned, read-only capture archive."""
import argparse
from contextlib import nullcontext
from datetime import date
import hashlib
from pathlib import Path
import re
from tempfile import TemporaryDirectory

from .comparison import MAX_INPUT_BYTES, MAX_INPUT_FILES, MAX_JSON_BYTES, sha256
from .files import atomic_json, beneath, spool_json
from .schema import Rejected

REQUIRED = {"snapshot_index.json", "capture_evidence.json"}


def source_manifest(entry):
    root = Path(entry["root"]).resolve(strict=True)
    manifest = beneath(root, root / "manifest.json")
    if not manifest.is_file() or manifest.stat().st_size > MAX_JSON_BYTES or sha256(manifest) != entry["manifest_sha256"]:
        raise Rejected("INPUT_MANIFEST_MISMATCH")
    spec = spool_json(root, manifest, MAX_JSON_BYTES)
    if not isinstance(spec, dict) or not isinstance(spec.get("files_sha256"), dict):
        raise Rejected("INVALID_FIXTURE_SOURCE_MANIFEST")
    files = spec["files_sha256"]
    settled = {"settled.xlsx", "settled.json"} & files.keys()
    if (spec.get("schema_version") != 1 or spec.get("evaluation_mode") not in {"formal", "reference"}
            or len(settled) != 1 or not REQUIRED <= files.keys() or len(files) > MAX_INPUT_FILES
            or any(not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum) or
                   (name not in REQUIRED | settled and not re.fullmatch(r"archive/(?:[\w.-]+/)*[\w.-]+", name)) or
                   set(name.split("/")) & {".", ".."} for name, checksum in files.items())):
        raise Rejected("INVALID_FIXTURE_SOURCE_MANIFEST")
    return root, spec, settled.pop()


def verify_sources(entry, *, staged=None, control=lambda: None):
    root, spec, settled = source_manifest(entry)
    remaining = MAX_INPUT_BYTES
    for name, expected in spec["files_sha256"].items():
        control()
        source = beneath(root, root / name)
        if not source.is_file():
            raise Rejected("FIXTURE_SOURCE_MISSING")
        output = nullcontext()
        if staged is not None:
            target = staged / name
            target.parent.mkdir(parents=True, exist_ok=True)
            output = target.open("xb")
        checksum = hashlib.sha256()
        with output as destination, source.open("rb") as data:
            while chunk := data.read(1024 * 1024):
                control()
                remaining -= len(chunk)
                if remaining < 0:
                    raise Rejected("FIXTURE_SOURCE_LIMIT")
                checksum.update(chunk)
                if destination is not None:
                    destination.write(chunk)
        if checksum.hexdigest() != expected:
            raise Rejected("FIXTURE_SOURCE_SHA_MISMATCH")
    return spec["evaluation_mode"], settled


def prepare_fixture(entry, day, workspace, control):
    from tools.prepare_ab_comparison import prepare

    with TemporaryDirectory(dir=workspace) as temp:
        staged = Path(temp)
        mode, settled = verify_sources(entry, staged=staged, control=control)
        output = workspace / "output"
        prepare(staged / "snapshot_index.json", staged / settled, staged / "capture_evidence.json",
                output, date(int(day[:4]), int(day[4:6]), int(day[6:])), evaluation_mode=mode, control=control)
    return {"summary": {"evaluation_mode": mode},
            "artifacts_sha256": {name: sha256(output / name) for name in
                                 ("manifest.json", "run.json", "snapshots.json", "results.json")}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", type=Path)
    args = parser.parse_args()
    workspace = args.spec.parent
    spec = spool_json(workspace, args.spec, MAX_JSON_BYTES)
    result = prepare_fixture(spec["input"], spec["raceday"], workspace, lambda: None)
    atomic_json(workspace / "result.json", result, mode=0o600)


if __name__ == "__main__":
    main()
