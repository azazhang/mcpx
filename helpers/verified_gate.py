#!/usr/bin/env python3
"""Machine-global fail-closed producer/consumer artifact gate.

This module is intentionally project-agnostic. A project supplies a mutable gate
JSON, a pipeline name, a producer result JSON, and the exact artifact set a
consumer may read. The gate enforces generation identity, validation success,
artifact hashes, and candidate immutability; it never guesses research semantics.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import uuid

GATE_VERSION = 1


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    path = Path(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _atomic_json(path: Path, payload: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        with tmp.open("xb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise AssertionError(f"invalid or unreadable gate/result JSON: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AssertionError(f"JSON must be an object: {path}")
    return value


def _relative(path: Path, root: Path) -> str:
    path = Path(path).resolve()
    root = Path(root).resolve()
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise AssertionError(f"artifact escapes project root: {path} not under {root}") from exc


def begin_gate(gate_path: Path, *, pipeline: str, project_root: Path, metadata: dict | None = None) -> str:
    gate_path = Path(gate_path)
    project_root = Path(project_root).resolve()
    if gate_path.exists():
        try:
            prior = _read_json(gate_path)
        except AssertionError:
            prior = {}
        if prior.get("state") == "BUILDING":
            raise AssertionError(
                f"BUILD_ALREADY_IN_PROGRESS pipeline={pipeline} "
                f"build_id={prior.get('build_id')} gate={gate_path}"
            )
    build_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')}-{uuid.uuid4().hex}"
    payload = {
        "version": GATE_VERSION,
        "pipeline": pipeline,
        "build_id": build_id,
        "state": "BUILDING",
        "started_at_utc": _utc(),
        "gate_path": _relative(gate_path, project_root),
        "metadata": metadata or {},
    }
    _atomic_json(gate_path, payload)
    return build_id


def mark_failed(gate_path: Path, *, pipeline: str, build_id: str, reason: str, exit_code: int | None = None) -> dict:
    gate_path = Path(gate_path)
    current = _read_json(gate_path)
    if current.get("pipeline") != pipeline or current.get("build_id") != build_id:
        raise AssertionError("FAILED_REFUSED_GENERATION_MISMATCH")
    if current.get("state") != "BUILDING":
        raise AssertionError(f"FAILED_REFUSED_STATE_{current.get('state')}")
    failed = dict(current)
    failed.update({
        "state": "FAILED",
        "failed_at_utc": _utc(),
        "failure_reason": str(reason),
        "producer_exit_code": exit_code,
    })
    _atomic_json(gate_path, failed)
    return failed


def publish_ready(
    gate_path: Path,
    result_payload: dict,
    *,
    pipeline: str,
    build_id: str,
    project_root: Path,
    artifacts: list[Path],
    expectations: dict | None = None,
) -> dict:
    gate_path = Path(gate_path)
    project_root = Path(project_root).resolve()
    current = _read_json(gate_path)
    if current.get("state") != "BUILDING":
        raise AssertionError(f"READY_REFUSED_STATE_{current.get('state')}")
    if current.get("pipeline") != pipeline or current.get("build_id") != build_id:
        raise AssertionError("READY_REFUSED_GENERATION_MISMATCH")
    if result_payload.get("passed") is not True or result_payload.get("failures") not in ([], None):
        raise AssertionError(
            "READY_REFUSED_VALIDATION_NOT_PASSED "
            f"passed={result_payload.get('passed')} failures={result_payload.get('failures')}"
        )

    manifest: dict[str, dict] = {}
    for raw in artifacts:
        path = Path(raw)
        if not path.is_file():
            raise AssertionError(f"READY_REFUSED_MISSING_ARTIFACT {path}")
        rel = _relative(path, project_root)
        if rel in manifest:
            raise AssertionError(f"READY_REFUSED_DUPLICATE_ARTIFACT {rel}")
        manifest[rel] = {"sha256": sha256_file(path), "bytes": path.stat().st_size}

    ready = dict(current)
    ready.update({
        "state": "READY",
        "ready_at_utc": _utc(),
        "artifacts": manifest,
        "expectations": expectations or {},
        "producer_result_summary": {
            "passed": result_payload.get("passed"),
            "failures": result_payload.get("failures"),
            "recommendation": result_payload.get("recommendation"),
        },
    })
    _atomic_json(gate_path, ready)
    return ready


def require_ready(
    gate_path: Path,
    *,
    candidate_root: Path,
    pipeline: str,
    required_artifacts: list[str | Path],
) -> dict:
    candidate_root = Path(candidate_root).resolve()
    gate_path = Path(gate_path).resolve()
    gate = _read_json(gate_path)
    if gate.get("version") != GATE_VERSION:
        raise AssertionError(f"AUDIT_REFUSED_GATE_VERSION {gate.get('version')}")
    if gate.get("state") != "READY":
        raise AssertionError(
            f"AUDIT_REFUSED_CANDIDATE_NOT_READY state={gate.get('state')} build_id={gate.get('build_id')}"
        )
    if gate.get("pipeline") != pipeline:
        raise AssertionError(f"AUDIT_REFUSED_PIPELINE expected={pipeline} got={gate.get('pipeline')}")
    manifest = gate.get("artifacts")
    if not isinstance(manifest, dict):
        raise AssertionError("AUDIT_REFUSED_INVALID_ARTIFACT_MANIFEST")

    # Verify the complete producer generation.  Caller-supplied required_artifacts
    # are additional presence assertions, not the boundary of what gets hashed.
    declared = list(manifest)
    required: list[str] = []
    for raw in (required_artifacts or []):
        p = Path(raw)
        rel = p.as_posix() if not p.is_absolute() else _relative(p, candidate_root)
        required.append(rel)
        if rel not in manifest:
            raise AssertionError(f"AUDIT_REFUSED_ARTIFACT_NOT_MANIFESTED {rel}")

    for rel in declared:
        entry = manifest.get(rel)
        if not isinstance(entry, dict):
            raise AssertionError(f"AUDIT_REFUSED_INVALID_ARTIFACT_ENTRY {rel}")
        actual = candidate_root / rel
        if not actual.is_file():
            raise AssertionError(f"AUDIT_REFUSED_ARTIFACT_MISSING {rel}")
        if actual.stat().st_size != entry.get("bytes"):
            raise AssertionError(f"AUDIT_REFUSED_ARTIFACT_SIZE_MISMATCH {rel}")
        if sha256_file(actual) != entry.get("sha256"):
            raise AssertionError(f"AUDIT_REFUSED_ARTIFACT_HASH_MISMATCH {rel}")

    return {
        "candidate_root": str(candidate_root),
        "gate_path": str(gate_path),
        "gate_sha256": sha256_file(gate_path),
        "pipeline": pipeline,
        "build_id": gate.get("build_id"),
        "required_artifacts": {rel: dict(manifest[rel]) for rel in declared},
        "explicit_required_artifacts": required,
    }


def verify_unchanged(token: dict) -> None:
    root = Path(token["candidate_root"])
    gate_path = Path(token["gate_path"])
    if sha256_file(gate_path) != token["gate_sha256"]:
        raise AssertionError("CANDIDATE_CHANGED_DURING_AUDIT gate")
    for rel, entry in token["required_artifacts"].items():
        path = root / rel
        if not path.is_file():
            raise AssertionError(f"CANDIDATE_CHANGED_DURING_AUDIT missing {rel}")
        if path.stat().st_size != entry.get("bytes") or sha256_file(path) != entry.get("sha256"):
            raise AssertionError(f"CANDIDATE_CHANGED_DURING_AUDIT {rel}")
