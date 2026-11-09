#!/usr/bin/env python3
"""Validate the tagged CI run whose artifacts a release recovery will reuse."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from pathlib import Path


REUSED_ARTIFACTS = {
    "ffi-linux-x86_64": "Build FFI (linux-x86_64)",
    "ffi-macos-x86_64": "Build FFI (macos-x86_64)",
    "ffi-macos-aarch64": "Build FFI (macos-aarch64)",
    "ffi-windows-x86_64": "Build FFI (windows-x86_64)",
    "polyglot-sql-wasi": "wasi-module",
}


def validate_inputs(release_tag: str, run_id: str) -> None:
    if not re.fullmatch(r"v\d+\.\d+\.\d+", release_tag):
        raise ValueError("release_tag must be a stable release tag such as v0.14.1")
    if not re.fullmatch(r"[1-9]\d*", run_id):
        raise ValueError("source_run_id must be a positive integer")


def validate_source(
    release_tag: str,
    run_id: str,
    tag_sha: str,
    run: dict,
    jobs: list[dict],
    artifacts: list[dict],
) -> dict[str, str]:
    validate_inputs(release_tag, run_id)
    if not re.fullmatch(r"[0-9a-f]{40}", tag_sha):
        raise ValueError("release tag did not resolve to a commit SHA")
    expected = {
        "id": int(run_id),
        "event": "push",
        "path": ".github/workflows/ci.yml",
        "head_branch": release_tag,
        "head_sha": tag_sha,
        "status": "completed",
    }
    for key, value in expected.items():
        if run.get(key) != value:
            raise ValueError(f"source run {key} must be {value!r}")
    if run.get("conclusion") not in {"success", "failure"}:
        raise ValueError("source run must have finished successfully or with a failure")

    for name in ("quality", "rust-test", *REUSED_ARTIFACTS.values()):
        matches = [job for job in jobs if job.get("name") == name]
        if len(matches) != 1 or matches[0].get("conclusion") != "success":
            raise ValueError(f"source run requires a successful {name!r} job")

    artifact_ids = []
    for name in REUSED_ARTIFACTS:
        matches = [artifact for artifact in artifacts if artifact.get("name") == name]
        if len(matches) != 1:
            raise ValueError(f"source run requires exactly one {name!r} artifact")
        artifact = matches[0]
        source = artifact.get("workflow_run", {})
        if (
            artifact.get("expired") is not False
            or artifact.get("size_in_bytes", 0) <= 0
            or source.get("id") != int(run_id)
            or source.get("head_sha") != tag_sha
            or source.get("head_branch") != release_tag
        ):
            raise ValueError(f"artifact {name!r} is expired, empty, or from another source")
        artifact_id = artifact.get("id")
        if type(artifact_id) is not int or artifact_id <= 0:
            raise ValueError(f"artifact {name!r} has an invalid ID")
        artifact_ids.append(str(artifact_id))
    if len(set(artifact_ids)) != len(artifact_ids):
        raise ValueError("source artifacts must have distinct IDs")
    return {"source_sha": tag_sha, "artifact_ids": ",".join(artifact_ids)}


def github_api(endpoint: str) -> dict:
    return json.loads(subprocess.check_output(["gh", "api", endpoint], text=True))


def validate_recovery_build(
    release_tag: str,
    source_run_id: str,
    source_sha: str,
    recovery_run_id: str,
    run: dict,
    jobs: list[dict],
    artifacts: list[dict],
    source_log: str,
) -> str:
    """A recovery artifact belongs to the workflow commit, not the release commit.

    Check the successful validation job's recorded release source as well as the
    artifact's workflow provenance. This also supports the first recovery run,
    whose archive predates the ability to reuse ARM64 artifacts.
    """
    validate_inputs(release_tag, source_run_id)
    validate_inputs(release_tag, recovery_run_id)
    expected = {
        "id": int(recovery_run_id),
        "event": "workflow_dispatch",
        "path": ".github/workflows/recover-release.yml",
        "head_branch": "main",
        "status": "completed",
    }
    for key, value in expected.items():
        if run.get(key) != value:
            raise ValueError(f"ARM64 recovery run {key} must be {value!r}")
    for name in ("validate-source", "rebuild-arm64"):
        matches = [job for job in jobs if job.get("name") == name]
        if len(matches) != 1 or matches[0].get("conclusion") != "success":
            raise ValueError(f"ARM64 recovery requires a successful {name!r} job")
    record = f"Validated {release_tag} at {source_sha} from run {source_run_id}"
    if not re.search(r"(?m)^\d{4}-\d{2}-\d{2}T\S+Z " + re.escape(record) + r"\r?$", source_log):
        raise ValueError("ARM64 recovery log does not confirm the requested release source")
    matches = [a for a in artifacts if a.get("name") == "recovered-ffi-linux-aarch64"]
    if len(matches) != 1:
        raise ValueError("ARM64 recovery requires exactly one recovered FFI artifact")
    artifact = matches[0]
    source = artifact.get("workflow_run", {})
    if (
        artifact.get("expired") is not False
        or artifact.get("size_in_bytes", 0) <= 0
        or source.get("id") != int(recovery_run_id)
        or source.get("head_sha") != run.get("head_sha")
        or source.get("head_branch") != "main"
        or not re.fullmatch(r"[0-9a-f]{40}", run.get("head_sha", ""))
        or type(artifact.get("id")) is not int
        or artifact["id"] <= 0
    ):
        raise ValueError("ARM64 recovery artifact is expired, empty, or from another run")
    return str(artifact["id"])


def github_items(endpoint: str, key: str) -> list[dict]:
    items = []
    page = 1
    separator = "&" if "?" in endpoint else "?"
    while True:
        batch = github_api(f"{endpoint}{separator}per_page=100&page={page}")[key]
        items.extend(batch)
        if len(batch) < 100:
            return items
        page += 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release_tag")
    parser.add_argument("source_run_id")
    parser.add_argument("--arm64-run-id", default="")
    args = parser.parse_args()
    validate_inputs(args.release_tag, args.source_run_id)
    if args.arm64_run_id:
        validate_inputs(args.release_tag, args.arm64_run_id)
    repo = os.environ["GITHUB_REPOSITORY"]
    endpoint = f"repos/{repo}/actions/runs/{args.source_run_id}"
    result = validate_source(
        args.release_tag,
        args.source_run_id,
        github_api(f"repos/{repo}/commits/{args.release_tag}")["sha"],
        github_api(endpoint),
        github_items(f"{endpoint}/jobs?filter=latest", "jobs"),
        github_items(f"{endpoint}/artifacts", "artifacts"),
    )
    result["arm64_artifact_id"] = ""
    if args.arm64_run_id:
        recovery_endpoint = f"repos/{repo}/actions/runs/{args.arm64_run_id}"
        recovery_jobs = github_items(f"{recovery_endpoint}/jobs?filter=latest", "jobs")
        source_jobs = [job for job in recovery_jobs if job.get("name") == "validate-source"]
        if len(source_jobs) != 1 or source_jobs[0].get("conclusion") != "success":
            raise ValueError("ARM64 recovery source validation did not succeed")
        source_log = subprocess.check_output(
            ["gh", "api", f"repos/{repo}/actions/jobs/{source_jobs[0]['id']}/logs"], text=True
        )
        result["arm64_artifact_id"] = validate_recovery_build(
            args.release_tag, args.source_run_id, result["source_sha"], args.arm64_run_id,
            github_api(recovery_endpoint), recovery_jobs,
            github_items(f"{recovery_endpoint}/artifacts", "artifacts"), source_log,
        )
    with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
        for key, value in result.items():
            output.write(f"{key}={value}\n")
    print(f"Validated {args.release_tag} at {result['source_sha']} from run {args.source_run_id}")


if __name__ == "__main__":
    main()
