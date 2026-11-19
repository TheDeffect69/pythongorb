"""Stage an approved agent release; activate it locally through the fixed systemd unit."""

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path


SERVICE = "lab-worker.service"


def select_link(base, name, release):
    with tempfile.TemporaryDirectory(prefix=".select-", dir=base) as tmp:
        link = Path(tmp) / "link"
        link.symlink_to(release, target_is_directory=True)
        os.replace(link, base / name)


def stage(base, source, version, expected_hash):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", version):
        raise ValueError("Invalid release name")
    if not re.fullmatch(r"[0-9a-fA-F]{64}", expected_hash):
        raise ValueError("Expected a SHA-256 digest")
    releases = base / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    target = releases / version
    if target.exists():
        raise ValueError("A release with this version already exists")
    with tempfile.TemporaryDirectory(prefix=".stage-", dir=releases) as temp:
        candidate = Path(temp) / version
        candidate.mkdir(mode=0o755)
        script = candidate / "agent.py"
        shutil.copyfile(source, script)
        script.chmod(0o644)
        with script.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != expected_hash.lower():
                raise ValueError("Release checksum does not match")
        compile(script.read_bytes(), "agent.py", "exec")
        candidate.rename(target)
    select_link(base, "next", target)


def restart():
    subprocess.run(["systemctl", "restart", SERVICE], check=True, timeout=30)
    # A Python startup error may arrive just after systemd accepted the request.
    time.sleep(1)
    subprocess.run(["systemctl", "is-active", "--quiet", SERVICE], check=True, timeout=10)


def activate(base, restart_service=restart):
    releases = (base / "releases").resolve(strict=True)
    candidate = (base / "next").resolve(strict=True)
    if candidate.parent != releases or not (candidate / "agent.py").is_file():
        raise ValueError("No valid staged release")
    current = base / "current"
    previous = current.resolve(strict=True) if current.is_symlink() else None
    select_link(base, "current", candidate)
    try:
        restart_service()
    except (OSError, subprocess.SubprocessError):
        if previous:
            select_link(base, "current", previous)
            restart_service()
        else:
            current.unlink()
        raise


if __name__ == "__main__":
    # The release tree and this tool must be writable only by the release administrator.
    import fcntl

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/video-lab"))
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("stage")
    prepare.add_argument("source", type=Path)
    prepare.add_argument("version")
    prepare.add_argument("sha256")
    commands.add_parser("activate")
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".release.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.command == "stage":
            stage(root, args.source, args.version, args.sha256)
        else:
            activate(root)
