"""Start a local, persistent three-worker lab, with independent worker directories."""

import argparse
import json
import os
import secrets
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path


PROJECT = Path(__file__).resolve().parent
WORKERS = ("worker-1", "worker-2", "worker-3")


def credentials(directory):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = directory / "credentials.json"
    if not path.exists():
        tokens = {name: secrets.token_urlsafe(32) for name in ("admin", "operator", "viewer", *WORKERS)}
        with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "w") as out:
            json.dump(tokens, out, indent=2)
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    if hasattr(sys.stdout, "reconfigure") and not sys.stdout.isatty():
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path(".lab"))
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--mock-port", type=int, default=8091)
    sources = parser.add_mutually_exclusive_group()
    sources.add_argument("--catalog", type=Path, help="Manifest of local files with download permissions")
    sources.add_argument("--kinescope-index", type=Path, help="Index of videos in the Kinescope workspace")
    args = parser.parse_args()
    state = args.state.resolve()
    tokens = credentials(state)
    base = f"http://127.0.0.1:{args.port}"
    source = f"http://127.0.0.1:{args.mock_port}"
    children = {}
    launch_specs = {}
    restart_at = {}
    interrupted = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: interrupted.set())
    signal.signal(signal.SIGINT, lambda *_: interrupted.set())
    def start(name, script, arguments, private_env=None):
        launch_specs[name] = script, arguments, private_env
        env = {k: v for k, v in os.environ.items() if not k.startswith("LAB_")}
        env.update(private_env or {})
        children[name] = subprocess.Popen([sys.executable, "-u", str(PROJECT / script), *map(str, arguments)],
                                          env=env, cwd=PROJECT)

    try:
        worker_env = {}
        if args.kinescope_index:
            if not os.getenv("LAB_KINESCOPE_TOKEN") or not args.kinescope_index.is_file():
                parser.error("Kinescope requires an index file and LAB_KINESCOPE_TOKEN")
            source = "kinescope"
            worker_env.update(LAB_KINESCOPE_TOKEN=os.environ["LAB_KINESCOPE_TOKEN"],
                              LAB_KINESCOPE_INDEX=str(args.kinescope_index.resolve()))
        else:
            profile = ["--catalog", args.catalog.resolve()] if args.catalog else ["--config", PROJECT/"fixtures/demo.json"]
            start("mock", "mock_service.py", ["--port", args.mock_port, *profile])
        api_env = {f"LAB_{role.upper()}_TOKEN": tokens[role] for role in ("admin", "operator", "viewer")}
        api_env["LAB_WORKER_TOKENS"] = json.dumps({w: tokens[w] for w in WORKERS})
        start("coordinator", "coordinator.py", ["--port", args.port, "--db", state/"lab.sqlite"], api_env)
        for name, threads in zip(WORKERS, (4, 8, 12)):
            start(name, "agent.py", ["--coordinator", base, "--source", source, "--id", name,
                  "--threads", threads, "--storage", state/name], {**worker_env, "LAB_WORKER_TOKEN": tokens[name]})
        ready_since = time.monotonic()
        print(f"Панель: {base}\nЛокальные ключи ролей: {state/'credentials.json'}\nОстановка стенда: Ctrl+C",
              flush=True)
        while not interrupted.wait(1):
            for name, child in children.items():
                if child.poll() is not None:
                    if name == "mock" or time.monotonic() - ready_since < 2:
                        raise SystemExit(f"{name} остановлен, код {child.returncode}")
                    if name not in restart_at:
                        # Leave enough time for another node to take over the expired lease.
                        delay = 40 if name in WORKERS else 2
                        restart_at[name] = time.monotonic() + delay
                        print(f"{name} отключён. Перезапуск через {delay} с.", flush=True)
                    elif time.monotonic() >= restart_at[name]:
                        start(name, *launch_specs[name])
                        del restart_at[name]
    finally:
        for child in reversed(list(children.values())):
            if child.poll() is None:
                child.terminate()
        for child in children.values():
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    main()
