"""Opt-in Linux test on a new 2 MiB tmpfs in a private user and mount namespace."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def main():
    if sys.platform != "linux":
        raise SystemExit("Requires Linux with unprivileged user namespaces.")
    for command in ("unshare","mount","umount","sh"):
        if not shutil.which(command):
            raise SystemExit(f"Missing {command}")
    mount_and_test = """
        mount -t tmpfs -o size=2M,nr_inodes=128,nosuid,nodev,noexec,mode=0700 tmpfs "$1"
        trap 'umount "$1"' EXIT
        "$2" -m unittest discover -s tests -p test_faults.py -k real_tmpfs_enospc -v
    """
    with tempfile.TemporaryDirectory(prefix="lab-tmpfs-") as directory:
        env = {**os.environ, "LAB_DISPOSABLE_TMPFS":directory}
        subprocess.run(["unshare","--user","--map-root-user","--mount","--propagation","private",
                        "sh","-eu","-c",mount_and_test,"verify_disk",directory,sys.executable],
                       cwd=Path(__file__).resolve().parent, env=env, check=True)


if __name__ == "__main__":
    main()
