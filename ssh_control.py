"""Run only fixed service actions on nodes in the administrator's inventory."""

import json
import os
import re
import subprocess
from pathlib import Path


COMMANDS = {
    "status": ("systemctl", "is-active", "lab-worker.service"),
    "start": ("systemctl", "start", "lab-worker.service"),
    "restart": ("systemctl", "restart", "lab-worker.service"),
    "update": ("systemctl", "start", "lab-worker-update.service"),
}


def run_service_action(worker_id, action):
    inventory = os.getenv("LAB_SSH_INVENTORY")
    if action not in COMMANDS or not inventory:
        raise ValueError("Операция SSH недоступна")
    server = json.loads(Path(inventory).read_text(encoding="utf-8")).get(worker_id)
    if not isinstance(server, dict):
        raise ValueError("Узел отсутствует в SSH-инвентаре")
    host = server.get("host", "")
    user = server.get("user", "")
    port = server.get("port", 22)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,252}", host):
        raise ValueError("Некорректный адрес в инвентаре")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,63}", user):
        raise ValueError("Некорректный пользователь в инвентаре")
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("Некорректный порт в инвентаре")
    command = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
               "-o", "ClearAllForwardings=yes", "-o", "ConnectTimeout=5", "-p", str(port)]
    if server.get("identity_file"):
        identity = Path(server["identity_file"])
        if not identity.is_absolute() or not identity.is_file():
            raise ValueError("Недоступен файл ключа из менеджера секретов")
        command.extend(["-o", "IdentitiesOnly=yes", "-i", str(identity)])
    try:
        process = subprocess.run([*command, f"{user}@{host}", *COMMANDS[action]],
                                 capture_output=True, timeout=110 if action == "update" else 15,
                                 check=False, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        raise RuntimeError("Истекло время ожидания SSH") from None
    if process.returncode:
        raise RuntimeError("Удалённая операция не завершена")
