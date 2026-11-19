"""Configure the laboratory from a local JSON file and submit jobs to its coordinator."""
import argparse
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
from pathlib import Path, PurePosixPath, PureWindowsPath

from agent import APIError, Coordinator as Client
from coordinator import Problem, identifier, integer, job_config, parse_range


ROOT = Path(__file__).resolve().parent
JOB_KEYS = {"name","range","threads","chunk_size","global_rate_limit_rps","check_rps","download_rps",
            "request_timeout_seconds","max_retries","min_free_space_gb","max_seconds","max_items",
            "stop_on_consecutive_missing"}
WORKER_SETTINGS = {"threads","worker_rps","check_rps","download_rps","min_free_space_gb"}
SSH_KEYS = {"host","ssh_user","ssh_port","ssh_key","ssh_auth"}


def object_keys(obj, allowed, section):
    if not isinstance(obj, dict):
        raise ValueError(f"{section}: ожидается JSON-объект")
    extra = obj.keys() - allowed
    if extra:
        raise ValueError(f"{section}: неизвестные поля {', '.join(sorted(extra))}")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Повторяющееся поле {key}")
        result[key] = value
    return result


def absolute_path(value):
    if not isinstance(value, str) or not value or any(ord(c)<32 for c in value):
        raise ValueError("Требуется абсолютный путь")
    path = PurePosixPath(value) if value.startswith("/") else PureWindowsPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("Требуется абсолютный путь без переходов в родительский каталог")
    return str(path)


def parse_ssh(server):
    if not SSH_KEYS.intersection(server):
        return None
    host, user = server.get("host"), server.get("ssh_user")
    if not isinstance(host,str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.:-]{0,252}",host):
        raise ValueError("Некорректный SSH-адрес")
    if not isinstance(user,str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]{0,63}",user):
        raise ValueError("Некорректный SSH-пользователь")
    port = integer(server.get("ssh_port",22), "ssh_port", 1, 65535)
    inventory = {"host":host,"user":user,"port":port}
    auth = server.get("ssh_auth", "key" if "ssh_key" in server else "agent")
    if auth == "key":
        inventory["identity_file"] = absolute_path(server.get("ssh_key"))
    elif auth != "agent" or "ssh_key" in server:
        raise ValueError("ssh_auth должен соответствовать ssh_key: key или agent")
    return inventory


def parse_config(data):
    object_keys(data, {"job","servers"}, "конфигурация")
    job = data.get("job",{})
    object_keys(job, JOB_KEYS, "job")
    job = dict(job)
    if "global_rate_limit_rps" in job:
        job["rate_limit"] = job.pop("global_rate_limit_rps")
    for cap in ("check_rps","download_rps"):
        if job.get(cap) is not None:
            integer(job[cap],cap,1,10000)
    if "name" in job and (not isinstance(job["name"],str) or not 1<=len(job["name"])<=120 or
                          any(ord(c)<32 for c in job["name"])):
        raise ValueError("Некорректное имя задания")
    job_config(job)
    parse_range(job.get("range",""))
    servers = data.get("servers",[])
    if not isinstance(servers,list):
        raise ValueError("servers: ожидается массив")
    nodes = {}
    for s in servers:
        object_keys(s, SSH_KEYS | WORKER_SETTINGS | {"name","storage_path"}, "servers")
        wid = identifier(s.get("name"))
        if wid in nodes:
            raise ValueError(f"Узел {wid} указан дважды")
        opts = {k:s[k] for k in WORKER_SETTINGS if k in s}
        for key in ("worker_rps","check_rps","download_rps"):
            if opts.get(key) is not None:
                integer(opts[key], key, 1, 10000)
        if opts.get("threads") is not None:
            integer(opts["threads"],"threads",1,512)
        if opts.get("min_free_space_gb") is not None:
            space = opts["min_free_space_gb"]
            if type(space) not in (int,float) or not math.isfinite(space) or not 0<=space<=10**9:
                raise ValueError("Некорректный резерв диска")
        nodes[wid] = {"storage_path":absolute_path(s.get("storage_path")),
                      "ssh":parse_ssh(s),"settings":opts}
    return job, nodes


def job_payload(job, nodes, start=None, end=None):
    body = dict(job)
    if start is not None or end is not None:
        body["range"] = {"from":0 if start is None else start, "to":end}
    parse_range(body.setdefault("range",""))
    if nodes:
        body["worker_ids"] = list(nodes)
        body["worker_settings"] = {k:v["settings"] for k,v in nodes.items() if v["settings"]}
    return body


def run_coordinator(args, nodes):
    env = os.environ.copy()
    inventory = {k:v["ssh"] for k,v in nodes.items() if v["ssh"]}
    # Keep the inventory on the coordinator, including when the API runs on a different host.
    with tempfile.TemporaryDirectory(prefix="video-lab-inventory-") as tmp:
        inv_path=Path(tmp)/"ssh.json"
        inv_path.write_text(json.dumps(inventory),encoding="utf-8")
        env["LAB_SSH_INVENTORY"]=str(inv_path)
        env["LAB_STORAGE_ROOTS"]=json.dumps({k:v["storage_path"] for k,v in nodes.items()})
        arguments=[sys.executable,str(ROOT/"coordinator.py"),"--host",args.host,"--port",str(args.port),
                   "--db",args.db,"--heartbeat-seconds",str(args.heartbeat_seconds),"--lease-seconds",str(args.lease_seconds)]
        if args.cert or args.key:
            if not (args.cert and args.key):
                raise ValueError("Для TLS укажите и --cert, и --key")
            arguments.extend(["--cert",args.cert,"--key",args.key])
        return subprocess.call(arguments,env=env)


def run_agent(args, nodes):
    if args.id not in nodes:
        raise ValueError("Узел отсутствует в конфигурации")
    server=nodes[args.id]
    local_root=server["storage_path"]
    if not Path(local_root).is_absolute():
        raise ValueError("Каталог агента должен быть абсолютным путём для системы узла")
    # Job and worker RPS are enforced by permits at the coordinator, including live edits.
    return subprocess.call([sys.executable,str(ROOT/"agent.py"),"--id",args.id,"--source",args.source,
                            "--coordinator",args.coordinator,"--storage",local_root,"--threads",str(args.capacity),
                            "--max-connections",str(args.capacity),"--heartbeat",str(args.heartbeat)])


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest="command",required=True)
    submit=commands.add_parser("submit",help="Создать задание и запустить его")
    submit.add_argument("--config",type=Path)
    submit.add_argument("--from",dest="first",type=int)
    submit.add_argument("--to",dest="last",type=int)
    submit.add_argument("--create-only",action="store_true")
    submit.add_argument("--coordinator",default="http://127.0.0.1:8090")
    service=commands.add_parser("coordinator",help="Запустить координатор с доверенным инвентарём")
    service.add_argument("--config",type=Path,required=True)
    service.add_argument("--host",default="127.0.0.1")
    service.add_argument("--port",type=int,default=8090)
    service.add_argument("--db",default=".lab/lab.sqlite")
    service.add_argument("--cert")
    service.add_argument("--key")
    service.add_argument("--heartbeat-seconds",type=float,default=10)
    service.add_argument("--lease-seconds",type=float,default=30)
    worker=commands.add_parser("agent",help="Запустить агент выбранного узла на его машине")
    worker.add_argument("--config",type=Path,required=True)
    worker.add_argument("--id",required=True)
    worker.add_argument("--coordinator",default="http://127.0.0.1:8090")
    worker.add_argument("--source",default="http://127.0.0.1:8091")
    worker.add_argument("--capacity",type=int,default=512,help="Предельная параллельность узла")
    worker.add_argument("--heartbeat",type=float,default=2)
    args=parser.parse_args(argv)
    if sys.stdout is not None and hasattr(sys.stdout, "reconfigure") and not sys.stdout.isatty():
        sys.stdout.reconfigure(encoding="utf-8")
    try:
        data=json.loads(args.config.read_text(encoding="utf-8-sig"),object_pairs_hook=unique_object) if args.config else {}
        job,nodes=parse_config(data)
        if args.command=="coordinator":
            return run_coordinator(args,nodes)
        if args.command=="agent":
            return run_agent(args,nodes)
        body=job_payload(job,nodes,args.first,args.last)
        token=os.environ.get("LAB_OPERATOR_TOKEN") or os.environ.get("LAB_ADMIN_TOKEN")
        if not token:
            raise ValueError("Задайте LAB_OPERATOR_TOKEN или LAB_ADMIN_TOKEN")
        client=Client(args.coordinator,token)
        result=client.post("jobs",body)
        print(f"Создано задание {result['id']}",flush=True)
        if not args.create_only:
            result=client.post(f"jobs/{result['id']}/start",{})
        print(json.dumps({"id":result["id"],"status":result["status"]},ensure_ascii=False))
        return 1 if result["status"] == "failed" else 0
    except (ValueError,Problem,OSError,APIError,urllib.error.URLError) as err:
        parser.exit(2,f"Ошибка: {err}\n")


if __name__=="__main__":
    raise SystemExit(main())
