"""Check certificate trust, HTTPS agent operations and temporary SQLite write unavailability."""

import argparse
import json
from pathlib import Path
import shutil
import sqlite3
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from verify_lab import Lab, ensure, wait


def certificates(folder, executable):
    def openssl(*args):
        subprocess.run([executable, *args], cwd=folder, capture_output=True, check=True)

    key_type = ["-newkey","ec","-pkeyopt","ec_paramgen_curve:P-256","-nodes"]
    openssl("req","-new","-x509",*key_type,"-days","1","-subj","/CN=Temporary Lab CA",
            "-addext","basicConstraints=critical,CA:TRUE",
            "-addext","keyUsage=critical,keyCertSign,cRLSign",
            "-addext","subjectKeyIdentifier=hash","-keyout","ca.key","-out","ca.crt")
    openssl("req","-new",*key_type,"-subj","/CN=localhost","-keyout","server.key","-out","server.csr")
    (folder/"extensions").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
            "extendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n"
            "authorityKeyIdentifier=keyid\nsubjectKeyIdentifier=hash\n",encoding="ascii")
    openssl("x509","-req","-in","server.csr","-CA","ca.crt","-CAkey","ca.key","-set_serial","2",
            "-days","1","-extfile","extensions","-out","server.crt")


def run(folder, executable):
    certificates(folder, executable)
    lab = Lab(folder, ["tls-worker"])
    lab.capture_logs = True
    lab.api = lab.api.replace("http:", "https:", 1)
    lab.coordinator_options = ["--cert",folder/"server.crt","--key",folder/"server.key"]
    lab.worker_env = {"SSL_CERT_FILE":str(folder/"ca.crt")}
    ca = ssl.create_default_context(cafile=str(folder/"ca.crt"))
    lab.http = urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPSHandler(context=ca))
    try:
        lab.setup({"delay_ms":130},1,30)
        # Use an empty trust store to test rejection on any host, regardless of installed roots.
        untrusted = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        untrusted_http = urllib.request.build_opener(urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=untrusted))
        try:
            with untrusted_http.open(lab.api+"/me", timeout=5):
                raise AssertionError("Untrusted certificate accepted")
        except urllib.error.URLError as error:
            ensure(isinstance(error.reason,ssl.SSLCertVerificationError),"expected certificate verification error")

        path = lab.job(range="[3000,3015]",threads=1,chunk_size=8,rate_limit=120)
        wait("first confirmed result",lambda:lab.request(path+"/items"))
        blocker = sqlite3.connect(str(folder/"state.db"),timeout=8)
        try:
            blocker.execute("BEGIN IMMEDIATE")
            blocked_at = time.monotonic()
            before = lab.request(path+"/items")
            ensure(0<len(before)<16,"outage must interrupt an active job")
            lab.denied(503,path,"PATCH",{"rate_limit":120})
            during = lab.request(path+"/items")
            ensure(during == before, "committed progress changed during write outage")
            write_outage = time.monotonic() - blocked_at
        finally:
            blocker.rollback()
            blocker.close()
        wait("progress after database recovery",lambda:lab.completed(path))
        rows = lab.request(path+"/items")
        ensure([r["external_id"] for r in rows] == list(range(3000,3016)), "missing or duplicated results")
        ensure(all(r["worker_id"]=="tls-worker" and r["status"]=="completed" for r in rows), "HTTPS worker results")
        ensure(all(r in rows for r in before),"lost previously confirmed results")
        audit = lab.request("/audit",role="admin")
        events = lab.request("/events")
        ensure(audit and events,"audit and events should be populated")
        inspected = json.dumps([events,audit,rows,lab.request("/workers")]).encode()
    finally:
        lab.close()
    inspected += b"".join(file.read_bytes() for file in folder.glob("*.log"))
    ensure(all(token.encode() not in inspected for token in lab.keys.values()),"secret in logs or API")
    return {"untrusted_ca_rejected":True,"https_worker":"tls-worker","write_outage_seconds":round(write_outage,2),
            "busy_write_http_status":503,
            "confirmed_before":len(before),"confirmed_after":len(rows),"secrets_found":False}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--openssl",default=shutil.which("openssl"))
    args = parser.parse_args()
    openssl = args.openssl or "C:/Program Files/Git/usr/bin/openssl.exe"
    if not Path(openssl).is_file() and not shutil.which(openssl):
        parser.error("OpenSSL is required; supply --openssl with the executable path")
    with tempfile.TemporaryDirectory(prefix="lab-test-tls-") as directory:
        print(json.dumps(run(Path(directory),openssl)))
