import argparse
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import jobctl
from coordinator import Problem
from verify_lab import Lab, wait


class ConfigTest(unittest.TestCase):
    def configuration(self):
        return json.loads((jobctl.ROOT/"fixtures/job.example.json").read_text(encoding="utf-8"))

    def test_range_flags_replace_config_inclusively(self):
        job,nodes=jobctl.parse_config(self.configuration())
        self.assertEqual(jobctl.job_payload(job,nodes)["range"],{"from":0,"to":500000})
        for start,end,expected in [(0,0,(0,0)),(13,None,(13,None)),(None,27,(0,27))]:
            result=jobctl.job_payload(job,nodes,start,end)
            self.assertEqual(jobctl.parse_range(result["range"]),expected)
        for start,end in [(-1,2),(10,2)]:
            with self.assertRaises(Problem):
                jobctl.job_payload(job,nodes,start,end)

    def test_server_configuration_remains_local(self):
        job,nodes=jobctl.parse_config(self.configuration())
        payload=jobctl.job_payload(job,nodes)
        self.assertEqual(payload["rate_limit"],20)
        self.assertEqual(payload["worker_settings"]["worker-02"]["min_free_space_gb"],50)
        self.assertEqual(payload["worker_settings"]["worker-03"]["check_rps"],6)
        for private in ["ssh_key","192.168.","/run/secrets/","/data/videos"]:
            self.assertNotIn(private,json.dumps(payload))
        with patch("jobctl.subprocess.call") as run:
            def consume(args,env):
                self.assertEqual(json.loads(env["LAB_STORAGE_ROOTS"])["worker-01"],"/data/videos")
                inventory=json.loads(Path(env["LAB_SSH_INVENTORY"]).read_text())
                self.assertEqual(inventory["worker-01"]["identity_file"],"/run/secrets/worker-01-key")
                self.assertEqual(inventory["worker-03"]["port"],22)
                return 0
            run.side_effect=consume
            self.assertEqual(jobctl.run_coordinator(argparse.Namespace(host="127.0.0.1",port=8090,db="lab.db",
                cert=None,key=None,heartbeat_seconds=2,lease_seconds=10),nodes),0)

    def test_unknown_duplicate_and_unsafe_config_rejected(self):
        variants=[{"job":{"extra":1}},{"servers":[{"name":"w","storage_path":"../outside"}]},
                  {"servers":[{"name":"w","storage_path":"/a","ssh_user":"u","ssh_key":"/key"}]},
                  {"job":{"check_rps":False}},{"servers":[{"name":"a","storage_path":"/s","worker_rps":0}]}]
        for data in variants:
            with self.subTest(data=data), self.assertRaises((ValueError,Problem)):
                jobctl.parse_config(data)
        with self.assertRaises(ValueError):
            json.loads('{"job":{},"job":{}}',object_pairs_hook=jobctl.unique_object)
        data=self.configuration()
        data["servers"].append(data["servers"][0])
        with self.assertRaises(ValueError):
            jobctl.parse_config(data)

    def test_agent_start_uses_own_storage(self):
        with tempfile.TemporaryDirectory() as root:
            _,nodes=jobctl.parse_config({"servers":[{"name":"w","storage_path":root}]})
            args=argparse.Namespace(id="w",source="http://mock:8091",coordinator="https://c",capacity=16,heartbeat=2)
            with patch("jobctl.subprocess.call",return_value=0) as launch:
                self.assertEqual(jobctl.run_agent(args,nodes),0)
                cmd=launch.call_args.args[0]
                self.assertEqual(cmd[cmd.index("--storage")+1],root)
                self.assertEqual(cmd[cmd.index("--max-connections")+1],"16")

    def test_create_and_start(self):
        with patch.dict(os.environ,{"LAB_OPERATOR_TOKEN":"local-test"}), patch("jobctl.Client") as cls, redirect_stdout(io.StringIO()):
            client=cls.return_value
            client.post.side_effect=[{"id":7,"status":"created"},{"id":7,"status":"running"}]
            self.assertEqual(jobctl.main(["submit","--from","3","--to","4"]),0)
            self.assertEqual(client.post.call_args_list[0].args,("jobs",{"range":{"from":3,"to":4}}))
            self.assertEqual(client.post.call_args_list[1].args,("jobs/7/start",{}))
            client.reset_mock()
            client.post.side_effect=[{"id":8,"status":"created"}]
            jobctl.main(["submit","--create-only"])
            client.post.assert_called_once_with("jobs",{"range":""})

    def test_cli_submits_config_to_a_running_coordinator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lab = Lab(root, ["worker-1"])
            try:
                lab.setup({}, 2, 30)
                config = root / "job.json"
                config.write_text(json.dumps({"job":{"range":"[0,100]", "check_rps":9},
                    "servers":[{"name":"worker-1", "storage_path":str(root/"worker-1"), "worker_rps":7}]}))
                result = subprocess.run([sys.executable, str(jobctl.ROOT/"jobctl.py"), "submit",
                    "--config", str(config), "--from", "7", "--to", "8", "--coordinator", lab.api[:-4]],
                    env={**os.environ, "LAB_OPERATOR_TOKEN":lab.keys["operator"]},
                    stdout=subprocess.PIPE, encoding="utf-8", check=True, timeout=20)
                path = "/jobs/" + str(json.loads(result.stdout.splitlines()[-1])["id"])
                job = wait("CLI job completion", lambda:lab.completed(path))
                self.assertEqual((job["range_start"], job["range_end"], job["processed"]),(7,8,2))
                self.assertEqual(job["worker_settings"]["worker-1"]["worker_rps"],7)
                self.assertEqual(job["check_rps"],9)
            finally:
                lab.close()
