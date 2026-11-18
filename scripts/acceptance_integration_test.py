"""Measured game-day gates, stable baselines and non-vacuous generator evidence."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import threading
from unittest.mock import patch

from acceptance_lib import compare,game_module,load_measurements
from generator_evidence_lib import Sampler,health


class AcceptanceChecks(unittest.TestCase):
    def fixture(self,folder,replicas=3,rps=100):
        folder.mkdir()
        report={"run_id":"run","status":"needs-owner-reconciliation","load_exit_code":0,"worktree_dirty":False,"scenario":"baseline","dataset_sha256":"dataset","script_sha256":"script","git_revision":"revision","original_deployment":{"name":"aggregator","replicas":replicas,"images":["image"]},"events":[],"generator_health":{"declared_network_capacity_mbps":1000}}
        work={"configuration":{"run_id":"run","vus":10000,"duration_seconds":150,"field_vus":500,"think_time_seconds":.05,"workload":"reads"},"metrics":{"m2_unexpected_failure_rate":{"values":{"rate":0}},"m2_successful_reads":{"values":{"count":rps*150}},"m2_successful_reads{role:field}":{"values":{"count":1}},"m2_successful_reads{role:media}":{"values":{"count":1}}}}
        deployment={"metadata":{"name":"aggregator","uid":"original","generation":1},"spec_replicas":replicas,"spec_sha256":"different-by-replicas","template_sha256":"stable-template","images":["image"],"status":{"readyReplicas":replicas,"observedGeneration":1}}
        samples=[{"monotonic":0,"cpu_total":100,"cpu_idle":90,"available_memory_mib":1000,"network_bytes":100},{"monotonic":1,"cpu_total":200,"cpu_idle":180,"available_memory_mib":1000,"network_bytes":200}]
        values={"result.json":report,"load.json":work,"sample-0-deployments.json":{"items":[deployment]},"before-deployments.json":{"items":[deployment]},"before-statefulsets.json":{"items":[]},"generator-samples.json":samples,"dataset-before.json":{"generation":1,"pending":0}}
        for name,value in values.items():(folder/name).write_text(json.dumps(value))
        return values

    def test_numeric_comparison_also_requires_stable_dataset_templates_and_generator_headroom(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary);self.fixture(root/"one",1,100);values=self.fixture(root/"three",3,210)
            self.assertTrue(compare(root/"one",root/"three")["passed"])
            (root/"three/dataset-before.json").write_text('{"generation":2,"pending":0}')
            with self.assertRaisesRegex(ValueError,"canonical state changed"):compare(root/"one",root/"three")
            (root/"three/dataset-before.json").write_text('{"generation":1,"pending":0}')
            values["before-deployments.json"]["items"][0]["template_sha256"]="changed-environment"
            (root/"three/before-deployments.json").write_text(json.dumps(values["before-deployments.json"]))
            with self.assertRaisesRegex(ValueError,"upstream"):compare(root/"one",root/"three")

    def test_combined_actions_and_ready_replicas_must_be_observed_during_load(self):
        schedule=game_module().schedule("combined",150)
        self.assertEqual([a for _,a,_ in schedule],["scale","rollout","pause","resume","scale"])
        with tempfile.TemporaryDirectory()as temporary:
            folder=Path(temporary)/"run";values=self.fixture(folder)
            values["result.json"].update(scenario="combined",requested_rollout_image="new-image",events=[{"action":a}for _,a,_ in schedule]);(folder/"result.json").write_text(json.dumps(values["result.json"]))
            with self.assertRaisesRegex(ValueError,"Ready"):load_measurements(folder,"combined")
            deployments=[]
            for replicas in(5,2):
                entry=copy.deepcopy(values["sample-0-deployments.json"]["items"][0]);entry["spec_replicas"]=replicas;entry["status"]["readyReplicas"]=replicas;deployments.append(entry)
            (folder/"sample-0-deployments.json").write_text(json.dumps({"items":deployments}))
            with self.assertRaisesRegex(ValueError,"new rollout image"):load_measurements(folder,"combined")
            (folder/"sample-0-pods.json").write_text(json.dumps({"items":[{"metadata":{"labels":{"app.kubernetes.io/name":"aggregator"}},"images":["new-image"],"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}))
            self.assertTrue(load_measurements(folder,"combined")[0]["passed"])

    def test_generator_rejects_missing_link_budget_saturation_and_counter_reset(self):
        samples=[{"monotonic":0,"cpu_total":100,"cpu_idle":0,"available_memory_mib":1000,"network_bytes":100},{"monotonic":1,"cpu_total":200,"cpu_idle":1,"available_memory_mib":1000,"network_bytes":200}]
        self.assertFalse(health(samples,1000)["passed"])
        samples[1]["cpu_idle"]=90;self.assertTrue(health(samples,1000)["passed"])
        self.assertFalse(health(samples)["passed"])
        samples[1]["network_bytes"]=0;self.assertFalse(health(samples,1000)["passed"])

    def test_generator_sampling_runs_independently_of_slow_scenario_actions(self):
        with tempfile.TemporaryDirectory()as folder:
            sampled=threading.Event();count=[0]
            def counter():
                count[0]+=1
                if count[0]>=3:sampled.set()
                return {"sample":count[0]}
            sampler=Sampler(Path(folder)/"samples.json",interval=.005)
            with patch("generator_evidence_lib.counters",side_effect=counter):
                sampler.start()
                self.assertTrue(sampled.wait(timeout=1));sampler.stop()
            self.assertGreaterEqual(len(sampler.samples),3);self.assertEqual(sampler.errors,[])


if __name__=="__main__":unittest.main()
