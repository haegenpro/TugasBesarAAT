"""Deferred placement/probe/credential-boundary checks; no infrastructure calls."""
import copy
import json
from pathlib import Path
import unittest

from aggregator_lib import render

ROOT = Path(__file__).resolve().parent.parent


class AggregatorDeploymentChecks(unittest.TestCase):
    def setUp(self):
        self.inventory = json.loads((ROOT / "deployment/platform/inventory.example.json").read_text())
        self.config = json.loads((ROOT / "deployment/aggregator/config.example.json").read_text())
        self.image = "example.invalid/aggregator@sha256:" + "a" * 64

    def test_replicas_spread_without_preventing_five_on_four_nodes(self):
        for replicas in (2, 3, 5):
            objects = render(self.inventory, self.config, self.image, replicas)["items"]
            deployment = next(o for o in objects if o["kind"] == "Deployment")["spec"]
            self.assertEqual(deployment["replicas"], replicas)
            pod = deployment["template"]["spec"]
            spread = pod["topologySpreadConstraints"][0]
            self.assertEqual(spread["topologyKey"], "kubernetes.io/hostname")
            self.assertEqual(spread["maxSkew"], 1)
            self.assertEqual(spread["minDomains"], 3)
            self.assertNotIn("podAntiAffinity", pod["affinity"], "one-pod-per-node would prevent five replicas")
            container = pod["containers"][0]
            self.assertEqual(container["readinessProbe"]["httpGet"]["path"], "/ready")
            self.assertEqual(container["livenessProbe"]["httpGet"]["path"], "/health")
            self.assertGreater(pod["terminationGracePeriodSeconds"], 35)
            self.assertEqual(deployment["strategy"]["rollingUpdate"]["maxUnavailable"], 0)

    def test_secrets_are_references_and_acquisition_is_enabled(self):
        objects = render(self.inventory, self.config, self.image)["items"]
        public = next(o for o in objects if o["kind"] == "ConfigMap")["data"]
        self.assertEqual(public["ACQUISITION_ENABLED"], "true")
        self.assertNotIn("DATABASE_URL", public)
        self.assertNotIn("BMKG_API_KEY", public)
        pod = next(o for o in objects if o["kind"] == "Deployment")["spec"]["template"]["spec"]
        env = {e["name"]: e for e in pod["containers"][0]["env"]}
        for key in ("DATABASE_URL", "BMKG_API_KEY", "PVMBG_TOKEN", "CANONICAL_SERVICE_TOKEN", "STORAGE_GROUP_0_URL"):
            self.assertIn("secretKeyRef", env[key]["valueFrom"])
        self.assertNotIn("STORAGE_GROUP_1_URL", env)
        self.assertFalse(pod["automountServiceAccountToken"])

    def test_public_overrides_cannot_disable_fencing_or_contain_credentials(self):
        config = copy.deepcopy(self.config)
        config["environment"]["ACQUISITION_ENABLED"] = "false"
        with self.assertRaises(ValueError):
            render(self.inventory, config, self.image)
        config = copy.deepcopy(self.config)
        config["environment"]["NATS_URL"] = "nats://user:password@broker:4222"
        with self.assertRaises(ValueError):
            render(self.inventory, config, self.image)
        with self.assertRaises(ValueError):
            render(self.inventory, self.config, "example.invalid/aggregator:latest")


if __name__ == "__main__":
    unittest.main()
