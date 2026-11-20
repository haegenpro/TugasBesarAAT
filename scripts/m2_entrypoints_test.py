"""Per-problem M2 entry points (scripts/m2) forward to real tools with valid subcommands."""
import ast
import re
import runpy
import subprocess
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parent
M2 = SCRIPTS / "m2"
sys.path.insert(0, str(M2))
import _forward  # noqa: E402


def entry_points():
    return sorted(p for p in M2.glob("*.py") if not p.name.startswith("_"))


def forwarded(path, argv):
    """Run an entry point with argv; return the command it would execute and its exit code."""
    calls = []
    def run(command, *args, **kwargs):
        calls.append(command);return subprocess.CompletedProcess(command, 0)
    with patch.object(sys, "argv", [str(path), *argv]), patch.object(_forward.subprocess, "run", side_effect=run):
        try:
            runpy.run_path(str(path), run_name="__main__")
            code = None
        except SystemExit as exc:
            code = exc.code
    return (calls[0] if calls else None), code


class EntryPointChecks(unittest.TestCase):
    def test_every_entry_point_is_documented_named_by_problem_and_targets_a_real_tool(self):
        readme = (M2 / "README.md").read_text(encoding="utf-8")
        demo_checks = re.search(r'p\.add_argument\("check", choices=\[([^\]]+)\]', (SCRIPTS / "platform-demo-check.py").read_text()).group(1)
        self.assertEqual(len(entry_points()), 21)
        for path in entry_points():
            with self.subTest(path.name):
                self.assertRegex(path.stem, r"^(p[1-5]|bonus)-[a-z0-9-]+$")
                self.assertIn("`" + path.name + "`", readme)
                doc = ast.get_docstring(ast.parse(path.read_text(encoding="utf-8")))
                self.assertRegex(doc, r"^(P[1-5]\.\d|Bonus)")
                command, code = forwarded(path, ["--inventory", "inv.json"] if "required" not in path.read_text() else [])
                if command is None:
                    self.assertEqual(code, 2)  # a required option was missing; nothing ran
                    continue
                self.assertEqual(code, 0)
                tool = Path(command[1])
                self.assertTrue(tool.is_file(), tool)
                self.assertEqual(tool.parent, SCRIPTS)
                if tool.name == "platform-demo-check.py":
                    self.assertIn('"' + command[2] + '"', demo_checks)

    def test_subcommands_defaults_and_overrides(self):
        command, _ = forwarded(M2 / "p2-load-scale.py", ["--inventory", "inv.json"])
        self.assertEqual(command[2:], ["run", "--scenario", "scale", "--inventory", "inv.json"])
        command, _ = forwarded(M2 / "p2-load-scale.py", ["plan", "--scenario", "combined"])
        self.assertEqual(command[2:], ["plan", "--scenario", "combined"])  # user values win; no duplicate flag
        command, _ = forwarded(M2 / "p2-takeover.py", ["--scope", "acquisition:BMKG"])
        self.assertEqual(command[2:], ["takeover", "--scope", "acquisition:BMKG"])
        command, _ = forwarded(M2 / "p5-game-day.py", ["--scenario", "x", "--", "key"])
        self.assertEqual(command[2:], ["tester", "--mode", "exam", "--scenario", "x", "--", "key"])
        command, _ = forwarded(M2 / "p1-replace-node.py", ["status", "--node", "n"])
        self.assertEqual(command[2:], ["status", "--node", "n"])

    def test_required_options_are_enforced_before_anything_runs(self):
        for name, flag in (("p2-freeze-node.py", "--freeze-node"), ("p2-clock-shift.py", "--since"), ("p3-replay.py", "--namespace-id"), ("p1-node-loss.py", "--scenario")):
            command, code = forwarded(M2 / name, ["--inventory", "inv.json"])
            self.assertIsNone(command);self.assertEqual(code, 2)
            command, code = forwarded(M2 / name, [flag + "=value", "--inventory", "inv.json"])
            self.assertIsNotNone(command);self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
