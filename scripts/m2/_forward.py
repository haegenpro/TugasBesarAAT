"""Shared forwarding for the per-problem M2 entry points (scripts/m2/<problem>-<topic>.py).

Each entry point names one M2 criterion and runs the existing tool in scripts/
with the right subcommand and defaults. The tools themselves stay where they
are: their paths are bound in maintenance receipts and imported by each other.
"""
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent


def given(args, flag):
    return any(a == flag or a.startswith(flag + "=") for a in args)


def build(script, fixed=(), *, actions=(), default_action=None, defaults=(), argv=None):
    """Command line for the forwarded tool: [action] [fixed] [defaults not overridden] [user args]."""
    args = list(sys.argv[1:] if argv is None else argv)
    prefix = []
    if actions:
        prefix = [args.pop(0)] if args and args[0] in actions else [default_action]
    extra = []
    for flag, value in defaults:
        if not given(args, flag):
            extra += [flag, value]
    return [sys.executable, str(SCRIPTS / script), *prefix, *fixed, *extra, *args]


def forward(script, fixed=(), *, actions=(), default_action=None, defaults=(), required=(), doc=None):
    args = sys.argv[1:]
    if args[:1] in (["-h"], ["--help"]):
        print((doc or "").strip() + "\n\n--- forwarded tool: scripts/" + script + " ---\n", flush=True)
        prefix = [default_action] if actions else []
        return sys.exit(subprocess.run([sys.executable, str(SCRIPTS / script), *prefix, *fixed, "--help"]).returncode)
    missing = [flag for flag in required if not given(args, flag)]
    if missing:
        print("missing required option(s): " + ", ".join(missing) + "  (see --help)", file=sys.stderr)
        return sys.exit(2)
    sys.exit(subprocess.run(build(script, fixed, actions=actions, default_action=default_action, defaults=defaults, argv=args)).returncode)
