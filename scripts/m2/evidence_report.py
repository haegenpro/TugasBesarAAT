"""Shared output helpers for the observation tools: aligned tables and an attachable result directory.

Every observation tool prints its tables and also saves them, unchanged, as
result.txt next to a raw result.json in a private directory so they can be
attached to the report. Secrets passed to Report are redacted from both.
"""
import datetime
import json
import os
from pathlib import Path

from common import LOCAL, private_json


def new_output_dir(tool, output=None):
    """A new private directory for one run (never reuses or overwrites an existing one)."""
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    path = Path(output) if output else LOCAL/f'evidence-{tool}-{stamp}'
    path.mkdir(mode=0o700, parents=True, exist_ok=False)
    return path


def format_table(headers, rows):
    """Left-aligned text table; every cell is rendered with str()."""
    cells = [[str(cell) for cell in row] for row in rows]
    widths = [len(str(h)) for h in headers]
    for row in cells:
        for index, cell in enumerate(row):
            widths[index] = max(widths[index], len(cell))
    def render(row):
        return '  '.join(cell.ljust(widths[index]) for index, cell in enumerate(row)).rstrip()
    return [render([str(h) for h in headers]), '  '.join('-'*w for w in widths), *[render(row) for row in cells]]


class Report:
    """Collects printed lines and raw data for result.txt and result.json."""

    def __init__(self, secrets=()):
        self.lines = []
        self.data = {}
        self._secrets = [s for s in secrets if s]

    def _clean(self, text):
        for secret in self._secrets:
            text = text.replace(secret, '***')
        return text

    def line(self, text=''):
        text = self._clean(str(text))
        print(text, flush=True)
        self.lines.append(text)

    def table(self, headers, rows):
        for text in format_table(headers, rows):
            self.line(text)

    def save(self, directory):
        directory = Path(directory)
        text_path = directory/'result.txt'
        text_path.write_text('\n'.join(self.lines)+'\n')
        os.chmod(text_path, 0o600)
        cleaned = self._clean(json.dumps(self.data, default=str))
        private_json(directory/'result.json', json.loads(cleaned))
        self.line(f'Saved result.txt and result.json in {directory}')
