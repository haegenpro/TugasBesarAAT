"""Read Prometheus text metrics and small internal endpoints through an owned port-forward.

The forward reconnects once if its pod was replaced, so a probe survives the very
failures it is observing. Credentials are sent but never returned or printed.
"""
from contextlib import ExitStack
import json
import math
import re
import time
import urllib.error
import urllib.request

LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_metrics(text):
    """{metric name: [(labels dict, float value)]} from Prometheus text; NaN/Inf are kept as floats."""
    result = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        head, _, value = line.rpartition(' ')
        if not head:
            continue
        name, brace, rest = head.partition('{')
        labels = dict(LABEL.findall(rest)) if brace else {}
        try:
            number = float(value)
        except ValueError:
            continue
        result.setdefault(name, []).append((labels, number))
    return result


def select(metrics, pattern):
    """Subset of metrics whose name matches the regular expression."""
    expression = re.compile(pattern)
    return {name: samples for name, samples in metrics.items() if expression.fullmatch(name)}


def value(metrics, name, default=None):
    """The first sample of a metric, or the default when it is absent."""
    samples = metrics.get(name)
    return samples[0][1] if samples else default


def is_missing(number):
    return number is None or (isinstance(number, float) and math.isnan(number))


def rate_between(previous, current, seconds, pattern):
    """Per-second increase of every metric matching pattern between two scrapes."""
    if seconds <= 0:
        return {}
    before, after = select(previous, pattern), select(current, pattern)
    rates = {}
    for name, samples in after.items():
        old = sum(v for _, v in before.get(name, []))
        new = sum(v for _, v in samples)
        rates[name] = max(0.0, new-old)/seconds
    return rates


class Scraper:
    """Port-forward to one service; get_text/get_json/metrics with one reconnect."""

    def __init__(self, cfg, service, port):
        self.cfg, self.service, self.port_remote = cfg, service, port
        self._stack, self.port = None, None

    def __enter__(self):
        self._open()
        return self

    def __exit__(self, *_):
        self.close()

    def _open(self):
        from evidence_integration_tests import forward
        self.close()
        self._stack = ExitStack()
        self.port = self._stack.enter_context(forward(self.cfg, self.service, self.port_remote))

    def close(self):
        if self._stack:
            self._stack.close()
            self._stack = None

    def get_text(self, path, token=None, timeout=10):
        for attempt in (0, 1):
            request = urllib.request.Request(f'http://127.0.0.1:{self.port}{path}',
                                             headers={'Authorization': 'Bearer '+token} if token else {})
            started = time.monotonic()
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.status, response.read().decode('utf-8', 'replace'), time.monotonic()-started
            except urllib.error.HTTPError as error:
                return error.code, error.read().decode('utf-8', 'replace'), time.monotonic()-started
            except OSError:
                if attempt == 0:
                    try:
                        self._open()
                        continue
                    except (RuntimeError, OSError):
                        pass
                return 0, '', time.monotonic()-started
        return 0, '', 0.0

    def get_json(self, path, token=None, timeout=10):
        status, text, seconds = self.get_text(path, token, timeout)
        try:
            return status, json.loads(text), seconds
        except ValueError:
            return status, None, seconds

    def metrics(self, path='/metrics'):
        status, text, _ = self.get_text(path)
        return parse_metrics(text) if status == 200 else None
