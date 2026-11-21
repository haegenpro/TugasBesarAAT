#!/usr/bin/env python3
"""Compare the WILDFIRE events in Audit with a reference supplied by the course assistants.

Criteria: P4-6 (agreement with the assistants' reference).

The reference file is JSON: either a list, or an object with a list under "events" or "items".
Each entry is a string (the source_ref_id) or an object with "source_ref_id". Entries may also carry
"severity" and "area_name", which are compared when present. The tool reports events only in the
reference, events only in the system, and field disagreements, with PASS/FAIL lines; it needs a reference
and cannot be judged without one. Optionally restrict both sides with --since (event time in Audit).
"""
import argparse
import json
from pathlib import Path
import sys

from common import LOCAL, read_config
from evidence_audit import AuditClient, scan_source
from evidence_report import Report, new_output_dir


def load_reference(path):
    """{source_ref_id: {extra fields}} from a reference file."""
    data = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    if isinstance(data, dict):
        data = data.get('events') or data.get('items') or []
    reference = {}
    for entry in data:
        if isinstance(entry, str):
            reference[entry] = {}
        elif isinstance(entry, dict) and entry.get('source_ref_id'):
            reference[str(entry['source_ref_id'])] = {k: v for k, v in entry.items() if k in ('severity', 'area_name')}
        else:
            raise ValueError('every reference entry needs a source_ref_id')
    return reference


def compare(reference, actual):
    """{'missing': ids only in the reference, 'extra': ids only in the system, 'different': [(id, field, ref, system)]}."""
    missing = sorted(set(reference)-set(actual))
    extra = sorted(set(actual)-set(reference))
    different = []
    for ref_id in sorted(set(reference)&set(actual)):
        for field, expected in reference[ref_id].items():
            if actual[ref_id].get(field) != expected:
                different.append((ref_id, field, expected, actual[ref_id].get(field)))
    return {'missing': missing, 'extra': extra, 'different': different, 'common': len(set(reference)&set(actual))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(LOCAL/'config.json'))
    parser.add_argument('--reference', required=True)
    parser.add_argument('--source', default='SATELIT')
    parser.add_argument('--since')
    parser.add_argument('--output')
    args = parser.parse_args(argv)
    cfg = read_config(args.config)
    report = Report()
    reference = load_reference(args.reference)
    with AuditClient(cfg) as client:
        scanned = scan_source(client, args.source, since=args.since, fields=('severity', 'area_name'))
    actual = {i['source_ref_id']: i for i in scanned['items']}
    result = compare(reference, actual)
    report.line(f"reference {len(reference)} events, system {len(actual)} events, {result['common']} in common (partial={scanned['partial']})")
    for label, ids in (('only in the reference', result['missing']), ('only in the system', result['extra'])):
        report.line(f'{len(ids)} {label}' + (': '+', '.join(ids[:10]) if ids else ''))
    for ref_id, field, expected, got in result['different'][:10]:
        report.line(f'{ref_id}: {field} reference={expected!r} system={got!r}')
    verdicts = [('every reference event exists in the system', not result['missing']),
                ('the system has no event outside the reference', not result['extra']),
                ('compared fields agree', not result['different']),
                ('the Audit scan was complete', not scanned['partial'])]
    for name, ok in verdicts:
        report.line(('PASS ' if ok else 'FAIL ')+name)
    report.data.update(result=result, checks=dict(verdicts))
    report.save(new_output_dir('p4-reference-compare', args.output))
    return 0 if all(ok for _, ok in verdicts) else 1


if __name__ == '__main__':
    sys.exit(main())
