#!/usr/bin/env python3
"""Write only demo client credentials for the separate load-generator PC.

Criteria: P2-6 (client credentials for the separate load-generator PC).
"""
import argparse
from pathlib import Path
from common import LOCAL, private_json, runtime_secrets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', default=str(LOCAL/'load-credentials.json'))
    args = parser.parse_args()
    credentials = runtime_secrets(LOCAL/'secrets.json')
    private_json(Path(args.output), {key: credentials[key] for key in ('media', 'field')})
    print('Wrote private generator credentials (Media/Field only): '+args.output)


if __name__ == '__main__': main()
