#!/usr/bin/env python3
"""Collect local pprof evidence during matched JSON fixture traffic."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.request

from sdk_integration import ROOT, running
from load_baseline import stage, machine_details


def collect(url, path):
    with urllib.request.urlopen(url, timeout=35) as response:
        path.write_bytes(response.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'tmp/profiles/before')
    parser.add_argument('--binary', type=Path, default=ROOT / 'bin/gateway')
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--first-delay', default='5ms')
    args = parser.parse_args()
    if not 1 <= args.seconds <= 30:
        parser.error('seconds must be 1..30')
    args.output.mkdir(parents=True, exist_ok=True)
    overrides = {'token_limit': {'capacity': 1_000_000_000, 'refill_per_second': 1_000_000}, 'max_concurrent': 128, 'max_streams': 128}
    payload = json.dumps({'model': 'default-chat', 'messages': [{'role': 'user', 'content': 'profile ' * 4096}], 'max_tokens': 16})
    with running(('-first-delay', args.first_delay), overrides=overrides, profiling=True, binary=args.binary.resolve()) as (client, gateway, stats):
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(collect, stats['management'] + f'/debug/pprof/profile?seconds={args.seconds}', args.output / 'cpu.prof')
            result = stage(str(client.base_url), payload, args.concurrency, args.seconds + 1, stats['management'], gateway.pid, stats['primary_pid'])
            pending.result()
        collect(stats['management'] + '/debug/pprof/heap?gc=1', args.output / 'heap.prof')
        collect(stats['management'] + '/debug/pprof/allocs', args.output / 'allocs.prof')
    for profile in ('cpu', 'heap', 'allocs'):
        command = ['go', 'tool', 'pprof', '-top', '-nodecount=25']
        if profile == 'allocs':
            command += ['-sample_index=alloc_space']
        output = subprocess.check_output(command + [str(args.binary), str(args.output / (profile + '.prof'))], text=True)
        (args.output / (profile + '-top.txt')).write_text(output)
    result.update(binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(), hardware=machine_details(), profile_seconds=args.seconds, workload=f'{args.concurrency} concurrent persistent connections, JSON, 32KiB input, first delay {args.first_delay}, no reuse')
    (args.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
