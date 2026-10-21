import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import time

import psutil


CASES = [
    ('mapreduce_1', ['mapreduce', '--workers', '1']),
    ('mapreduce_4', ['mapreduce', '--workers', '4']),
    ('duckdb_csv', ['duckdb', '--workers', '4']),
    ('duckdb_parquet', ['duckdb', '--workers', '4', '--parquet-key', 'optimized/it_vacancies_core.parquet']),
    ('pandas', ['pandas']),
]


def measure(name, arguments, attempt):
    root = Path('results/benchmark')
    logs = Path('evidence/benchmark')
    root.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    output = root/f'{name}-{attempt}.json'
    command = [sys.executable, 'jobs.py', *arguments, '--config', 'config.json', '--output', str(output)]
    started_at = datetime.now().astimezone().isoformat()
    peak = 0
    with (logs/f'{name}-{attempt}.txt').open('w') as log:
        log.write('$ python ' + ' '.join(command[1:]) + '\n')
        log.flush()
        started = time.perf_counter()
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        handle = psutil.Process(process.pid)
        killed = False
        samples = 0
        while process.poll() is None:
            processes = [handle]
            try:
                processes += handle.children(recursive=True)
            except psutil.Error:
                pass
            rss = 0
            for child in processes:
                try:
                    rss += child.memory_info().rss
                except psutil.Error:
                    pass
            peak = max(peak, rss)
            samples += 1
            if rss > 6 * 1024**3:
                os.killpg(process.pid, signal.SIGTERM)
                killed = True
            time.sleep(0.01)
        code = process.wait()
        wall = time.perf_counter()-started
    result = dict(case=name, attempt=attempt, started_at=started_at, wall_seconds=wall, peak_rss_bytes=peak,
                  exit_code=code, samples=samples, memory_limit_exceeded=killed,
                  output=str(output), command=['python', *command[1:]])
    print(json.dumps(result), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--attempts', type=int, default=3)
    args = parser.parse_args()
    result = []
    path = Path('results/benchmark.json')
    for attempt in range(1, args.attempts+1):
        offset = (attempt-1) % len(CASES)
        for name, arguments in CASES[offset:]+CASES[:offset]:
            result.append(measure(name, arguments, attempt))
            path.write_text(json.dumps(result, indent=2))
    hardware = dict(os=platform.platform(), architecture=platform.machine(),
                    python=platform.python_version(), logical_cpus=psutil.cpu_count(),
                    ram_bytes=psutil.virtual_memory().total,
                    rss_definition='Sum of RSS of the client process and all descendant processes; 10 ms polling. Shared pages can be counted more than once. RustFS is excluded.',
                    cache='OS and RustFS page caches warm; new client process per trial; rotated case order.')
    Path('evidence/hardware.json').write_text(json.dumps(hardware, indent=2))


if __name__ == '__main__':
    main()
