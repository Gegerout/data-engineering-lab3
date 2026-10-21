"""Время и пик RSS процесса вместе с потомками за несколько запусков."""
import json
import statistics
import subprocess
import sys
import time

import psutil

from common import LAB


def tree_rss(p):
    total = 0
    try:
        procs = [p] + p.children(recursive=True)
    except psutil.NoSuchProcess:
        return 0
    for q in procs:
        try:
            total += q.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total


def run_once(cmd):
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, cwd=LAB)
    ps = psutil.Process(proc.pid)
    peak = 0
    while proc.poll() is None:
        peak = max(peak, tree_rss(ps))
        time.sleep(0.02)
    wall = time.perf_counter() - t0
    if proc.returncode != 0:
        raise SystemExit(proc.stderr.read().decode())
    return wall, peak / 2 ** 20


def main():
    name, runs = sys.argv[1], int(sys.argv[2])
    cmd = sys.argv[sys.argv.index('--') + 1:]
    print(f'$ .venv/bin/python scripts/profile.py {name} {runs} -- ' + ' '.join(cmd), flush=True)
    res = []
    for i in range(1, runs + 1):
        wall, peak = run_once(cmd)
        res.append({'run': i, 'wall_s': round(wall, 2), 'peak_rss_mib': round(peak, 1)})
        print(f'{name} run {i} time={wall:.2f}s peak_rss={peak:.1f}MiB', flush=True)
    walls = [r['wall_s'] for r in res]
    peaks = [r['peak_rss_mib'] for r in res]
    print(f'{name} median time={statistics.median(walls):.2f}s peak_rss={statistics.median(peaks):.1f}MiB')
    out = LAB / 'logs' / f'profile_{name}.json'
    out.write_text(json.dumps({'name': name, 'cmd': cmd, 'runs': res}, indent=2))


if __name__ == '__main__':
    main()
