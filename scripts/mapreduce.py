"""MapReduce по объекту в RustFS, частоты навыков, пар и троек навыков варианта."""
import argparse
import csv
import heapq
import json
import multiprocessing as mp
import os
import shutil
import sys
import time
import zlib
from collections import Counter
from itertools import combinations, groupby

from common import BUCKET, LAB, RAW_KEY, VARIANT_ROLE, in_variant, s3_client, skills_of

MAX_ATTEMPTS = 4
ROLE_BYTES = VARIANT_ROLE.encode()
EXTRA = 256 * 1024  # шаг дочитывания последней строки за концом сплита


def get_range(s3, lo, hi):
    return s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f'bytes={lo}-{hi}')['Body'].read()


def read_split(s3, start, end, size):
    """Сплит целиком от байта start-1 до конца последней строки, начатой до end."""
    base = start - 1 if start > 0 else 0
    hi = end - 1
    buf = bytearray(get_range(s3, base, hi))
    while hi < size - 1 and buf.find(b'\n', end - 1 - base) == -1:
        lo, hi = hi + 1, min(hi + EXTRA, size - 1)
        buf += get_range(s3, lo, hi)
    return bytes(buf), base


def owned_lines(buf, base, start, end):
    """Строки, которые начинаются внутри сплита, как в LineRecordReader из Hadoop."""
    if start == 0:
        p = 0
    else:
        nl = buf.find(b'\n')  # buf начинается с байта start-1
        if nl == -1:
            return
        p = nl + 1
    n = len(buf)
    while p < n and base + p < end:
        nl = buf.find(b'\n', p)
        if nl == -1:
            nl = n
        yield base + p, buf[p:nl]
        p = nl + 1


def whole_split_lines(s3, start, end, size, c):
    """Без потоковой обработки, сплит и копии всех строк в памяти."""
    buf, base = read_split(s3, start, end, size)
    c['bytes_read'] += len(buf)
    lines = list(owned_lines(buf, base, start, end))
    del buf
    yield from lines


def stream_lines(s3, start, end, size, c, need=None):
    """Потоковое чтение кусками по 1 МиБ. Строки без байтов need отдаются как None."""
    base = start - 1 if start > 0 else 0
    pos, tail, skip = base, b'', start > 0
    lo, hi = base, end - 1
    while True:
        body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f'bytes={lo}-{hi}')['Body']
        for chunk in body.iter_chunks(1 << 20):
            c['bytes_read'] += len(chunk)
            data, i = tail + chunk, 0
            while (nl := data.find(b'\n', i)) != -1:
                lp = pos + i
                if skip:
                    skip = False  # хвост строки предыдущего сплита
                elif lp >= end:
                    body.close()
                    return
                elif need is not None and data.find(need, i, nl) == -1:
                    yield lp, None
                else:
                    yield lp, data[i:nl]
                i = nl + 1
            tail, pos = data[i:], pos + i
        if pos >= end:
            return
        if hi >= size - 1:
            if tail and not skip:
                yield pos, tail if need is None or need in tail else None
            return
        lo, hi = hi + 1, min(hi + EXTRA, size - 1)


def part_of(key, reducers):
    return zlib.crc32('\x1f'.join(map(str, key)).encode()) % reducers


def map_task(job, task, start, end, size, opts, attempt):
    s3 = s3_client()
    out = os.path.join(job, f'map-{task:03d}')
    tmp = f'{out}.attempt-{attempt}'
    os.makedirs(tmp)
    c = Counter()
    parts = [Counter() if opts['combiner'] else [] for _ in range(opts['reducers'])]
    if opts['stream']:
        lines = stream_lines(s3, start, end, size, c, ROLE_BYTES if opts['prefilter'] else None)
    else:
        lines = whole_split_lines(s3, start, end, size, c)
    mid = start + (end - start) // 2
    for pos, line in lines:
        if opts['fail_map'] == task and attempt == 1 and pos >= mid:
            # имитация отказа маппера посреди сплита
            with open(os.path.join(tmp, 'part-000.jsonl'), 'w') as f:
                f.write('incomplete\n')
            os._exit(17)
        c['map_input_records'] += 1
        # без текста роли строка не подходит, CSV не разбираем
        if line is None or (opts['prefilter'] and ROLE_BYTES not in line):
            continue
        row = next(csv.reader([line.decode('utf-8')]))
        if pos == 0 and row[0] == 'source':
            continue  # заголовок CSV
        if not in_variant(row):
            continue
        c['variant_records'] += 1
        sk = skills_of(row)
        if sk:
            c['variant_records_with_skills'] += 1
        for k in (1, 2, 3):
            for combo in combinations(sk, k):
                key = (k,) + combo
                c['map_output_records'] += 1
                p = parts[part_of(key, opts['reducers'])]
                if opts['combiner']:
                    p[key] += 1
                else:
                    p.append((key, 1))
    for r, p in enumerate(parts):
        items = sorted(p.items()) if opts['combiner'] else sorted(p)
        c['spilled_records'] += len(items)
        with open(os.path.join(tmp, f'part-{r:03d}.jsonl'), 'w', encoding='utf-8') as f:
            for key, v in items:
                f.write(json.dumps([list(key), v], ensure_ascii=False) + '\n')
    with open(os.path.join(tmp, 'counters.json'), 'w') as f:
        json.dump(c, f)
    os.rename(tmp, out)  # фиксируется только успешная попытка


def read_part(path):
    with open(path, encoding='utf-8') as f:
        for line in f:
            key, v = json.loads(line)
            yield tuple(key), v


def reduce_task(job, r, maps, attempt):
    tmp = os.path.join(job, f'reduce-{r:03d}.attempt-{attempt}.jsonl')
    streams = [read_part(os.path.join(job, f'map-{m:03d}', f'part-{r:03d}.jsonl')) for m in range(maps)]
    c = Counter()
    with open(tmp, 'w', encoding='utf-8') as f:
        for key, group in groupby(heapq.merge(*streams, key=lambda kv: kv[0]), key=lambda kv: kv[0]):
            total = 0
            for _, v in group:
                total += v
                c['reduce_input_records'] += 1
            c['reduce_output_records'] += 1
            f.write(json.dumps([list(key), total], ensure_ascii=False) + '\n')
    with open(os.path.join(job, f'reduce-{r:03d}.counters.json'), 'w') as f:
        json.dump(c, f)
    os.rename(tmp, os.path.join(job, f'reduce-{r:03d}.jsonl'))


def run_tasks(kind, tasks, workers, log):
    """До workers процессов одновременно, упавшая попытка запускается заново."""
    ctx = mp.get_context('spawn')
    pending = list(tasks)
    attempts = {}
    running = {}
    while pending or running:
        while pending and len(running) < workers:
            tid, target, args = pending.pop(0)
            attempts[tid] = attempts.get(tid, 0) + 1
            p = ctx.Process(target=target, args=args + (attempts[tid],))
            p.start()
            running[tid] = (p, target, args)
        time.sleep(0.01)
        for tid in list(running):
            p, target, args = running[tid]
            if p.is_alive():
                continue
            p.join()
            del running[tid]
            if p.exitcode != 0:
                log(f'{kind} task {tid} attempt {attempts[tid]} failed exit={p.exitcode}')
                if attempts[tid] >= MAX_ATTEMPTS:
                    raise SystemExit(f'{kind} task {tid} failed {MAX_ATTEMPTS} times')
                pending.insert(0, (tid, target, args))
                log(f'{kind} task {tid} restarted attempt {attempts[tid] + 1}')
            elif attempts[tid] > 1:
                log(f'{kind} task {tid} attempt {attempts[tid]} succeeded')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--chunk-mb', type=int, default=32)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--reducers', type=int, default=4)
    ap.add_argument('--no-combiner', action='store_true')
    ap.add_argument('--no-prefilter', action='store_true')
    ap.add_argument('--no-stream', action='store_true', help='держать весь сплит в памяти')
    ap.add_argument('--fail-map', type=int, default=-1, help='номер map-задачи, первая попытка которой упадет')
    ap.add_argument('--job', default='mr')
    ap.add_argument('--out', default='mapreduce', help='папка результатов в results/')
    ap.add_argument('--upload', action='store_true', help='записать результаты в s3://hh-lake/results/mapreduce/')
    a = ap.parse_args()
    print('$ .venv/bin/python scripts/mapreduce.py ' + ' '.join(sys.argv[1:]), flush=True)
    log = lambda m: print(f'{time.perf_counter() - t0:.2f}s {m}', flush=True)
    kv = lambda c: ' '.join(f'{k}={v}' for k, v in sorted(c.items()))
    t0 = time.perf_counter()
    opts = {'combiner': not a.no_combiner, 'prefilter': not a.no_prefilter, 'stream': not a.no_stream,
            'reducers': a.reducers, 'fail_map': a.fail_map}
    job = str(LAB / 'tmp' / 'mr' / a.job)
    shutil.rmtree(job, ignore_errors=True)
    os.makedirs(job)
    size = s3_client().head_object(Bucket=BUCKET, Key=RAW_KEY)['ContentLength']
    cs = a.chunk_mb * 1024 * 1024
    splits = [(i, s, min(s + cs, size)) for i, s in enumerate(range(0, size, cs))]
    log(f'input s3://{BUCKET}/{RAW_KEY} bytes={size} chunk_mib={a.chunk_mb} splits={len(splits)} '
        f'workers={a.workers} reducers={a.reducers} combiner={opts["combiner"]} prefilter={opts["prefilter"]} stream={opts["stream"]}')
    tasks = [(i, map_task, (job, i, s, e, size, opts)) for i, s, e in splits]
    run_tasks('map', tasks, a.workers, log)
    mc = Counter()
    for i, _, _ in splits:
        with open(os.path.join(job, f'map-{i:03d}', 'counters.json')) as f:
            mc.update(json.load(f))
    log('map phase done ' + kv(mc))
    run_tasks('reduce', [(r, reduce_task, (job, r, len(splits))) for r in range(a.reducers)], a.workers, log)
    rc = Counter()
    rows = {1: [], 2: [], 3: []}
    for r in range(a.reducers):
        with open(os.path.join(job, f'reduce-{r:03d}.counters.json')) as f:
            rc.update(json.load(f))
        for key, v in read_part(os.path.join(job, f'reduce-{r:03d}.jsonl')):
            rows[key[0]].append((key[1:], v))
    log('reduce phase done ' + kv(rc))
    out = LAB / 'results' / a.out
    out.mkdir(parents=True, exist_ok=True)
    s3 = s3_client() if a.upload else None
    for k in (1, 2, 3):
        rows[k].sort(key=lambda kv: (-kv[1], kv[0]))
        path = out / f'skills_k{k}.csv'
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow([f'skill_{j + 1}' for j in range(k)] + ['vacancies'])
            for combo, v in rows[k]:
                w.writerow(list(combo) + [v])
        log(f'k={k} distinct={len(rows[k])} total={sum(v for _, v in rows[k])}')
        for i, (combo, v) in enumerate(rows[k][:3], 1):
            log(f'k={k} top{i} {" + ".join(combo)} vacancies={v}')
        if s3:
            s3.upload_file(str(path), BUCKET, f'results/mapreduce/skills_k{k}.csv')
            log(f'uploaded s3://{BUCKET}/results/mapreduce/skills_k{k}.csv bytes={path.stat().st_size}')
    log('job finished')


if __name__ == '__main__':
    main()
