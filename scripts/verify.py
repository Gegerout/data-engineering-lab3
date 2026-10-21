"""Полная сверка результатов MapReduce и DuckDB из hh-lake."""
import csv
import hashlib
import io
import sys

from common import BUCKET, s3_client


def load(s3, engine, k):
    body = s3.get_object(Bucket=BUCKET, Key=f'results/{engine}/skills_k{k}.csv')['Body'].read().decode('utf-8')
    rows = list(csv.reader(io.StringIO(body)))[1:]
    return {tuple(r[:-1]): int(r[-1]) for r in rows}


def digest(d):
    canon = '\n'.join('\x1f'.join(key) + '\t' + str(v) for key, v in sorted(d.items()))
    return hashlib.sha256(canon.encode()).hexdigest()[:16]


def main():
    print('$ .venv/bin/python scripts/verify.py', flush=True)
    s3 = s3_client()
    ok = True
    for k in (1, 2, 3):
        mr, db = load(s3, 'mapreduce', k), load(s3, 'duckdb', k)
        only_mr = mr.keys() - db.keys()
        only_db = db.keys() - mr.keys()
        diff = [key for key in mr.keys() & db.keys() if mr[key] != db[key]]
        same = not only_mr and not only_db and not diff
        ok &= same
        print(f'k={k} mapreduce rows={len(mr)} sum={sum(mr.values())} sha={digest(mr)}')
        print(f'k={k} duckdb rows={len(db)} sum={sum(db.values())} sha={digest(db)}')
        print(f'k={k} only_mapreduce={len(only_mr)} only_duckdb={len(only_db)} count_diff={len(diff)} equal={same}')
    print(f'all_equal={ok}')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
