"""Только чтение объекта из RustFS теми же сплитами, без обработки."""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common import BUCKET, RAW_KEY, s3_client


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--chunk-mb', type=int, default=32)
    ap.add_argument('--workers', type=int, default=8)
    a = ap.parse_args()
    print('$ .venv/bin/python scripts/bench_read.py ' + ' '.join(sys.argv[1:]), flush=True)
    s3 = s3_client()
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)['ContentLength']
    cs = a.chunk_mb * 1024 * 1024

    def read(start):
        body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f'bytes={start}-{min(start + cs, size) - 1}')['Body']
        return sum(len(ch) for ch in body.iter_chunks(1 << 20))

    t0 = time.perf_counter()
    with ThreadPoolExecutor(a.workers) as ex:
        total = sum(ex.map(read, range(0, size, cs)))
    dt = time.perf_counter() - t0
    print(f'read bytes={total} time={dt:.2f}s speed_mb_per_s={total / dt / 1e6:.0f}')


if __name__ == '__main__':
    main()
