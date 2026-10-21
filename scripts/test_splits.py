"""Каждая строка файла достается ровно одному сплиту. S3 подменен локальной копией файла."""
from collections import Counter

import mapreduce as m
from common import LOCAL_CSV

data = LOCAL_CSV.read_bytes()
size = len(data)


class Body:
    def __init__(self, b):
        self.b = b

    def iter_chunks(self, n):
        for i in range(0, len(self.b), n):
            yield self.b[i:i + n]

    def close(self):
        pass


class LocalS3:
    def get_object(self, Bucket, Key, Range):
        lo, hi = map(int, Range[len('bytes='):].split('-'))
        return {'Body': Body(data[lo:hi + 1])}


m.get_range = lambda s3, lo, hi: data[lo:hi + 1]
starts = [0]
i = data.find(b'\n')
while i != -1 and i + 1 < size:
    starts.append(i + 1)
    i = data.find(b'\n', i + 1)
print('$ .venv/bin/python scripts/test_splits.py')
print(f'file bytes={size} lines={len(starts)}')
for cs in [1_000_003, 7 * 1024 * 1024 + 11, 32 * 1024 * 1024]:
    for mode in ('whole', 'stream'):
        got = []
        for s in range(0, size, cs):
            e = min(s + cs, size)
            c = Counter()
            lines = (m.whole_split_lines(None, s, e, size, c) if mode == 'whole'
                     else m.stream_lines(LocalS3(), s, e, size, c))
            for pos, line in lines:
                nl = data.find(b'\n', pos)
                assert data[pos:nl if nl != -1 else size] == line
                got.append(pos)
        print(f'chunk={cs} mode={mode} splits={-(-size // cs)} lines={len(got)} each_line_once={got == starts}')
kept = sum(1 for p in starts if m.ROLE_BYTES in data[p:(data.find(b'\n', p) if data.find(b'\n', p) != -1 else size)])
print(f'prefilter lines_with_role={kept} lines_total={len(starts)}')
