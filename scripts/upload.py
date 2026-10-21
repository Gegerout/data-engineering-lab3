"""Загрузка датасета в hh-lake и проверка SHA256."""
import hashlib
import sys
import time

from boto3.s3.transfer import TransferConfig

from common import BUCKET, LOCAL_CSV, RAW_KEY, s3_client


def sha256_file(path):
    with open(path, 'rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def main():
    s3 = s3_client()
    print('$ .venv/bin/python scripts/upload.py', flush=True)
    names = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if BUCKET not in names:
        s3.create_bucket(Bucket=BUCKET)
        print('created bucket', BUCKET)
    size = LOCAL_CSV.stat().st_size
    local_sha = sha256_file(LOCAL_CSV)
    t0 = time.perf_counter()
    s3.upload_file(str(LOCAL_CSV), BUCKET, RAW_KEY,
                   Config=TransferConfig(multipart_chunksize=64 * 1024 * 1024))
    print(f'uploaded s3://{BUCKET}/{RAW_KEY} bytes={size} time={time.perf_counter() - t0:.1f}s')
    head = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)
    print(f"head content_length={head['ContentLength']}")
    h = hashlib.sha256()
    for chunk in s3.get_object(Bucket=BUCKET, Key=RAW_KEY)['Body'].iter_chunks(8 * 1024 * 1024):
        h.update(chunk)
    print(f'sha256_local={local_sha}')
    print(f'sha256_s3={h.hexdigest()}')
    print(f'match={h.hexdigest() == local_sha}')
    return 0 if h.hexdigest() == local_sha else 1


if __name__ == '__main__':
    sys.exit(main())
