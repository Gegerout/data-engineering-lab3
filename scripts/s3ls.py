"""Список объектов бакета hh-lake."""
from common import BUCKET, s3_client

s3 = s3_client()
print('$ .venv/bin/python scripts/s3ls.py')
total = 0
for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET):
    for o in page.get('Contents', []):
        total += o['Size']
        print(f"{o['LastModified'].astimezone():%Y-%m-%d %H:%M:%S} {o['Size']} s3://{BUCKET}/{o['Key']}")
print(f'total_bytes={total}')
